-- ConnectionManager class for resty.redis_mux.
-- Manages the shared TCP connection, ring buffer, and read/write driver threads.
--
-- Architecture:
--   Driver threads (writeloop, readloop) are bound to a single TCP connection
--   lifecycle. On TCP error they return err; the main coroutine (spawn_driver)
--   handles reconnection and spawns new threads with a new connection.

local type = type
local tostring = tostring
local ngx_sleep = ngx.sleep
local worker_exiting = ngx.worker.exiting
local timer_at = ngx.timer.at
local thread_spawn = ngx.thread.spawn
local thread_wait = ngx.thread.wait
local tcp = ngx.socket.tcp
local semaphore_new = ngx.semaphore.new

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function(narr, nrec) return {} end
end

local protocol = require "resty.redis_mux.protocol"
local client_mod = require "resty.redis_mux.client"

-- Capture protocol functions as locals for performance
local _gen_req = protocol._gen_req
local _read_reply = protocol._read_reply
local put_tab_into_pool = protocol.put_tab_into_pool
local advance = protocol.advance
local create_degraded_connection = protocol.create_degraded_connection

local STATE_DISCONNECTED  = protocol.STATE_DISCONNECTED
local STATE_CONNECTING    = protocol.STATE_CONNECTING
local STATE_CONNECTED     = protocol.STATE_CONNECTED
local STATE_RECONNECTING  = protocol.STATE_RECONNECTING
local STATE_DEAD          = protocol.STATE_DEAD
local STATE_DRAINING      = protocol.STATE_DRAINING

local new_client = client_mod.new

local _M = new_tab(0, 20)

----------------------------------------------------------------------
-- State machine validation
----------------------------------------------------------------------

local valid_transitions = {
    [STATE_DISCONNECTED]  = { [STATE_CONNECTING] = true },
    [STATE_CONNECTING]    = { [STATE_CONNECTED] = true, [STATE_DISCONNECTED] = true },
    [STATE_CONNECTED]     = { [STATE_RECONNECTING] = true, [STATE_DRAINING] = true,
                              [STATE_DEAD] = true },
    [STATE_RECONNECTING]  = { [STATE_CONNECTED] = true, [STATE_DEAD] = true,
                              [STATE_DRAINING] = true },
    [STATE_DEAD]          = { [STATE_DISCONNECTED] = true },
    [STATE_DRAINING]      = { [STATE_DISCONNECTED] = true },
}

local function set_state(shared, new_state)
    local old = shared.state
    local allowed = valid_transitions[old]
    if allowed and allowed[new_state] then
        shared.state = new_state
    else
        ngx.log(ngx.WARN, "redis_mux: invalid state transition: ", old, " -> ", new_state)
        shared.state = new_state  -- still execute to avoid blocking
    end
end

----------------------------------------------------------------------
-- Shared state creation
----------------------------------------------------------------------

local function create_shared_state(opts)
    local capacity = opts.capacity or 100
    if capacity < 1 then
        capacity = 1
    end

    local shared = {
        -- Connection
        sock = nil,
        host = opts.host or "127.0.0.1",
        port = opts.port or 6379,
        opts = opts,

        -- Ring buffer
        send_queue = new_tab(capacity, 0),
        response_slots = new_tab(capacity, 0),
        capacity = capacity,
        enqueue_idx = 1,
        write_idx = 1,
        read_idx = 1,

        -- Admission + work signaling
        enqueue_sem = semaphore_new(capacity),
        work_available = semaphore_new(0),

        -- Per-ConnectionManager semaphore pool
        sem_pool = new_tab(capacity, 0),
        sem_pool_len = 0,

        -- State
        state = STATE_DISCONNECTED,
        state_err = nil,

        -- Driver control
        write_thread = nil,
        read_thread = nil,
        driver_stop_sem = semaphore_new(0),
        driver_done_sem = semaphore_new(0),
        connect_done_sem = semaphore_new(0),

        -- Failure handling
        failure_mode = opts.failure_mode or "reconnect",
        on_reconnect = opts.on_reconnect,
        backoff_initial = opts.reconnect_backoff_initial or 0.1,
        backoff_max = opts.reconnect_backoff_max or 30,
        backoff_multiplier = opts.reconnect_backoff_multiplier or 2.0,
        backoff_current = opts.reconnect_backoff_initial or 0.1,
        reconnect_max_retries = opts.reconnect_max_retries or 10,

        -- Blocking command handling
        blocking_strategy = opts.blocking_strategy or "fork",
        fork_pool_size = opts.fork_pool_size or 10,
        fork_idle_timeout = opts.fork_idle_timeout or 30000,

        -- Drain
        draining = false,
        drain_timeout = opts.drain_timeout or 5,
        drain_poll_interval = opts.drain_poll_interval or 1.0,
    }

    return shared
end

----------------------------------------------------------------------
-- Cleanup helpers
----------------------------------------------------------------------

-- Clear semaphore pool (drain tokens and empty pool array)
local function tb_clear_sem_pool(shared)
    local pool = shared.sem_pool
    local pool_len = shared.sem_pool_len
    for i = 1, pool_len do
        local sem = pool[i]
        while sem:wait(0) do end
        pool[i] = nil
    end
    shared.sem_pool_len = 0
end


-- Error all inflight slots from read_idx to enqueue_idx
local function error_all_inflight(shared, err_msg)
    local read_idx = shared.read_idx
    local enqueue_idx = shared.enqueue_idx
    local capacity = shared.capacity

    local i = read_idx
    while i ~= enqueue_idx do
        local slot = shared.response_slots[i]
        if slot and not slot.done then
            slot.res = nil
            slot.err = err_msg
            slot.done = true
            if slot.sem then
                slot.sem:post(1)
            end
        end
        i = advance(i, capacity)
    end
end

-- Attempt reconnection (called by spawn_driver during reconnect)
local function attempt_reconnect(shared)
    local sock, err = tcp()
    if not sock then
        return false, err
    end

    local opts = shared.opts
    sock:settimeouts(
        opts.connect_timeout or 1000,
        opts.send_timeout or 1000,
        opts.read_timeout or 1000
    )

    local ok, conn_err = sock:connect(shared.host, shared.port)
    if not ok then
        return false, conn_err
    end

    if opts.ssl then
        ok, conn_err = sock:sslhandshake(false, opts.server_name, opts.ssl_verify)
        if not ok then
            return false, "SSL handshake failed: " .. (conn_err or "unknown")
        end
    end

    -- Re-authenticate
    if opts.password then
        local auth_req = _gen_req({"auth", opts.password})
        local bytes, send_err = sock:send(auth_req)
        put_tab_into_pool(auth_req)
        if not bytes then
            return false, "AUTH failed: " .. (send_err or "unknown")
        end
        local res, read_err = _read_reply(sock)
        if not res then
            return false, "AUTH response failed: " .. (read_err or "unknown")
        end
    end

    -- Re-select DB
    if opts.db then
        local select_req = _gen_req({"select", opts.db})
        local bytes, send_err = sock:send(select_req)
        put_tab_into_pool(select_req)
        if not bytes then
            return false, "SELECT failed: " .. (send_err or "unknown")
        end
        local res, read_err = _read_reply(sock)
        if not res then
            return false, "SELECT response failed: " .. (read_err or "unknown")
        end
    end

    shared.sock = sock
    return true
end

----------------------------------------------------------------------
-- writeloop — bound to one TCP connection lifecycle
-- On TCP error: returns err to spawn_driver (does NOT handle reconnect)
-- On worker_exiting: enters DRAINING state, drains pending sends, exits
----------------------------------------------------------------------

local function writeloop(shared)
    local capacity = shared.capacity

    while true do
        -- === Exit detection: worker exiting → enter drain mode ===
        if worker_exiting() and shared.state ~= STATE_DRAINING
           and shared.state ~= STATE_DEAD then
            set_state(shared, STATE_DRAINING)
            shared.draining = true
        end

        -- === Terminal states ===
        if shared.state == STATE_DEAD then
            break
        end

        -- === Draining: send all enqueued commands, then exit ===
        if shared.state == STATE_DRAINING then
            -- write_idx == enqueue_idx can mean empty OR full ring buffer.
            -- Check if there's actually data at the current write position.
            if shared.write_idx == shared.enqueue_idx
               and not shared.send_queue[shared.write_idx] then
                break
            end
            -- Fall through to send remaining commands
        end

        -- === Non-working states (DISCONNECTED/CONNECTING/RECONNECTING) ===
        -- RECONNECTING is transient here; spawn_driver will kill threads soon
        if shared.state ~= STATE_CONNECTED and shared.state ~= STATE_DRAINING then
            ngx_sleep(0.01)
            goto continue
        end

        -- === Wait for work (timeout = exit detection pulse) ===
        local ok, wait_err = shared.work_available:wait(shared.drain_poll_interval)
        if not ok then
            -- timeout → loop back to check worker_exiting()
            -- other errors → retry
            goto continue
        end

        -- === Send command ===
        -- write_idx == enqueue_idx can mean empty OR full ring buffer.
        -- Check for actual data at the current write position.
        local write_idx = shared.write_idx
        local req = shared.send_queue[write_idx]
        if not req then
            -- Spurious wakeup or buffer wrapped-but-empty
            goto continue
        end

        -- Pipeline or single-command send
        if type(req[1]) == "table" then
            for _, frag in ipairs(req) do
                local bytes, send_err = shared.sock:send(frag)
                if not bytes then
                    -- TCP error → return to spawn_driver
                    return "send error: " .. (send_err or "unknown")
                end
            end
        else
            local bytes, send_err = shared.sock:send(req)
            if not bytes then
                -- TCP error → return to spawn_driver
                return "send error: " .. (send_err or "unknown")
            end
        end

        shared.write_idx = advance(write_idx, capacity)
        -- Keep the sent data in the slot for potential resend on reconnect.
        -- The slot will be overwritten when a new command is enqueued at this position.
        ::continue::
    end

    -- === Thread exit cleanup (runs for all exit reasons) ===
    shared.draining = true
    shared.driver_stop_sem:post(1)
    -- Close socket if we still own it
    if shared.sock then
        shared.sock:close()
        shared.sock = nil
    end
    -- Normal exit returns nil (no err)
end

----------------------------------------------------------------------
-- readloop — bound to one TCP connection lifecycle
-- On TCP error: returns err to spawn_driver (does NOT handle reconnect)
-- On worker_exiting: enters DRAINING state, drains pending reads, exits
----------------------------------------------------------------------

local function readloop(shared)
    local capacity = shared.capacity

    while true do
        -- === Exit detection: worker exiting → enter drain mode ===
        if worker_exiting() and shared.state ~= STATE_DRAINING
           and shared.state ~= STATE_DEAD then
            set_state(shared, STATE_DRAINING)
        end

        -- === Terminal states ===
        if shared.state == STATE_DEAD then
            break
        end

        -- === Draining: read all responses for sent commands, then exit ===
        if shared.state == STATE_DRAINING then
            -- read_idx == write_idx can mean empty OR full (wrapped) buffer.
            if shared.read_idx == shared.write_idx then
                local slot = shared.response_slots[shared.read_idx]
                if not slot or slot.done then
                    if shared.draining then
                        -- writeloop has finished draining too
                        break
                    end
                    -- writeloop still draining, wait for signal
                    shared.driver_stop_sem:wait(shared.drain_poll_interval)
                    goto continue
                end
                -- Buffer wrapped: there are pending responses, fall through to read
            end
            -- Still have responses to read; fall through
        end

        -- === Non-working states ===
        if shared.state ~= STATE_CONNECTED and shared.state ~= STATE_DRAINING then
            ngx_sleep(0.01)
            goto continue
        end

        -- === Wait for data ===
        -- read_idx == write_idx can mean empty OR full (wrapped) buffer.
        -- Check response_slots to distinguish.
        local read_idx = shared.read_idx
        if read_idx == shared.write_idx then
            local slot = shared.response_slots[read_idx]
            if not slot or slot.done then
                -- Truly empty
                if shared.draining then
                    -- writeloop done; wait for final notification
                    shared.driver_stop_sem:wait(shared.drain_poll_interval)
                    goto continue
                end
                -- No data yet; avoid busy-wait
                ngx_sleep(0.01)
                goto continue
            end
            -- Buffer wrapped: there are pending responses, fall through to read
        end

        -- === Read response ===
        local res, read_err = _read_reply(shared.sock)
        if res == nil and read_err then
            -- TCP error → return to spawn_driver
            return "read error: " .. (read_err or "unknown")
        end

        -- === Route response to slot ===
        local slot = shared.response_slots[read_idx]
        if slot then
            if slot.abandoned then
                -- Caller timed out; free the slot
                shared.enqueue_sem:post(1)
                shared.send_queue[read_idx] = nil
            elseif slot.pipeline then
                -- Pipeline slot: read N responses
                local nreqs = slot.nreqs
                local vals = new_tab(nreqs, 0)
                local nvals = 0

                -- First response already read
                if res then
                    nvals = nvals + 1
                    vals[nvals] = res
                elseif res == nil then
                    -- Socket error on first pipeline response
                    return "pipeline read error: " .. (read_err or "unknown")
                else
                    -- Redis error on first response
                    nvals = nvals + 1
                    vals[nvals] = {false, read_err}
                end

                -- Read remaining responses
                for i = 2, nreqs do
                    local r2, r2_err = _read_reply(shared.sock)
                    if r2 == nil and r2_err then
                        -- Socket error during pipeline read
                        return "pipeline read error: " .. (r2_err or "unknown")
                    elseif r2 then
                        nvals = nvals + 1
                        vals[nvals] = r2
                    else
                        nvals = nvals + 1
                        vals[nvals] = {false, r2_err}
                    end
                end

                slot.res = vals
                slot.err = nil
                slot.done = true
                slot.sem:post(1)
            else
                -- Regular single-command slot
                slot.res = res
                slot.err = read_err
                slot.done = true
                if slot.sem then
                    slot.sem:post(1)  -- wake the waiting caller
                end
            end
        end

        shared.read_idx = advance(read_idx, capacity)
        ::continue::
    end
end

----------------------------------------------------------------------
-- spawn_driver — main coroutine: spawn threads, handle errors/reconnect
--
-- This is the central orchestration point:
--   - Spawns writeloop + readloop bound to current TCP connection
--   - On thread error: kills both threads, closes socket, handles reconnect
--   - On normal exit (DRAINING complete / worker exit): cleans up
--   - Reconnection logic (exponential backoff / callback) lives HERE,
--     not inside driver threads
----------------------------------------------------------------------

local function spawn_driver(shared)
    -- Reset ring buffer for fresh driver pair
    shared.enqueue_idx = 1
    shared.write_idx = 1
    shared.read_idx = 1
    shared.draining = false

    tb_clear_sem_pool(shared)

    shared.enqueue_sem = semaphore_new(shared.capacity)
    shared.work_available = semaphore_new(0)

    -- Spawn both driver threads
    shared.write_thread = thread_spawn(writeloop, shared)
    shared.read_thread = thread_spawn(readloop, shared)

    -- Wait for either thread to exit
    local _, res = thread_wait(shared.write_thread, shared.read_thread)

    if res == nil then
        -- === Normal exit (DRAINING complete / worker exit) ===
        -- Wait for the other thread to also finish
        if coroutine.status(shared.write_thread) ~= "dead" then
            thread_wait(shared.write_thread)
        end
        if coroutine.status(shared.read_thread) ~= "dead" then
            thread_wait(shared.read_thread)
        end
        shared.driver_done_sem:post(1)
        return
    end

    -- === Error exit: one thread returned an error ===
    -- Kill the other thread
    ngx.thread.kill(shared.write_thread)
    ngx.thread.kill(shared.read_thread)

    -- Close old socket
    if shared.sock then
        shared.sock:close()
        shared.sock = nil
    end

    -- Immediately error all inflight clients. We do NOT retry commands,
    -- do NOT assume success, do NOT assume failure — just report the abort.
    error_all_inflight(shared, "command exec aborted due to tcp error")

    -- === Handle according to failure_mode ===

    if shared.failure_mode == "error" then
        -- Mode 1: Immediate dead, no recovery
        set_state(shared, STATE_DEAD)
        shared.state_err = "connection lost: " .. (res or "unknown")
        shared.driver_done_sem:post(1)
        return res
    end

    if shared.failure_mode == "reconnect" then
        -- Mode 2: Auto-reconnect with exponential backoff
        set_state(shared, STATE_RECONNECTING)
        local retries = 0
        local max_retries = shared.reconnect_max_retries

        while not worker_exiting() do
            -- Exponential backoff delay
            local delay = shared.backoff_current
            shared.backoff_current = shared.backoff_current * shared.backoff_multiplier
            if shared.backoff_current > shared.backoff_max then
                shared.backoff_current = shared.backoff_max
            end
            ngx_sleep(delay)

            if worker_exiting() then
                set_state(shared, STATE_DRAINING)
                shared.driver_done_sem:post(1)
                return
            end

            local ok, rec_err = attempt_reconnect(shared)
            if ok then
                -- Reconnect succeeded: reset and spawn fresh driver threads
                shared.backoff_current = shared.backoff_initial
                set_state(shared, STATE_CONNECTED)
                -- Recurse with clean state (no inflight commands remain)
                return spawn_driver(shared)
            end

            retries = retries + 1
            if max_retries > 0 and retries >= max_retries then
                set_state(shared, STATE_DEAD)
                shared.state_err = "reconnect failed after " .. retries .. " retries"
                shared.driver_done_sem:post(1)
                return
            end
        end

        -- worker_exiting during reconnect loop
        set_state(shared, STATE_DRAINING)
        shared.driver_done_sem:post(1)
        return
    end

    if shared.failure_mode == "callback" then
        -- Mode 3: User-provided callback controls reconnection
        set_state(shared, STATE_RECONNECTING)

        local cb = shared.on_reconnect
        if cb then
            local cb_ok, cb_result = pcall(cb, shared.mgr)
            if not cb_ok then
                set_state(shared, STATE_DEAD)
                shared.state_err = "reconnect callback error"
                shared.driver_done_sem:post(1)
                return
            end
            if cb_result == false then
                set_state(shared, STATE_DEAD)
                shared.state_err = "reconnect callback returned false"
                shared.driver_done_sem:post(1)
                return
            end
        end

        -- Callback approved, attempt reconnection
        local ok, rec_err = attempt_reconnect(shared)
        if ok then
            shared.backoff_current = shared.backoff_initial
            set_state(shared, STATE_CONNECTED)
            return spawn_driver(shared)
        else
            set_state(shared, STATE_DEAD)
            shared.state_err = "reconnect failed"
            shared.driver_done_sem:post(1)
            return
        end
    end

    -- Fallback (should not reach here)
    shared.driver_done_sem:post(1)
end

----------------------------------------------------------------------
-- ConnectionManager class
----------------------------------------------------------------------

_M.__index = _M

function _M.new(self, opts)
    if type(opts) ~= "table" then
        error("bad argument #1 opts: table expected, got " .. type(opts), 2)
    end

    local host = opts.host
    if host and type(host) ~= "string" then
        error("bad field opts.host: string expected, got " .. type(host), 2)
    end

    local port = opts.port
    if port and type(port) ~= "number" then
        error("bad field opts.port: number expected, got " .. type(port), 2)
    end

    local failure_mode = opts.failure_mode or "reconnect"
    if failure_mode ~= "error" and failure_mode ~= "reconnect"
       and failure_mode ~= "callback" then
        error("bad field opts.failure_mode: 'error', 'reconnect', or 'callback' expected", 2)
    end

    if failure_mode == "callback" and not opts.on_reconnect then
        error("opts.on_reconnect required when failure_mode is 'callback'", 2)
    end

    local blocking_strategy = opts.blocking_strategy or "fork"
    if blocking_strategy ~= "error" and blocking_strategy ~= "fork" then
        error("bad field opts.blocking_strategy: 'error' or 'fork' expected", 2)
    end

    local shared = create_shared_state(opts)
    shared.mgr = nil  -- will be set after mgr is created

    local mgr = setmetatable({
        _shared = shared,
    }, _M)

    shared.mgr = mgr  -- back-reference for callback mode

    return mgr
end

function _M.set_option(self, key, value)
    local shared = self._shared
    if key == "host" then
        shared.host = value
    elseif key == "port" then
        shared.port = value
    elseif key == "failure_mode" then
        shared.failure_mode = value
    else
        -- Allow setting arbitrary opts fields
        shared.opts[key] = value
    end
end

function _M.is_dead(self)
    return self._shared.state == STATE_DEAD
end

function _M.is_shutting_down(self)
    return self._shared.state == STATE_DRAINING
end

function _M.get_state(self)
    return self._shared.state
end

-- Initialize the cosocket connection (MUST be called from timer context)
-- Returns true on success, nil + error message on failure
local function init_connection(shared)
    local sock, err = tcp()
    if not sock then
        return nil, "failed to create socket: " .. (err or "unknown")
    end

    local opts = shared.opts
    sock:settimeouts(
        opts.connect_timeout or 1000,
        opts.send_timeout or 1000,
        opts.read_timeout or 1000
    )

    local ok, conn_err = sock:connect(shared.host, shared.port)
    if not ok then
        return nil, "failed to connect: " .. (conn_err or "unknown")
    end

    -- SSL handshake if configured
    if opts.ssl then
        ok, conn_err = sock:sslhandshake(false, opts.server_name, opts.ssl_verify)
        if not ok then
            return nil, "failed to do SSL handshake: " .. (conn_err or "unknown")
        end
    end

    -- AUTH if password configured
    if opts.password then
        local auth_req = _gen_req({"auth", opts.password})
        local bytes, send_err = sock:send(auth_req)
        put_tab_into_pool(auth_req)
        if not bytes then
            return nil, "failed to send AUTH: " .. (send_err or "unknown")
        end
        local res, read_err = _read_reply(sock)
        if not res then
            return nil, "AUTH failed: " .. (read_err or "unknown")
        end
    end

    -- SELECT if db configured
    if opts.db then
        local select_req = _gen_req({"select", opts.db})
        local bytes, send_err = sock:send(select_req)
        put_tab_into_pool(select_req)
        if not bytes then
            return nil, "failed to send SELECT: " .. (send_err or "unknown")
        end
        local res, read_err = _read_reply(sock)
        if not res then
            return nil, "SELECT failed: " .. (read_err or "unknown")
        end
    end

    shared.sock = sock
    return true
end

_M.init_connection = init_connection

function _M.connect(self)
    local shared = self._shared

    -- From terminal states: ngx.thread.kill only works within the same
    -- context, and connect() may be called from a different context than
    -- the one that spawned the old driver threads. Rebuild _shared instead
    -- of trying to kill old threads; old threads will exit naturally on
    -- their own shared table via the DEAD/DRAINING state checks.
    if shared.state == STATE_DEAD or shared.state == STATE_DRAINING then
        local new_shared = create_shared_state(shared.opts)
        new_shared.mgr = self
        self._shared = new_shared
        shared = new_shared
    end

    if shared.state ~= STATE_DISCONNECTED then
        return nil, "already connected or connecting (state: " .. shared.state .. ")"
    end

    set_state(shared, STATE_CONNECTING)
    shared.backoff_current = shared.backoff_initial

    -- Spawn timer to establish the connection and launch drivers.
    -- The socket MUST be created inside the timer context because cosockets
    -- are bound to the request/context that creates them, and the driver
    -- threads (writeloop/readloop) run inside the timer context.
    local opts = shared.opts
    local connect_timeout = (opts.connect_timeout or 1000) / 1000

    local ok_spawn, spawn_err = timer_at(0, function(premature)
        if premature then
            set_state(shared, STATE_DISCONNECTED)
            shared.connect_done_sem:post(1)
            return
        end

        local ok, err = init_connection(shared)
        if not ok then
            set_state(shared, STATE_DISCONNECTED)
            shared.state_err = err
            shared.connect_done_sem:post(1)
            return
        end

        set_state(shared, STATE_CONNECTED)
        shared.connect_done_sem:post(1)

        -- Now spawn the driver threads (both run inside this timer context)
        spawn_driver(shared)
    end)

    if not ok_spawn then
        set_state(shared, STATE_DISCONNECTED)
        return nil, "failed to spawn driver timer: " .. (spawn_err or "unknown")
    end

    -- Wait for the timer to complete the connection (or fail)
    local wait_ok, wait_err = shared.connect_done_sem:wait(connect_timeout + 0.5)
    if not wait_ok or shared.state ~= STATE_CONNECTED then
        local err_msg = shared.state_err or wait_err or "connect timeout"
        -- Only transition to DISCONNECTED if the timer callback hasn't already done so
        -- (timer callback already sets DISCONNECTED on connection failure or premature exit)
        if shared.state ~= STATE_DISCONNECTED then
            set_state(shared, STATE_DISCONNECTED)
        end
        shared.state_err = nil
        return nil, err_msg
    end

    return true
end

function _M.get_client(self)
    local shared = self._shared

    -- Dead state
    if shared.state == STATE_DEAD then
        return nil, shared.state_err or "connection dead"
    end

    -- Draining → return degraded resty.redis instance (no multiplexing)
    if shared.state == STATE_DRAINING then
        return create_degraded_connection(shared)
    end

    -- Reconnecting → reject fast, don't let client wait and timeout
    if shared.state == STATE_RECONNECTING then
        return nil, "reconnecting to redis..."
    end

    -- Only CONNECTED state provides normal multiplexed clients
    if shared.state ~= STATE_CONNECTED then
        return nil, "not connected (state: " .. tostring(shared.state) .. ")"
    end

    -- Normal path → return Client
    return new_client(client_mod, shared)
end

-- Alias
_M.get_redis = _M.get_client

function _M.shutdown(self)
    local shared = self._shared

    -- Enter drain mode: new clients get degraded connections,
    -- driver threads drain remaining work gracefully
    set_state(shared, STATE_DRAINING)
    shared.draining = true

    -- Wake driver threads so they can start draining
    shared.work_available:post(1)
    shared.driver_stop_sem:post(1)

    -- Wait for driver threads to finish draining (with timeout)
    shared.driver_done_sem:wait(shared.drain_timeout or 5)

    -- Best-effort kill: only works within the same context.
    -- If called cross-context, old threads exit on their own via state checks.
    local wt = shared.write_thread
    if wt and coroutine.status(wt) ~= "dead" then
        ngx.thread.kill(wt)
    end
    local rt = shared.read_thread
    if rt and coroutine.status(rt) ~= "dead" then
        ngx.thread.kill(rt)
    end

    -- Error any remaining inflight commands on old shared
    error_all_inflight(shared, "manager shutdown")

    -- Rebuild _shared: avoids racing with old threads on ring buffer indices
    -- and ensures a clean state for any future reconnect.
    local new_shared = create_shared_state(shared.opts)
    new_shared.mgr = self
    self._shared = new_shared
end

return _M
