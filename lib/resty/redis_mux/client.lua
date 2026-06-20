-- Client class for resty.redis_mux.
-- Lightweight per-request object returned by ConnectionManager:get_client().
-- All clients share the same underlying connection via ring buffer.

local ipairs = ipairs
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local rawget = rawget
local select = select
local type = type

local protocol = require "resty.redis_mux.protocol"

-- Capture frequently used protocol functions as locals for performance
local _gen_req = protocol._gen_req
local put_tab_into_pool = protocol.put_tab_into_pool
local get_sem_from_pool = protocol.get_sem_from_pool
local put_sem_into_pool = protocol.put_sem_into_pool
local advance = protocol.advance
local blocked_cmds = protocol.blocked_cmds
local fork_and_execute = protocol.fork_and_execute

local STATE_DEAD = protocol.STATE_DEAD
local STATE_DRAINING = protocol.STATE_DRAINING
local STATE_CONNECTED = protocol.STATE_CONNECTED
local STATE_RECONNECTING = protocol.STATE_RECONNECTING

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function(narr, nrec) return {} end
end

local _M = new_tab(0, 20)

----------------------------------------------------------------------
-- Client class
----------------------------------------------------------------------

_M.__index = _M

local client_mt = { __index = _M }

function _M.new(self, shared)
    local client = setmetatable({
        _shared = shared,
        _reqs = nil,           -- pipeline buffer
        _module_prefix = nil,
        _degraded_conn = nil,  -- for shutting_down degraded mode
    }, client_mt)
    return client
end

----------------------------------------------------------------------
-- Degraded command execution (for shutting_down state)
----------------------------------------------------------------------

local function _degraded_command(self, cmd, ...)
    -- Lazy-create degraded connection (one per client)
    local dconn = rawget(self, "_degraded_conn")
    if not dconn then
        local shared = self._shared
        local redis = require "resty.redis"
        local r = redis:new()
        if not r then
            return nil, "failed to create degraded redis instance"
        end

        r:set_timeouts(
            shared.opts.connect_timeout or 1000,
            shared.opts.send_timeout or 1000,
            shared.opts.read_timeout or 1000
        )

        local ok, err = r:connect(shared.host, shared.port, shared.opts)
        if not ok then
            return nil, "degraded connect failed: " .. (err or "unknown")
        end

        self._degraded_conn = r
        dconn = r
    end

    local method = dconn[cmd]
    if not method then
        return nil, "unknown degraded command: " .. cmd
    end
    return method(dconn, ...)
end

----------------------------------------------------------------------
-- Multiplexed command execution path
----------------------------------------------------------------------

local function _mux_command(self, cmd, ...)
    local shared = self._shared
    local capacity = shared.capacity
    local opts = shared.opts

    -- Serialize command
    local args = {cmd, ...}
    local req = _gen_req(args)

    -- Pipeline mode: cache the request
    local reqs = rawget(self, "_reqs")
    if reqs then
        reqs[#reqs + 1] = req
        return
    end

    -- Wait for an available slot (admission control / backpressure)
    local wait_timeout = (opts.send_timeout or 1000) / 1000
    local ok, wait_err = shared.enqueue_sem:wait(wait_timeout)
    if not ok then
        put_tab_into_pool(req)
        if wait_err == "timeout" then
            return nil, "write slot timeout (all " .. capacity .. " slots busy)"
        end
        return nil, "enqueue failed: " .. (wait_err or "unknown")
    end

    -- Check state again after acquiring slot
    if shared.state == STATE_DEAD then
        shared.enqueue_sem:post(1)  -- return the slot
        put_tab_into_pool(req)
        return nil, shared.state_err or "connection dead"
    end

    if shared.state == STATE_DRAINING then
        shared.enqueue_sem:post(1)  -- return the slot
        put_tab_into_pool(req)
        -- Degrade to direct connection
        return _degraded_command(self, cmd, ...)
    end

    -- Enqueue into ring buffer
    local idx = shared.enqueue_idx
    shared.send_queue[idx] = req

    local my_sem = get_sem_from_pool(shared)
    shared.response_slots[idx] = {
        sem = my_sem,
        res = nil,
        err = nil,
        done = false,
        abandoned = false,
    }

    shared.enqueue_idx = advance(idx, capacity)

    -- Wake the writeloop
    shared.work_available:post(1)

    -- Wait for response from readloop
    local read_timeout = (opts.read_timeout or 1000) / 1000
    local res_ok, res_err = my_sem:wait(read_timeout)

    if not res_ok then
        -- Timeout or error while waiting
        local slot = shared.response_slots[idx]
        if slot then
            slot.abandoned = true
        end
        put_sem_into_pool(shared, my_sem)
        if res_err == "timeout" then
            return nil, "response timeout"
        end
        return nil, "wait error: " .. (res_err or "unknown")
    end

    -- Collect result
    local slot = shared.response_slots[idx]
    local res, err = slot.res, slot.err
    slot.done = true

    -- Clear the send_queue slot now that response is confirmed.
    -- (writeloop no longer clears it after send, to support resend on reconnect)
    shared.send_queue[idx] = nil

    -- Free the slot for reuse
    shared.enqueue_sem:post(1)
    put_sem_into_pool(shared, my_sem)

    -- Check for dead state post-response
    if shared.state == STATE_DEAD then
        return nil, shared.state_err or "connection dead"
    end

    return res, err
end

----------------------------------------------------------------------
-- Main command entry point for Client
----------------------------------------------------------------------

local function do_cmd(self, cmd, ...)
    local shared = self._shared

    -- Check for blocked commands
    if blocked_cmds[cmd] then
        if shared.blocking_strategy == "fork" then
            return fork_and_execute(shared, cmd, ...)
        else
            return nil, cmd:upper() .. " not supported on multiplexed connection;"
                .. " use resty.redis directly"
        end
    end

    -- Module prefix handling
    local module_prefix = rawget(self, "_module_prefix")
    if module_prefix then
        self._module_prefix = nil
        return _mux_command(self, module_prefix .. "." .. cmd, ...)
    end

    -- Dead state check (fast path)
    if shared.state == STATE_DEAD then
        return nil, shared.state_err or "connection dead"
    end

    -- Shutting down → degrade
    if shared.state == STATE_DRAINING then
        return _degraded_command(self, cmd, ...)
    end

    -- Normal multiplexed path
    return _mux_command(self, cmd, ...)
end

-- Degraded pipeline execution (for shutting_down)
local function _degraded_pipeline(self, reqs)
    -- Execute pipeline commands sequentially on degraded connection
    local r = rawget(self, "_degraded_conn")
    if not r then
        local shared = self._shared
        local redis = require "resty.redis"
        local conn = redis:new()
        if not conn then
            return nil, "failed to create degraded redis instance"
        end
        conn:set_timeouts(
            shared.opts.connect_timeout or 1000,
            shared.opts.send_timeout or 1000,
            shared.opts.read_timeout or 1000
        )
        local ok, err = conn:connect(shared.host, shared.port, shared.opts)
        if not ok then
            return nil, "degraded connect failed: " .. (err or "unknown")
        end
        self._degraded_conn = conn
        r = conn
    end

    r:init_pipeline(#reqs)

    -- Send raw pre-serialized requests directly to the degraded socket
    for _, req in ipairs(reqs) do
        -- req is a table of RESP fragments; send directly
        local sock = rawget(r, "_sock")
        if sock then
            local bytes, err = sock:send(req)
            if not bytes then
                for _, r2 in ipairs(reqs) do
                    put_tab_into_pool(r2)
                end
                return nil, "degraded pipeline send failed: " .. (err or "unknown")
            end
        end
    end

    -- Read responses using protocol._read_reply
    local nreqs = #reqs
    local vals = new_tab(nreqs, 0)
    local nvals = 0
    local sock = rawget(r, "_sock")

    for i = 1, nreqs do
        local res, read_err = protocol._read_reply(sock)
        if res then
            nvals = nvals + 1
            vals[nvals] = res
        elseif res == nil then
            for _, r2 in ipairs(reqs) do
                put_tab_into_pool(r2)
            end
            return nil, read_err
        else
            nvals = nvals + 1
            vals[nvals] = {false, read_err}
        end
    end

    for _, r2 in ipairs(reqs) do
        put_tab_into_pool(r2)
    end

    return vals
end


----------------------------------------------------------------------
-- Client pipeline support
----------------------------------------------------------------------

function _M.init_pipeline(self, n)
    self._reqs = new_tab(n or 4, 0)
end

function _M.cancel_pipeline(self)
    local reqs = rawget(self, "_reqs")
    if reqs then
        for _, req in ipairs(reqs) do
            put_tab_into_pool(req)
        end
    end
    self._reqs = nil
end

function _M.commit_pipeline(self)
    local reqs = rawget(self, "_reqs")
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs = nil

    local shared = self._shared

    -- Check state
    if shared.state == STATE_DEAD then
        for _, req in ipairs(reqs) do
            put_tab_into_pool(req)
        end
        return nil, shared.state_err or "connection dead"
    end

    if shared.state == STATE_DRAINING then
        -- Degrade the entire pipeline
        return _degraded_pipeline(self, reqs)
    end

    local capacity = shared.capacity
    local opts = shared.opts

    -- Wait for a slot
    local wait_timeout = (opts.send_timeout or 1000) / 1000
    local ok, wait_err = shared.enqueue_sem:wait(wait_timeout)
    if not ok then
        for _, req in ipairs(reqs) do
            put_tab_into_pool(req)
        end
        return nil, "pipeline write slot timeout"
    end

    -- Re-check state
    if shared.state ~= STATE_CONNECTED and shared.state ~= STATE_RECONNECTING then
        shared.enqueue_sem:post(1)
        for _, req in ipairs(reqs) do
            put_tab_into_pool(req)
        end
        if shared.state == STATE_DEAD then
            return nil, shared.state_err or "connection dead"
        end
        return nil, "not connected"
    end

    -- Enqueue the entire pipeline as one request
    local idx = shared.enqueue_idx
    shared.send_queue[idx] = reqs  -- table of request fragments

    local my_sem = get_sem_from_pool(shared)
    shared.response_slots[idx] = {
        sem = my_sem,
        res = nil,
        err = nil,
        done = false,
        abandoned = false,
        pipeline = true,
        nreqs = #reqs,
    }

    shared.enqueue_idx = advance(idx, capacity)
    shared.work_available:post(1)

    -- Wait for response
    local read_timeout = (opts.read_timeout or 1000) / 1000
    local res_ok, res_err = my_sem:wait(read_timeout)

    if not res_ok then
        local slot = shared.response_slots[idx]
        if slot then
            slot.abandoned = true
        end
        put_sem_into_pool(shared, my_sem)
        if res_err == "timeout" then
            return nil, "pipeline response timeout"
        end
        return nil, "pipeline wait error: " .. (res_err or "unknown")
    end

    local slot = shared.response_slots[idx]
    local res, err = slot.res, slot.err

    shared.send_queue[idx] = nil
    shared.enqueue_sem:post(1)
    put_sem_into_pool(shared, my_sem)

    return res, err
end
----------------------------------------------------------------------
-- Client utility methods
----------------------------------------------------------------------

function _M.set_timeout(self, timeout)
    -- no-op for multiplexed client; timeouts configured at manager level
end

function _M.set_timeouts(self, connect_timeout, send_timeout, read_timeout)
    -- no-op for multiplexed client
end

----------------------------------------------------------------------
-- Module prefix support
----------------------------------------------------------------------

function _M.register_module_prefix(self, mod)
    self._module_prefix = mod
    return self
end

----------------------------------------------------------------------
-- Lazy method generation for any Redis command
----------------------------------------------------------------------

setmetatable(_M, {__index = function(self, cmd)
        local method = function(obj, ...)
            return do_cmd(obj, cmd, ...)
        end
        _M[cmd] = method
        return method
    end})

----------------------------------------------------------------------
-- hmset special handling (same as resty.redis)
----------------------------------------------------------------------

function _M.hmset(self, hashname, ...)
    if select('#', ...) == 1 then
        local t = select(1, ...)
        local n = 0
        for k, v in pairs(t) do
            n = n + 2
        end
        local array = new_tab(n, 0)
        local i = 0
        for k, v in pairs(t) do
            array[i + 1] = k
            array[i + 2] = v
            i = i + 2
        end
        return do_cmd(self, "hmset", hashname, unpack(array))
    end
    return do_cmd(self, "hmset", hashname, ...)
end

function _M.array_to_hash(self, t)
    local n = #t
    local h = new_tab(0, n / 2)
    for i = 1, n, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end

return _M
