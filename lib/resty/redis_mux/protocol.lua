-- RESP protocol functions, object pools, constants, and shared utilities
-- for resty.redis_mux.

local sub = string.sub
local byte = string.byte
local null = ngx.null
local type = type
local tostring = tostring
local tonumber = tonumber
local semaphore_new = ngx.semaphore.new

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function(narr, nrec) return {} end
end

local ok_tb, tb_clear = pcall(require, "table.clear")
if not ok_tb or type(tb_clear) ~= "function" then
    tb_clear = function(t)
        for k, _ in pairs(t) do
            t[k] = nil
        end
    end
end

local _M = new_tab(0, 30)

----------------------------------------------------------------------
-- Table pool for RESP serialization buffers
----------------------------------------------------------------------

local tab_pool_len = 0
local tab_pool = new_tab(16, 0)

local function get_tab_from_pool()
    if tab_pool_len > 0 then
        tab_pool_len = tab_pool_len - 1
        return tab_pool[tab_pool_len + 1]
    end
    return new_tab(24, 0)
end
_M.get_tab_from_pool = get_tab_from_pool

local function put_tab_into_pool(tab)
    if tab_pool_len >= 32 then
        return
    end
    tb_clear(tab)
    tab_pool_len = tab_pool_len + 1
    tab_pool[tab_pool_len] = tab
end
_M.put_tab_into_pool = put_tab_into_pool

----------------------------------------------------------------------
-- Per-ConnectionManager semaphore pool
-- Pool storage lives in shared state; these functions operate on it.
----------------------------------------------------------------------

local function get_sem_from_pool(shared)
    local pool = shared.sem_pool
    local pool_len = shared.sem_pool_len
    if pool_len > 0 then
        shared.sem_pool_len = pool_len - 1
        return pool[pool_len]
    end
    return semaphore_new(0)
end
_M.get_sem_from_pool = get_sem_from_pool

local function put_sem_into_pool(shared, sem)
    local pool_len = shared.sem_pool_len
    if pool_len >= 32 then
        return
    end
    -- Drain any leftover tokens
    while sem:wait(0) do end
    local pool = shared.sem_pool
    shared.sem_pool_len = pool_len + 1
    pool[pool_len + 1] = sem
end
_M.put_sem_into_pool = put_sem_into_pool

----------------------------------------------------------------------
-- Blocking / stateful command list
-- These cannot be multiplexed; handled by blocking_strategy
----------------------------------------------------------------------

local blocked_cmds = {
    -- Pub/sub (connection enters pub/sub mode)
    subscribe = true,
    psubscribe = true,
    unsubscribe = true,
    punsubscribe = true,

    -- Blocking pops (block connection until data available)
    blpop = true,
    brpop = true,
    brpoplpush = true,
    bzpopmin = true,
    bzpopmax = true,

    -- Transactions (stateful)
    watch = true,
    multi = true,
    exec = true,
    discard = true,

    -- Monitoring (streaming mode)
    monitor = true,
}
_M.blocked_cmds = blocked_cmds

local sub_cmds = {
    subscribe = true,
    psubscribe = true,
}
_M.sub_cmds = sub_cmds

----------------------------------------------------------------------
-- State constants
----------------------------------------------------------------------

_M.STATE_DISCONNECTED  = "disconnected"
_M.STATE_CONNECTING    = "connecting"
_M.STATE_CONNECTED     = "connected"
_M.STATE_RECONNECTING  = "reconnecting"
_M.STATE_DEAD          = "dead"
_M.STATE_DRAINING      = "draining"
----------------------------------------------------------------------
-- The _gen_req and _read_reply functions below are adapted from
-- lib/resty/redis.lua in lua-resty-redis
--   Copyright (C) 2012-2017 Yichun Zhang (agentzh), OpenResty Inc.
-- Used under the BSD 2-Clause license.
----------------------------------------------------------------------
-- RESP Protocol functions
----------------------------------------------------------------------

function _M._gen_req(args)
    local nargs = #args

    local req = get_tab_from_pool()
    req[1] = "*"
    req[2] = nargs
    req[3] = "\r\n"
    local nbits = 4

    for i = 1, nargs do
        local arg = args[i]
        if type(arg) ~= "string" then
            arg = tostring(arg)
        end

        req[nbits] = "$"
        req[nbits + 1] = #arg
        req[nbits + 2] = "\r\n"
        req[nbits + 3] = arg
        req[nbits + 4] = "\r\n"

        nbits = nbits + 5
    end

    return req
end

function _M._read_reply(sock)
    local line, err = sock:receive()
    if not line then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    local prefix = byte(line)

    if prefix == 36 then    -- char '$' (bulk reply)
        local size = tonumber(sub(line, 2))
        if size < 0 then
            return null
        end

        local data, err = sock:receive(size)
        if not data then
            if err == "timeout" then
                sock:close()
            end
            return nil, err
        end

        local dummy, err = sock:receive(2) -- ignore CRLF
        if not dummy then
            if err == "timeout" then
                sock:close()
            end
            return nil, err
        end

        return data

    elseif prefix == 43 then    -- char '+' (status reply)
        return sub(line, 2)

    elseif prefix == 42 then -- char '*' (multi-bulk reply)
        local n = tonumber(sub(line, 2))

        if n < 0 then
            return null
        end

        local vals = new_tab(n, 0)
        local nvals = 0
        for i = 1, n do
            local res, err = _M._read_reply(sock)
            if res then
                nvals = nvals + 1
                vals[nvals] = res

            elseif res == nil then
                return nil, err

            else
                -- valid redis error value
                nvals = nvals + 1
                vals[nvals] = {false, err}
            end
        end

        return vals

    elseif prefix == 58 then    -- char ':' (integer reply)
        return tonumber(sub(line, 2))

    elseif prefix == 45 then    -- char '-' (error reply)
        return false, sub(line, 2)

    else
        return nil, "unknown prefix: \"" .. tostring(prefix) .. "\""
    end
end

----------------------------------------------------------------------
-- Ring buffer index helper
----------------------------------------------------------------------

function _M.advance(idx, capacity)
    local next_idx = idx + 1
    if next_idx > capacity then
        return 1
    end
    return next_idx
end

----------------------------------------------------------------------
-- Fork connection management (for blocking_strategy = "fork")
----------------------------------------------------------------------

local function create_fork_connection(shared)
    local redis = require "resty.redis"
    local r = redis:new()
    if not r then
        return nil, "failed to create fork redis instance"
    end

    r:set_timeouts(
        shared.opts.connect_timeout or 1000,
        shared.opts.send_timeout or 1000,
        shared.opts.read_timeout or 1000
    )

    local ok, err = r:connect(shared.host, shared.port, shared.opts)
    if not ok then
        return nil, "fork connect failed: " .. (err or "unknown")
    end

    return r
end

local function return_fork_connection(shared, conn, cmd)
    -- Don't return connections in pub/sub mode to the pool
    if sub_cmds[cmd] then
        return  -- caller manages the connection lifecycle
    end

    -- Use dedicated pool name to avoid contaminating the mux connection pool
    conn:set_keepalive(shared.fork_idle_timeout, shared.fork_pool_size, "redis_mux_fork")
end

function _M.fork_and_execute(shared, cmd, ...)
    local conn, err = create_fork_connection(shared)
    if not conn then
        return nil, err
    end

    local method = conn[cmd]
    if not method then
        return_fork_connection(shared, conn, cmd)
        return nil, "unknown fork command: " .. cmd
    end

    local res, exec_err = method(conn, ...)
    return_fork_connection(shared, conn, cmd)
    return res, exec_err
end

----------------------------------------------------------------------
-- Degraded connection creation (for shutting_down state)
----------------------------------------------------------------------

function _M.create_degraded_connection(shared)
    local redis = require "resty.redis"
    local r = redis:new()
    if not r then
        return nil, "failed to create resty.redis instance"
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

    return r
end

return _M
