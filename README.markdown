Name
====

lua-resty-redis-mux - Redis connection multiplexer for the ngx_lua cosocket API

Table of Contents
=================

* [Name](#name)
* [Status](#status)
* [Description](#description)
    * [State Machine](#state-machine)
* [Synopsis](#synopsis)
* [Dependencies](#dependencies)
* [Methods](#methods)
    * [new](#new)
    * [connect](#connect)
    * [get_client](#get_client)
    * [set_option](#set_option)
    * [is_dead](#is_dead)
    * [is_shutting_down](#is_shutting_down)
    * [get_state](#get_state)
    * [shutdown](#shutdown)
    * [init_pipeline](#init_pipeline)
    * [commit_pipeline](#commit_pipeline)
    * [cancel_pipeline](#cancel_pipeline)
    * [hmset](#hmset)
    * [array_to_hash](#array_to_hash)
    * [register_module_prefix](#register_module_prefix)
    * [set_timeout](#set_timeout)
    * [set_timeouts](#set_timeouts)
* [Blocking Commands](#blocking-commands)
* [Failure Modes and Error Handling](#failure-modes-and-error-handling)
    * [Error Flow](#error-flow)
* [Reconnection](#reconnection)
* [Graceful Shutdown](#graceful-shutdown)
* [Limitations](#limitations)
* [Installation](#installation)
* [Copyright and License](#copyright-and-license)

Status
======

Early release. The library is functional but the API may evolve.

Description
===========

This library provides a Redis connection multiplexer for OpenResty / ngx_lua.
It shares a single TCP connection across all concurrent requests within one
nginx worker, using separate read and write
[light threads](https://github.com/openresty/lua-nginx-module#ngxthreadspawn).

Instead of opening one connection per request (or using connection pooling with
its inherent overhead), all requests multiplex through one persistent connection
managed by the library. This reduces Redis server connection load and improves
throughput under high concurrency.

Architecture:

```
                    ┌─────────────────────────────┐
                    │     spawn_driver (main coroutine)                        │
                    │  - Manages reconnection/backoff                          │
                    │  - Kills threads on error                                │
                    │  - Restarts with new TCP conn                            │
                    └──────────┬──────────────────┘
                               │ spawn / wait / kill
                    ┌──────────┴──────────┐
                    │                                          │
               writeloop                                   readloop
               (send only)                              (receive + route)
               TCP error → err                           TCP error → err
               worker_exit → drain                       worker_exit → drain
```

* A **ring buffer** with a configurable capacity (default 100 slots) provides
  admission control and backpressure.
* Two internal **driver threads** (`writeloop` and `readloop`) serialize writes
  and route responses back to callers via per-slot semaphores.
* Callers obtain lightweight **Client** objects from the ConnectionManager.
  All clients share the same ring buffer and TCP connection.
* Blocking commands (BLPOP, BRPOP, SUBSCRIBE, PUBLISH, MULTI, etc.) are
  handled through a **fork strategy** (separate `resty.redis` connections)
  or rejected with an explicit error.
* Driver threads and TCP connection share the same lifecycle. When a TCP error
  occurs, the affected thread returns an error to `spawn_driver`, which handles
  reconnection and spawns new threads with a fresh TCP connection.

### State Machine

```
                    connect()
  [disconnected] ────────────→ [connecting] ──→ [connected]
       ↑                            ↑              │
       │                            │              │ socket error
       │                 success    │              ↓
       │                 ↑         ├── [reconnecting]  ← auto-reconnect
       │                 │         │        │           ← callback mode
       │                 └─────────┘   exhaust/        │
       │                               callback fail   │
       │                                      │        │
       │                                      ↓        │
       │                                  [dead] ←────┘
       │                                    │    (error mode: direct dead)
       │                                    │
       │                           worker_exiting()
       │                           or shutdown()
       │                                    ↓
       └────────────────────────── [draining]
            connect() re-launches       │
                              get_client() → degraded resty.redis
                              drain pending → cleanup
```

| State | `get_client()` | `Client:command()` | Driver threads |
|-------|---------------|-------------------|----------------|
| `disconnected` | returns `nil, err` | N/A | not started |
| `connecting` | returns `nil, err` | N/A | not started |
| `connected` | returns Client | enqueues to ring buffer | running |
| `reconnecting` | returns `nil, err` | N/A | new threads after reconnect |
| `dead` | returns `nil, err` | N/A | stopped |
| `draining` | returns degraded `resty.redis` | forwarded to degraded conn | draining then exit |

Synopsis
========

> **Important:** Cosocket API is **not available** during `init_worker_by_lua`.
> However, `connect()` internally uses `ngx.timer.at(0)` to defer the actual
> TCP connection into a timer context where cosockets work, so you can call it
> directly from `init_worker_by_lua`.

```lua
-- Module-level cache
local redis_mux = require "resty.redis_mux"
local mgr -- nil until first use

local function get_manager()
    if mgr and not mgr:is_dead() then
        return mgr
    end

    mgr = redis_mux.new({
        host = "127.0.0.1",
        port = 6379,
        failure_mode = "reconnect",
    })

    local ok, err = mgr:connect()
    if not ok then
        mgr = nil
        return nil, err
    end
    return mgr
end

-- In request context
local my_mgr, err = get_manager()
if not my_mgr then
    ngx.say("redis unavailable: ", err)
    return
end

local client, err = my_mgr:get_client()
-- ... use client ...
```

### Shutdown

Worker graceful exit (e.g., `nginx -s quit`) is handled **automatically**.
The driver threads detect `ngx.worker.exiting()` and transition to the draining
state without any manual intervention. You do **not** need to call `mgr:shutdown()`
in `worker_shutdown_by_lua` — the library handles this internally.

If you do need to programmatically shut down the manager (e.g., before
reconfiguration), `mgr:shutdown()` is available as a public API. After shutdown
the manager returns to the `disconnected` state and can be reconnected.

Dependencies
============

This library requires the following packages to be installed:

* **[lua-resty-redis](https://github.com/openresty/lua-resty-redis)** >= 0.28
  Required for degraded (draining) and fork (blocking command) connection paths.
* **[lua-resty-core](https://github.com/openresty/lua-resty-core)**
  Provides `ngx.semaphore`, which is required for admission control and
  response routing.

The library validates the presence of `lua-resty-redis` at module load time
and reports a clear error if the dependency is missing.

Methods
=======

ConnectionManager
-----------------

The ConnectionManager is the central orchestrator. It holds the shared TCP
connection, spawns the driver threads, manages reconnection, and hands out
Client objects.

### new

`syntax: mgr = redis_mux.new(opts)`

Creates a new ConnectionManager instance. Accepts an options table with the
following fields:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `"127.0.0.1"` | Redis server hostname or IP address. |
| `port` | number | `6379` | Redis server port. |
| `capacity` | number | `100` | Ring buffer size (max concurrent in-flight commands). |
| `connect_timeout` | number | `1000` | Connection timeout in milliseconds. |
| `send_timeout` | number | `1000` | Socket send timeout in milliseconds. |
| `read_timeout` | number | `1000` | Socket read timeout in milliseconds. |
| `password` | string | nil | Redis AUTH password. |
| `db` | number | nil | Redis database index (SELECT). |
| `ssl` | boolean | nil | Enable SSL/TLS for the connection. |
| `server_name` | string | nil | SNI name for SSL handshake. |
| `ssl_verify` | boolean | nil | Enable SSL certificate verification. |
| `pool` | string | nil | Custom connection pool name. |
| `pool_size` | number | nil | Connection pool size. |
| `backlog` | number | nil | Connection pool backlog. |
| `failure_mode` | string | `"reconnect"` | Behavior on TCP error: `"reconnect"`, `"error"`, or `"callback"`. See [Failure Modes and Error Handling](#failure-modes-and-error-handling). |
| `on_reconnect` | function | nil | User callback for `"callback"` failure mode. Receives the manager object. See [Reconnection](#reconnection). |
| `reconnect_backoff_initial` | number | `0.1` | Initial backoff in seconds. |
| `reconnect_backoff_max` | number | `30` | Maximum backoff in seconds. |
| `reconnect_backoff_multiplier` | number | `2.0` | Backoff multiplier. |
| `reconnect_max_retries` | number | `10` | Max reconnect attempts (0 = unlimited). |
| `blocking_strategy` | string | `"fork"` | How to handle blocking commands: `"fork"` or `"error"`. See [Blocking Commands](#blocking-commands). |
| `fork_pool_size` | number | `10` | Connection pool size for fork connections. |
| `fork_idle_timeout` | number | `30000` | Idle timeout for fork connections in milliseconds. |
| `drain_timeout` | number | `5` | Max seconds to wait for drain during shutdown. |

The ConnectionManager should be created once per nginx worker (typically in
`init_worker_by_lua`) and reused across requests.

### connect

`syntax: ok, err = mgr:connect()`

Initializes the TCP cosocket connection to Redis, sends AUTH and/or SELECT
as configured, and spawns the writeloop and readloop driver threads.

**Cosocket limitation:** The cosocket API is not available in `init_worker_by_lua`.
To work around this, `connect()` internally uses `ngx.timer.at(0)` to defer the
actual TCP connection to a timer callback context where cosockets are operational.
This means you can safely call `connect()` in `init_worker_by_lua` — the library
handles the deferral.

If you need the connection ready before the first request arrives, call
`connect()` in `init_worker_by_lua` and store the manager at module level
(see [Synopsis](#synopsis) for examples).

Returns `true` on success, or `nil` and an error message on failure.
The function blocks for up to `connect_timeout + 0.5` seconds waiting for
the timer to complete.

### get_client

`syntax: client, err = mgr:get_client()`

Returns a lightweight Client object for multiplexed Redis command execution.

* In `STATE_CONNECTED`: returns a Client that shares the multiplexed connection.
* In `STATE_DRAINING`: returns a Client backed by a degraded (direct, non-multiplexed) `resty.redis` connection.
* In other states (`disconnected`, `reconnecting`, `dead`): returns `nil` and an error message.

The alias `mgr:get_redis()` is also available and behaves identically.

### set_option

`syntax: mgr:set_option(key, value)`

Dynamically updates a configuration option on a running ConnectionManager.
Supported keys: `host`, `port`, `failure_mode`, `on_reconnect`, and any
option accepted by `new()`.

### is_dead

`syntax: is_dead = mgr:is_dead()`

Returns `true` if the ConnectionManager is in the dead state (connection lost
and recovery not possible or not configured).

### is_shutting_down

`syntax: is_shutting_down = mgr:is_shutting_down()`

Returns `true` if the ConnectionManager is in the draining state (graceful
shutdown in progress).

### get_state

`syntax: state = mgr:get_state()`

Returns the current state string. Possible values: `"disconnected"`,
`"connecting"`, `"connected"`, `"reconnecting"`, `"dead"`, `"draining"`.

### shutdown

`syntax: mgr:shutdown()`

Initiates graceful shutdown: enters the draining state, allows the driver
threads to finish all enqueued commands, force-kills threads that don't finish
within `drain_timeout`, and cleans up all state. After shutdown, the manager
returns to `STATE_DISCONNECTED` and can be reconnected via `connect()`.

**Worker graceful exit is handled automatically.** The driver threads
periodically check `ngx.worker.exiting()` via a 1-second detection pulse.
When the worker begins shutting down, the threads automatically transition
to the draining state — no manual `shutdown()` call is needed.

Use `shutdown()` only when you need to programmatically close the manager,
e.g., before configuration changes or during manual resource cleanup.

Client
------

The Client is a lightweight per-request object returned by
`ConnectionManager:get_client()`. It supports all standard Redis commands
as methods via automatic method generation (any Redis command name can be
called as a method).

Additionally, it provides the following special methods:

### init_pipeline

`syntax: client:init_pipeline(n)`

Starts pipeline mode. `n` is an optional capacity hint for the buffer.
Subsequent Redis commands are buffered into a pipeline rather than enqueued
individually.

Only one pipeline can be active per client.

### commit_pipeline

`syntax: results, err = client:commit_pipeline()`

Commits the buffered pipeline: enqueues all buffered commands as a single
ring buffer slot and returns a table of results for each command.

### cancel_pipeline

`syntax: client:cancel_pipeline()`

Discards the buffered pipeline without executing any commands.

### hmset

`syntax: client:hmset(hashname, field1, value1, field2, value2, ...)`

`syntax: client:hmset(hashname, { field1 = value1, field2 = value2, ... })`

Sets multiple field-value pairs in a hash. Accepts either key-value pairs
as separate arguments (backward-compatible) or a single Lua table mapping
field names to values.

### array_to_hash

`syntax: hash = client:array_to_hash(t)`

Converts a flat array `{ key1, val1, key2, val2, ... }` into a Lua hash table
`{ key1 = val1, key2 = val2, ... }`. Useful for converting HGETALL results.

### register_module_prefix

`syntax: client:register_module_prefix(mod)`

Registers a Redis module prefix. After registration, calling
`client:module_prefix()` sets the module prefix, and subsequent commands
are sent as `module_prefix.command_name`. This enables use of Redis module
commands like those provided by RedisBloom or RediSearch.

### set_timeout

`syntax: client:set_timeout(timeout)`

No-op. Timeouts are configured at the ConnectionManager level.

### set_timeouts

`syntax: client:set_timeouts(connect_timeout, send_timeout, read_timeout)`

No-op. Timeouts are configured at the ConnectionManager level.

Blocking Commands
=================

Commands that cannot be multiplexed through a shared connection (because they
hold state or block the connection for extended periods) are:

* **Pub/Sub**: `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`
* **Blocking pops**: `BLPOP`, `BRPOP`
* **Transactions**: `MULTI`, `EXEC`, `WATCH`, `UNWATCH`, `DISCARD`
* **Monitor**: `MONITOR`

The handling of these commands is controlled by the `blocking_strategy` option:

### fork (default)

Spawns a separate `resty.redis` connection for each blocking command. The fork
connections come from a dedicated pool (`redis_mux_fork`) with configurable
size and idle timeout.

This allows blocking commands to coexist with multiplexed usage, at the cost
of occasional additional connections.

### error

Returns an error message indicating that the command is not supported on the
mux connection. The caller is responsible for handling this case (typically
by creating their own `resty.redis` connection).

Failure Modes and Error Handling
================================

The behavior on TCP error is controlled by the `failure_mode` option:

### error

On TCP error, transitions immediately to `STATE_DEAD`. No recovery is
attempted. The manager must be recreated or reconnected manually via
`connect()` after a dead state reset.

### reconnect (default)

On TCP error, transitions to `STATE_RECONNECTING` and attempts automatic
reconnection with exponential backoff. All inflight commands receive an
error ("command exec aborted due to tcp error") and callers must retry
their own commands. Once reconnected, the manager returns to
`STATE_CONNECTED` and resumes normal operation.

Configure backoff behavior with:
* `reconnect_backoff_initial` (default `0.1` seconds)
* `reconnect_backoff_max` (default `30` seconds)
* `reconnect_backoff_multiplier` (default `2.0`)
* `reconnect_max_retries` (default `10`, `0` = unlimited)

### callback

On TCP error, calls the user-provided `on_reconnect` callback (set via
`opts.on_reconnect` or `set_option("on_reconnect", fn)`). The callback
receives the ConnectionManager object and is responsible for orchestrating
reconnection. This is useful for integrating with external service discovery
or custom retry logic.

### Error Flow

```
TCP error detected in writeloop or readloop
        │
        ▼
  Thread returns "error message" (single string)
  (NOT nil, err — nil signals normal exit)
        │
        ▼
  spawn_driver thread_wait captures the error
        │
        ├─ Kills the other driver thread
        ├─ Closes the broken socket
        ├─ error_all_inflight(): wakes all waiting clients
        │    with "command exec aborted due to tcp error"
        │
        └─ Dispatches by failure_mode:
             ├─ "error"      → STATE_DEAD, stop
             ├─ "reconnect"  → exponential backoff loop
             │                  → spawn_driver() recursively
             │                  → new threads, fresh TCP
             │                  → STATE_CONNECTED
             └─ "callback"   → pcall(on_reconnect)
                                → reconnect or STATE_DEAD
```

During the reconnect loop, `get_client()` returns `nil, err` for fast-fail
rejection (no timeout buildup). Once reconnected, normal operation resumes.

Reconnection
============

In `"reconnect"` failure mode, the ConnectionManager internally spawns a
timer-based reconnection loop via `ngx.timer.at`. The loop implements
exponential backoff with jitter:

```
delay = backoff_initial  (0.1s)
↓
attempt reconnect
  ├─ success → reset backoff, spawn new drivers, STATE_CONNECTED
  └─ failure → delay *= multiplier (max: backoff_max)
                retries++
                if max_retries > 0 and retries >= max_retries:
                  → STATE_DEAD
```

The reconnection loop creates new driver threads with a fresh TCP connection.
All previously inflight commands are errored during the disconnect. Successful
reconnection transitions the manager to `STATE_CONNECTED`.

In `"callback"` mode, the user's `on_reconnect` callback is invoked via pcall
(protected call). If the callback returns `nil, err`, the manager transitions
to `STATE_DEAD`.

Graceful Shutdown
=================

**Automatic worker exit handling:** The driver threads check `ngx.worker.exiting()`
every second (the detection pulse interval). When the worker begins shutting down,
the threads automatically enter `STATE_DRAINING` — no manual intervention needed.

When `shutdown()` is called (or worker exit is detected):

1. The manager enters `STATE_DRAINING`. New `get_client()` calls receive
   degraded (direct `resty.redis`) connections.
2. The driver threads wake up and drain all remaining enqueued commands
   from the ring buffer.
3. After `drain_timeout` seconds (default 5), any threads still running
   are force-killed.
4. All remaining inflight commands are errored and the shared state is
   cleaned up.
5. The manager transitions to `STATE_DISCONNECTED` and can be reconnected.

Limitations
===========

* The ConnectionManager must be stored at the nginx worker level (e.g., as
  a module-level variable set in `init_worker_by_lua`). It must NOT be shared
  across worker processes.
* Cosocket API calls (including `connect()`) cannot be made from `init_by_lua`,
  `log_by_lua`, or `header_filter_by_lua` phases. This library uses
  `ngx.timer.at(0)` internally to defer the TCP connection.
* Blocking commands require `lua-resty-redis` to be installed (fork strategy)
  or will return an error (error strategy).
* The ring buffer capacity limits maximum concurrent in-flight commands per
  worker. Exceeding capacity causes `get_client()` or `command()` to block
  on the enqueue semaphore until a slot frees up.

Installation
============

First, ensure the required dependencies are installed:

```bash
# Install lua-resty-core (required for ngx.semaphore)
opm install openresty/lua-resty-core

# Install lua-resty-redis (required for degraded and fork connections)
opm install openresty/lua-resty-redis
```

Then install lua-resty-redis-mux:

```bash
# From source
make install
```

Or via OPM (OpenResty Package Manager):

```bash
opm install openresty/lua-resty-redis-mux
```

Copyright and License
=====================

This module is licensed under the BSD 2-Clause license.

The `_gen_req` and `_read_reply` functions in `lib/resty/redis_mux/protocol.lua`
are adapted from `lib/resty/redis.lua` in
[lua-resty-redis](https://github.com/openresty/lua-resty-redis).

Copyright (C) 2012-2017 Yichun Zhang (agentzh), OpenResty Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
