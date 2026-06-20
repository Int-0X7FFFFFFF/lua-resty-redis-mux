use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: new() with default options
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local mgr, err = redis_mux.new({})
        if mgr then
            ngx.say("type: " .. type(mgr))
            ngx.say("state: " .. mgr:get_state())
        else
            ngx.say("error: " .. (err or "unknown"))
        end
    }
--- response_body
type: table
state: disconnected
--- no_error_log
[error]

=== TEST 2: new() with custom host/port
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local mgr = redis_mux.new({host = "192.168.1.1", port = 1234})
        ngx.say("state: " .. mgr:get_state())
    }
--- response_body
state: disconnected
--- no_error_log
[error]

=== TEST 3: new() validation -- bad failure_mode
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {failure_mode = "invalid"})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("err: " .. tostring(err):gsub("bad field opts%.failure_mode.*", "bad field opts.failure_mode"))
        end
    }
--- response_body
ok: false
err: bad field opts.failure_mode
--- no_error_log
[error]

=== TEST 4: new() validation -- callback without on_reconnect
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {failure_mode = "callback"})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("has on_reconnect: " .. tostring(err:find("on_reconnect") ~= nil))
        end
    }
--- response_body
ok: false
has on_reconnect: true
--- no_error_log
[error]

=== TEST 5: new() validation -- bad blocking_strategy
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {blocking_strategy = "invalid"})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("err: " .. tostring(err):gsub("bad field opts%.blocking_strategy.*", "bad field opts.blocking_strategy"))
        end
    }
--- response_body
ok: false
err: bad field opts.blocking_strategy
--- no_error_log
[error]

=== TEST 6: new() validation -- bad opts type (pass string)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, "not_a_table")
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("err: " .. tostring(err):gsub("bad argument #1 opts.*", "bad argument #1 opts"))
        end
    }
--- response_body
ok: false
err: bad argument #1 opts
--- no_error_log
[error]

=== TEST 7: new() validation -- bad host type
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {host = 123})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("err: " .. tostring(err):gsub("bad field opts%.host.*", "bad field opts.host"))
        end
    }
--- response_body
ok: false
err: bad field opts.host
--- no_error_log
[error]

=== TEST 8: new() validation -- bad port type
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {port = "not_a_number"})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("err: " .. tostring(err):gsub("bad field opts%.port.*", "bad field opts.port"))
        end
    }
--- response_body
ok: false
err: bad field opts.port
--- no_error_log
[error]

=== TEST 9: connect() happy path
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        ngx.say("initial: " .. mgr:get_state())

        local ok, err = mgr:connect()
        if ok then
            ngx.say("connect: ok")
            ngx.say("state: " .. mgr:get_state())
            ngx.say("is_dead: " .. tostring(mgr:is_dead()))
        else
            ngx.say("connect: failed - " .. (err or "unknown"))
        end

        -- cleanup
        if mgr:get_state() == "connected" then
            local c = mgr:get_client()
            if c then
                c:flushall()
            end
            mgr:shutdown()
        end
    }
--- response_body
initial: disconnected
connect: ok
state: connected
is_dead: false
--- no_error_log
[error]

=== TEST 10: connect() double connect rejected
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        local ok, err = mgr:connect()
        ngx.say("first connect: " .. tostring(ok))

        local ok2, err2 = mgr:connect()
        ngx.say("second connect ok: " .. tostring(ok2))
        if err2 then
            ngx.say("second connect err: " .. tostring(err2):gsub("already connected.*", "already connected"))
        end

        -- cleanup
        if mgr:get_state() == "connected" then
            local c = mgr:get_client()
            if c then c:flushall() end
            mgr:shutdown()
        end
    }
--- response_body
first connect: true
second connect ok: nil
second connect err: already connected
--- no_error_log
[error]

=== TEST 11: get_client() in connected state
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()

        local client, err = mgr:get_client()
        ngx.say("client type: " .. type(client))
        if client then
            ngx.say("client not nil: true")
        end

        local c = mgr:get_client()
        if c then c:flushall() end
        mgr:shutdown()
    }
--- response_body
client type: table
client not nil: true
--- no_error_log
[error]

=== TEST 12: get_client() in disconnected state
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local mgr = redis_mux.new({})
        local client, err = mgr:get_client()
        ngx.say("client: " .. tostring(client))
        if err then
            ngx.say("err contains 'not connected': " .. tostring(err:find("not connected") ~= nil))
        end
    }
--- response_body
client: nil
err contains 'not connected': true
--- no_error_log
[error]

=== TEST 13: get_redis() alias returns client
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()

        local c1 = mgr:get_client()
        local c2 = mgr:get_redis()
        ngx.say("both tables: " .. tostring(type(c1) == "table" and type(c2) == "table"))

        if c1 then c1:flushall() end
        mgr:shutdown()
    }
--- response_body
both tables: true
--- no_error_log
[error]

=== TEST 14: shutdown() returns to disconnected state
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        ngx.say("before shutdown: " .. mgr:get_state())

        mgr:shutdown()
        ngx.say("after shutdown: " .. mgr:get_state())
    }
--- response_body
before shutdown: connected
after shutdown: disconnected
--- no_error_log
[error]

=== TEST 15: reconnect after shutdown
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        mgr:shutdown()
        ngx.say("after shutdown: " .. mgr:get_state())

        local ok, err = mgr:connect()
        ngx.say("reconnect ok: " .. tostring(ok))
        if ok then
            ngx.say("reconnect state: " .. mgr:get_state())
            local c = mgr:get_client()
            if c then c:flushall() end
            mgr:shutdown()
        end
    }
--- response_body
after shutdown: disconnected
reconnect ok: true
reconnect state: connected
--- no_error_log
[error]

=== TEST 16: set_option() host and port
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local mgr = redis_mux.new({host = "original", port = 1111})
        mgr:set_option("host", "updated")
        mgr:set_option("port", 2222)
        ngx.say("options set: ok")
    }
--- response_body
options set: ok
--- no_error_log
[error]

=== TEST 17: set_option() failure_mode
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local mgr = redis_mux.new({failure_mode = "reconnect"})
        mgr:set_option("failure_mode", "error")
        ngx.say("failure_mode set: ok")
    }
--- response_body
failure_mode set: ok
--- no_error_log
[error]

=== TEST 18: module version
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        ngx.say("version: " .. redis_mux._VERSION)
    }
--- response_body
version: 0.1.0
--- no_error_log
[error]

=== TEST 19: module exports -- new function
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        ngx.say("new is function: " .. tostring(type(redis_mux.new) == "function"))
    }
--- response_body
new is function: true
--- no_error_log
[error]
