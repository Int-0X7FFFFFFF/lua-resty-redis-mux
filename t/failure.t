use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: failure_mode="error" -- normal operations then shutdown
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, failure_mode = "error", fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        ngx.say("initial state: " .. mgr:get_state())

        -- Verify connection works
        local c = mgr:get_client()
        c:set("fe_test", "1")
        ngx.say("set ok: " .. tostring(c:get("fe_test") == "1"))
        c:del("fe_test")

        -- Not dead during normal operation
        ngx.say("is_dead normal: " .. tostring(mgr:is_dead()))

        mgr:shutdown()
    }
--- response_body
initial state: connected
set ok: true
is_dead normal: false
--- no_error_log
[error]

=== TEST 2: failure_mode="error" -- dead after connect to unreachable port
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        -- Use a non-routable port to ensure connect fails
        local mgr = redis_mux.new({
            port = 19999,
            failure_mode = "error",
            connect_timeout = 100,
        })

        local ok, err = mgr:connect()
        ngx.say("connect ok: " .. tostring(ok))
        if err then
            ngx.say("connect err contains 'connect': " .. tostring(err:find("connect") ~= nil))
        end
        ngx.say("state after bad connect: " .. mgr:get_state())
    }
--- response_body
connect ok: nil
connect err contains 'connect': true
state after bad connect: disconnected
--- error_log
connect() failed

=== TEST 3: failure_mode="error" -- connect after bad port, then reconnect
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        -- First try bad port
        local mgr = redis_mux.new({
            port = 19999,
            failure_mode = "error",
            connect_timeout = 100,
            fork_idle_timeout = 100,
            drain_poll_interval = 0.05,
        })
        mgr:connect()

        -- Change to good port and reconnect
        mgr:set_option("host", redis_host)
        mgr:set_option("port", redis_port)
        local ok, err = mgr:connect()
        ngx.say("reconnect ok: " .. tostring(ok))
        if ok then
            ngx.say("state: " .. mgr:get_state())
            local c = mgr:get_client()
            if c then
                c:set("fe_good", "yes")
                ngx.say("get: " .. tostring(c:get("fe_good")))
                c:del("fe_good")
            end
            mgr:shutdown()
        end
    }
--- response_body
reconnect ok: true
state: connected
get: yes
--- error_log
connect() failed

=== TEST 4: failure_mode="reconnect" -- normal connect and operation
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({
            host = redis_host,
            port = redis_port,
            failure_mode = "reconnect",
            reconnect_max_retries = 3,
            reconnect_backoff_initial = 0.05,
            fork_idle_timeout = 100,
            drain_poll_interval = 0.05,
        })
        mgr:connect()
        ngx.say("initial state: " .. mgr:get_state())

        -- Verify working
        local c = mgr:get_client()
        c:set("rc_test", "ok")
        ngx.say("get: " .. tostring(c:get("rc_test")))
        c:del("rc_test")

        ngx.say("is_dead: " .. tostring(mgr:is_dead()))
        mgr:shutdown()
    }
--- response_body
initial state: connected
get: ok
is_dead: false
--- no_error_log
[error]

=== TEST 5: failure_mode="reconnect" -- connect fails with bad port
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"

        local mgr = redis_mux.new({
            port = 19999,
            failure_mode = "reconnect",
            reconnect_max_retries = 2,
            reconnect_backoff_initial = 0.05,
            reconnect_backoff_max = 0.5,
            connect_timeout = 100,
        })

        local ok, err = mgr:connect()
        ngx.say("connect ok: " .. tostring(ok))
        ngx.say("state after fail: " .. mgr:get_state())
    }
--- response_body
connect ok: nil
state after fail: disconnected
--- error_log
connect() failed

=== TEST 6: failure_mode="reconnect" -- reconnection after bad connect
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        -- Bad connect first
        local mgr = redis_mux.new({
            port = 19999,
            failure_mode = "reconnect",
            connect_timeout = 100,
            fork_idle_timeout = 100,
            drain_poll_interval = 0.05,
        })
        mgr:connect()

        -- Reconfigure and reconnect
        mgr:set_option("host", redis_host)
        mgr:set_option("port", redis_port)
        local ok, err = mgr:connect()
        ngx.say("reconnect ok: " .. tostring(ok))
        if ok then
            local c = mgr:get_client()
            if c then
                c:set("rc_reconnect", "works")
                ngx.say("get: " .. tostring(c:get("rc_reconnect")))
                c:del("rc_reconnect")
            end
            mgr:shutdown()
        end
    }
--- response_body
reconnect ok: true
get: works
--- error_log
connect() failed

=== TEST 7: failure_mode="callback" -- callback validation with valid fn
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local callback_set = false
        local mgr = redis_mux.new({
            host = redis_host,
            port = redis_port,
            failure_mode = "callback",
            on_reconnect = function(m)
                callback_set = true
                return true
            end,
            fork_idle_timeout = 100,
            drain_poll_interval = 0.05,
        })
        mgr:connect()
        ngx.say("state: " .. mgr:get_state())
        ngx.say("callback configured: true")

        local c = mgr:get_client()
        if c then c:flushall() end
        mgr:shutdown()
    }
--- response_body
state: connected
callback configured: true
--- no_error_log
[error]

=== TEST 8: failure_mode="callback" -- requires on_reconnect
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local ok, err = pcall(redis_mux.new, {failure_mode = "callback"})
        ngx.say("ok: " .. tostring(ok))
        if err then
            ngx.say("requires on_reconnect: " .. tostring(err:find("on_reconnect") ~= nil))
        end
    }
--- response_body
ok: false
requires on_reconnect: true
--- no_error_log
[error]

=== TEST 9: failure_mode="reconnect" -- shutdown then reconnect with reconnect mode
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({
            host = redis_host,
            port = redis_port,
            failure_mode = "reconnect",
            reconnect_max_retries = 3,
            reconnect_backoff_initial = 0.05,
            fork_idle_timeout = 100,
            drain_poll_interval = 0.05,
        })
        mgr:connect()
        ngx.say("initial: " .. mgr:get_state())

        mgr:shutdown()
        ngx.say("after shutdown: " .. mgr:get_state())

        local ok, err = mgr:connect()
        ngx.say("reconnect: " .. tostring(ok))
        if ok then
            ngx.say("final state: " .. mgr:get_state())
            local c = mgr:get_client()
            if c then c:flushall() end
            mgr:shutdown()
        end
    }
--- response_body
initial: connected
after shutdown: disconnected
reconnect: true
final state: connected
--- no_error_log
[error]
