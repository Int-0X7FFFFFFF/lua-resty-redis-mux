use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: hmset with key-value pairs
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:hmset("ec_hash", "f1", "v1", "f2", "v2")
        local v1 = client:hget("ec_hash", "f1")
        local v2 = client:hget("ec_hash", "f2")
        ngx.say("f1: " .. tostring(v1))
        ngx.say("f2: " .. tostring(v2))

        client:del("ec_hash")
        mgr:shutdown()
    }
--- response_body
f1: v1
f2: v2
--- no_error_log
[error]

=== TEST 2: hmset with table argument
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:hmset("ec_hash2", {a = "1", b = "2"})
        local vals = client:hmget("ec_hash2", "a", "b")
        ngx.say("a: " .. tostring(vals[1]))
        ngx.say("b: " .. tostring(vals[2]))

        client:del("ec_hash2")
        mgr:shutdown()
    }
--- response_body
a: 1
b: 2
--- no_error_log
[error]

=== TEST 3: array_to_hash (using HGETALL result)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        -- HGETALL returns {field1, val1, field2, val2, ...}
        client:hmset("ec_hash3", "x", "10", "y", "20")
        local arr = client:hgetall("ec_hash3")
        local hash = client:array_to_hash(arr)
        ngx.say("x: " .. tostring(hash["x"]))
        ngx.say("y: " .. tostring(hash["y"]))

        client:del("ec_hash3")
        mgr:shutdown()
    }
--- response_body
x: 10
y: 20
--- no_error_log
[error]

=== TEST 4: register_module_prefix consumed (single use)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        -- register_module_prefix sets a prefix for the next command
        -- After registration, the next command uses prefix + "." + cmd
        client:register_module_prefix("bf")
        -- This sends "bf.ping" which is not a valid Redis command
        local res, err = client:ping()
        -- After prefix is consumed, the next command should NOT use prefix
        local ping_res = client:ping()
        ngx.say("prefix consumed: " .. tostring(ping_res == "PONG"))

        mgr:shutdown()
    }
--- response_body
prefix consumed: true
--- no_error_log
[error]

=== TEST 5: capacity=0 clamped to 1
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, capacity = 0, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        -- Should not error; capacity should be clamped to 1
        local ok, err = mgr:connect()
        if ok then
            local c = mgr:get_client()
            if c then
                local v = c:ping()
                ngx.say("works: " .. tostring(v == "PONG"))
            end
            mgr:shutdown()
        else
            ngx.say("connect failed: " .. (err or "unknown"))
        end
    }
--- response_body
works: true
--- no_error_log
[error]

=== TEST 6: unknown command on client
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        -- An unknown Redis command should return error from server
        local res, err = client:this_command_does_not_exist("arg")
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))

        mgr:shutdown()
    }
--- response_body
res: false
has_err: true
--- no_error_log
[error]

=== TEST 7: SET with EX and NX options (no extra args test)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        -- SET with EX
        client:set("ex_key", "ex_val", "EX", 3600)
        local v = client:get("ex_key")
        ngx.say("set with EX: " .. tostring(v))
        local ttl = client:ttl("ex_key")
        ngx.say("ttl > 0: " .. tostring(ttl > 0))

        client:del("ex_key")
        mgr:shutdown()
    }
--- response_body
set with EX: ex_val
ttl > 0: true
--- no_error_log
[error]

=== TEST 8: multiple CONNECT/DISCONNECT cycles
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        local cycles = 3
        local all_ok = true

        for i = 1, cycles do
            local ok, err = mgr:connect()
            if not ok then
                all_ok = false
                break
            end
            local c = mgr:get_client()
            c:set("cycle_key", tostring(i))
            local v = c:get("cycle_key")
            if v ~= tostring(i) then
                all_ok = false
            end
            c:del("cycle_key")
            mgr:shutdown()
        end

        ngx.say("cycles ok: " .. tostring(all_ok))
    }
--- response_body
cycles ok: true
--- no_error_log
[error]

=== TEST 9: LPUSH with multiple values
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        local n = client:lpush("multi_vals", "a", "b", "c")
        ngx.say("push count: " .. tostring(n))
        local vals = client:lrange("multi_vals", 0, -1)
        ngx.say("vals[1]: " .. tostring(vals[1]))
        ngx.say("vals[2]: " .. tostring(vals[2]))
        ngx.say("vals[3]: " .. tostring(vals[3]))

        client:del("multi_vals")
        mgr:shutdown()
    }
--- response_body
push count: 3
vals[1]: c
vals[2]: b
vals[3]: a
--- no_error_log
[error]

=== TEST 10: SADD and SMEMBERS (set operations)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:sadd("myset", "a", "b", "c")
        local members = client:smembers("myset")
        ngx.say("count: " .. #members)
        ngx.say("has a: " .. tostring(members[1] == "a" or members[2] == "a" or members[3] == "a"))
        ngx.say("has b: " .. tostring(members[1] == "b" or members[2] == "b" or members[3] == "b"))
        ngx.say("has c: " .. tostring(members[1] == "c" or members[2] == "c" or members[3] == "c"))

        client:del("myset")
        mgr:shutdown()
    }
--- response_body
count: 3
has a: true
has b: true
has c: true
--- no_error_log
[error]

=== TEST 11: ZADD and ZRANGE (sorted set operations)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:zadd("zset", 10, "a")
        client:zadd("zset", 20, "b")
        client:zadd("zset", 30, "c")
        local vals = client:zrange("zset", 0, -1)
        ngx.say("len: " .. #vals)
        ngx.say("v1: " .. tostring(vals[1]))
        ngx.say("v2: " .. tostring(vals[2]))
        ngx.say("v3: " .. tostring(vals[3]))

        client:del("zset")
        mgr:shutdown()
    }
--- response_body
len: 3
v1: a
v2: b
v3: c
--- no_error_log
[error]

=== TEST 12: SELECT db option in connect
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, db = 0, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        local ok, err = mgr:connect()
        if ok then
            local c = mgr:get_client()
            if c then
                c:set("db0_key", "in_db0")
                local v = c:get("db0_key")
                ngx.say("db0 get: " .. tostring(v))
                c:del("db0_key")
            end
            mgr:shutdown()
        else
            ngx.say("connect failed: " .. tostring(err))
        end
    }
--- response_body
db0 get: in_db0
--- no_error_log
[error]

=== TEST 13: is_shutting_down in connected state
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        ngx.say("shutting_down (connected): " .. tostring(mgr:is_shutting_down()))
        mgr:shutdown()
    }
--- response_body
shutting_down (connected): false
--- no_error_log
[error]

=== TEST 14: LPUSH with single value then LRANGE
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:lpush("single_list", "only")
        local vals = client:lrange("single_list", 0, -1)
        ngx.say("len: " .. #vals)
        ngx.say("val: " .. tostring(vals[1]))

        client:del("single_list")
        mgr:shutdown()
    }
--- response_body
len: 1
val: only
--- no_error_log
[error]
