use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: basic SET and GET
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        local ok, err = client:set("mux_test_key", "hello_mux")
        ngx.say("set: " .. tostring(ok))

        local val, err = client:get("mux_test_key")
        ngx.say("get: " .. tostring(val))

        client:del("mux_test_key")
        mgr:shutdown()
    }
--- response_body
set: OK
get: hello_mux
--- no_error_log
[error]

=== TEST 2: multiple commands in sequence
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("k1", "v1")
        client:set("k2", "v2")
        local r1 = client:get("k1")
        local r2 = client:get("k2")
        ngx.say("k1: " .. tostring(r1))
        ngx.say("k2: " .. tostring(r2))

        client:del("k1")
        client:del("k2")
        mgr:shutdown()
    }
--- response_body
k1: v1
k2: v2
--- no_error_log
[error]

=== TEST 3: integer commands (INCR / DECR)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("counter", "10")
        local v1 = client:incr("counter")
        ngx.say("incr: " .. tostring(v1))
        local v2 = client:decr("counter")
        ngx.say("decr: " .. tostring(v2))

        client:del("counter")
        mgr:shutdown()
    }
--- response_body
incr: 11
decr: 10
--- no_error_log
[error]

=== TEST 4: nil bulk reply (GET nonexistent key)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        local val = client:get("nonexistent_key_xyz")
        ngx.say("is_null: " .. tostring(val == ngx.null))

        mgr:shutdown()
    }
--- response_body
is_null: true
--- no_error_log
[error]

=== TEST 5: Redis error reply (wrong type operation)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("string_key", "value")
        local res, err = client:lpush("string_key", "item")
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))

        client:del("string_key")
        mgr:shutdown()
    }
--- response_body
res: false
has_err: true
--- no_error_log
[error]

=== TEST 6: list operations (LPUSH / LRANGE)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:lpush("mylist", "a")
        client:lpush("mylist", "b")
        client:lpush("mylist", "c")
        local vals = client:lrange("mylist", 0, -1)
        ngx.say("len: " .. #vals)
        for i, v in ipairs(vals) do
            ngx.say("val[" .. i .. "]: " .. v)
        end

        client:del("mylist")
        mgr:shutdown()
    }
--- response_body
len: 3
val[1]: c
val[2]: b
val[3]: a
--- no_error_log
[error]

=== TEST 7: hash operations (HSET / HGET)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:hset("myhash", "field1", "val1")
        local v = client:hget("myhash", "field1")
        ngx.say("hget: " .. tostring(v))

        client:del("myhash")
        mgr:shutdown()
    }
--- response_body
hget: val1
--- no_error_log
[error]

=== TEST 8: multiple clients sharing same connection
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local c1 = mgr:get_client()
        local c2 = mgr:get_client()

        c1:set("shared_key", "from_c1")
        local r2 = c2:get("shared_key")
        ngx.say("c2 reads after c1 set: " .. tostring(r2))

        c2:set("shared_key", "from_c2")
        local r1 = c1:get("shared_key")
        ngx.say("c1 reads after c2 set: " .. tostring(r1))

        c1:del("shared_key")
        mgr:shutdown()
    }
--- response_body
c2 reads after c1 set: from_c1
c1 reads after c2 set: from_c2
--- no_error_log
[error]

=== TEST 9: set_timeout and set_timeouts are no-ops
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        -- should not crash
        client:set_timeout(100)
        client:set_timeouts(100, 200, 300)
        -- verify still works
        local v = client:ping()
        ngx.say("ping after no-ops: " .. tostring(v))

        mgr:shutdown()
    }
--- response_body
ping after no-ops: PONG
--- no_error_log
[error]

=== TEST 10: repeated GET to verify connection reuse
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("reuse_key", "reuse_val")
        local ok = true
        for i = 1, 5 do
            local v = client:get("reuse_key")
            if v ~= "reuse_val" then
                ok = false
                break
            end
        end
        ngx.say("all 5 gets ok: " .. tostring(ok))

        client:del("reuse_key")
        mgr:shutdown()
    }
--- response_body
all 5 gets ok: true
--- no_error_log
[error]

=== TEST 11: PING command
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        local res = client:ping()
        ngx.say("ping: " .. tostring(res))

        mgr:shutdown()
    }
--- response_body
ping: PONG
--- no_error_log
[error]

=== TEST 12: special characters in key and value
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        local key = "key with spaces and unicode"
        local val = "value\nwith\nnewlines"
        client:set(key, val)
        local got = client:get(key)
        ngx.say("roundtrip ok: " .. tostring(got == val))

        client:del(key)
        mgr:shutdown()
    }
--- response_body
roundtrip ok: true
--- no_error_log
[error]

=== TEST 13: EXISTS command
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("exist_key", "1")
        local e1 = client:exists("exist_key")
        ngx.say("exists after set: " .. tostring(e1))
        client:del("exist_key")
        local e2 = client:exists("exist_key")
        ngx.say("exists after del: " .. tostring(e2))

        mgr:shutdown()
    }
--- response_body
exists after set: 1
exists after del: 0
--- no_error_log
[error]

=== TEST 14: EXPIRE and TTL
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("expire_key", "val")
        client:expire("expire_key", 3600)
        local ttl = client:ttl("expire_key")
        ngx.say("ttl > 0: " .. tostring(ttl > 0))

        client:del("expire_key")
        mgr:shutdown()
    }
--- response_body
ttl > 0: true
--- no_error_log
[error]

=== TEST 15: MGET command
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05, drain_timeout = 5})
        mgr:connect()
        local client = mgr:get_client()

        client:set("mg_a", "1")
        client:set("mg_b", "2")
        local vals = client:mget("mg_a", "mg_b", "mg_c")
        ngx.say("num: " .. #vals)
        ngx.say("v1: " .. tostring(vals[1]))
        ngx.say("v2: " .. tostring(vals[2]))
        ngx.say("v3_null: " .. tostring(vals[3] == ngx.null))

        client:del("mg_a")
        client:del("mg_b")
        mgr:shutdown()
    }
--- response_body
num: 3
v1: 1
v2: 2
v3_null: true
--- no_error_log
[error]
