use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: two concurrent clients
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local results = {}
        local t1 = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:set("cc_key1", "val1")
            local v = c:get("cc_key1")
            return v
        end)
        local t2 = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:set("cc_key2", "val2")
            local v = c:get("cc_key2")
            return v
        end)

        local ok1, res1 = ngx.thread.wait(t1)
        local ok2, res2 = ngx.thread.wait(t2)
        ngx.say("t1: " .. tostring(res1))
        ngx.say("t2: " .. tostring(res2))

        local c = mgr:get_client()
        c:del("cc_key1")
        c:del("cc_key2")
        mgr:shutdown()
    }
--- response_body
t1: val1
t2: val2
--- no_error_log
[error]

=== TEST 2: five concurrent clients
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local threads = {}
        for i = 1, 5 do
            threads[i] = ngx.thread.spawn(function(idx)
                local c = mgr:get_client()
                local key = "cc5_" .. idx
                c:set(key, "v" .. idx)
                local v = c:get(key)
                c:del(key)
                return v
            end, i)
        end

        local all_ok = true
        for i = 1, 5 do
            local ok, res = ngx.thread.wait(threads[i])
            if res ~= "v" .. i then
                all_ok = false
            end
        end
        ngx.say("all_ok: " .. tostring(all_ok))

        mgr:shutdown()
    }
--- response_body
all_ok: true
--- no_error_log
[error]

=== TEST 3: ten concurrent clients
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local threads = {}
        for i = 1, 10 do
            threads[i] = ngx.thread.spawn(function(idx)
                local c = mgr:get_client()
                local key = "cc10_key_" .. idx
                local val = "cc10_val_" .. idx
                local ok, err = c:set(key, val)
                if not ok then return nil, err end
                local got = c:get(key)
                c:del(key)
                return got
            end, i)
        end

        local all_ok = true
        for i = 1, 10 do
            local ok, res = ngx.thread.wait(threads[i])
            if res ~= "cc10_val_" .. i then
                all_ok = false
                ngx.say("thread " .. i .. " failed: " .. tostring(res))
            end
        end
        ngx.say("all_ok: " .. tostring(all_ok))

        mgr:shutdown()
    }
--- response_body
all_ok: true
--- no_error_log
[error]

=== TEST 4: concurrent read/write interleaving
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        -- pre-set a known value
        local c = mgr:get_client()
        c:set("rw_key", "initial")

        -- Spawn a writer and wait for it to complete
        local tw = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:set("rw_key", "updated_by_writer")
            return "write_done"
        end)

        local ok_w, res_w = ngx.thread.wait(tw)
        -- After writer completes, the value should be updated
        local r = c:get("rw_key")
        ngx.say("writer: " .. tostring(res_w))
        ngx.say("final value: " .. tostring(r))

        c:del("rw_key")
        mgr:shutdown()
    }
--- response_body
writer: write_done
final value: updated_by_writer
--- no_error_log
[error]

=== TEST 5: ring buffer capacity -- capacity=10 with 15 concurrent
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, capacity = 10, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local threads = {}
        for i = 1, 15 do
            threads[i] = ngx.thread.spawn(function(idx)
                local c = mgr:get_client()
                local key = "cap_key_" .. idx
                c:set(key, "v" .. idx)
                local v = c:get(key)
                c:del(key)
                return v
            end, i)
        end

        local count = 0
        for i = 1, 15 do
            local ok, res = ngx.thread.wait(threads[i])
            if res == "v" .. i then
                count = count + 1
            end
        end
        ngx.say("success: " .. count .. "/15")

        mgr:shutdown()
    }
--- response_body
success: 15/15
--- no_error_log
[error]

=== TEST 6: capacity=1 edge case
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, capacity = 1, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local c = mgr:get_client()
        c:set("cap1_key", "val")
        local v = c:get("cap1_key")
        ngx.say("capacity=1 set/get: " .. tostring(v))

        c:del("cap1_key")
        mgr:shutdown()
    }
--- response_body
capacity=1 set/get: val
--- no_error_log
[error]

=== TEST 7: different command types in parallel threads
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()

        local t1 = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:set("mixed_str", "hello")
            return c:get("mixed_str")
        end)
        local t2 = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:lpush("mixed_list", "item1")
            c:lpush("mixed_list", "item2")
            return c:llen("mixed_list")
        end)
        local t3 = ngx.thread.spawn(function()
            local c = mgr:get_client()
            c:hset("mixed_hash", "f", "v")
            return c:hget("mixed_hash", "f")
        end)

        local _, r1 = ngx.thread.wait(t1)
        local _, r2 = ngx.thread.wait(t2)
        local _, r3 = ngx.thread.wait(t3)
        ngx.say("string: " .. tostring(r1))
        ngx.say("list_len: " .. tostring(r2))
        ngx.say("hash: " .. tostring(r3))

        local c = mgr:get_client()
        c:del("mixed_str")
        c:del("mixed_list")
        c:del("mixed_hash")
        mgr:shutdown()
    }
--- response_body
string: hello
list_len: 2
hash: v
--- no_error_log
[error]

=== TEST 8: response ordering preservation
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379
        local redis_host = os.getenv("TEST_NGINX_REDIS_HOST") or "127.0.0.1"

        local mgr = redis_mux.new({host = redis_host, port = redis_port, fork_idle_timeout = 100, drain_poll_interval = 0.05})
        mgr:connect()
        local c = mgr:get_client()

        -- sequential commands from same client must return in order
        local r1 = c:set("ord_key", "1")
        local r2 = c:get("ord_key")
        local r3 = c:set("ord_key", "2")
        local r4 = c:get("ord_key")
        ngx.say("r1: " .. tostring(r1))
        ngx.say("r2: " .. tostring(r2))
        ngx.say("r3: " .. tostring(r3))
        ngx.say("r4: " .. tostring(r4))

        c:del("ord_key")
        mgr:shutdown()
    }
--- response_body
r1: OK
r2: 1
r3: OK
r4: 2
--- no_error_log
[error]
