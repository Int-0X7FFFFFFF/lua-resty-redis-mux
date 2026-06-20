use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: blocking_strategy="error" -- BLPOP rejected
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "error"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:blpop("nonexistent_list", 1)
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))
        ngx.say("not_supported: " .. tostring(err and err:find("not supported") ~= nil))

        mgr:shutdown()
    }
--- response_body
res: nil
has_err: true
not_supported: true
--- no_error_log
[error]

=== TEST 2: blocking_strategy="error" -- SUBSCRIBE rejected
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "error"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:subscribe("channel")
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))
        ngx.say("not_supported: " .. tostring(err and err:find("not supported") ~= nil))

        mgr:shutdown()
    }
--- response_body
res: nil
has_err: true
not_supported: true
--- no_error_log
[error]

=== TEST 3: blocking_strategy="error" -- MULTI rejected
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "error"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:multi()
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))

        mgr:shutdown()
    }
--- response_body
res: nil
has_err: true
--- no_error_log
[error]

=== TEST 4: blocking_strategy="error" -- EXEC rejected
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "error"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:exec()
        ngx.say("res: " .. tostring(res))
        ngx.say("has_err: " .. tostring(err ~= nil))

        mgr:shutdown()
    }
--- response_body
res: nil
has_err: true
--- no_error_log
[error]

=== TEST 5: blocking_strategy="fork" -- BLPOP via fork (timeout expected)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "fork"})
        mgr:connect()
        local client = mgr:get_client()

        -- BLPOP on nonexistent list with 1s timeout should return null
        local res, err = client:blpop("fork_nonexistent_list", 1)
        ngx.say("res_is_null: " .. tostring(res == ngx.null or res == nil))

        mgr:shutdown()
    }
--- response_body
res_is_null: true
--- error_log
lua tcp socket read timed out

=== TEST 6: blocking_strategy="fork" -- BRPOP via fork
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "fork"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:brpop("brpop_nonexistent", 1)
        ngx.say("res_is_null: " .. tostring(res == ngx.null or res == nil))

        mgr:shutdown()
    }
--- response_body
res_is_null: true
--- error_log
lua tcp socket read timed out

=== TEST 7: blocking_strategy="fork" -- normal commands still work
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "fork"})
        mgr:connect()
        local client = mgr:get_client()

        -- After using fork for blocking command, normal commands should still work
        client:blpop("fork_after_list", 1)  -- fork path
        local v = client:set("fork_after", "works")  -- mux path
        ngx.say("mux after fork: " .. tostring(v))
        local r = client:get("fork_after")
        ngx.say("get after fork: " .. tostring(r))

        client:del("fork_after")
        mgr:shutdown()
    }
--- response_body
mux after fork: OK
get after fork: works
--- error_log
lua tcp socket read timed out

=== TEST 8: default blocking_strategy (fork)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        -- default blocking_strategy is "fork"
        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:blpop("default_fork_list", 1)
        -- with fork strategy, should not get "not supported" error
        ngx.say("not_error_msg: " .. tostring(not (err and err:find("not supported"))))

        mgr:shutdown()
    }
--- response_body
not_error_msg: true
--- error_log
lua tcp socket read timed out

=== TEST 9: blocking_strategy="fork" -- BRPOPLPUSH via fork
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port, blocking_strategy = "fork"})
        mgr:connect()
        local client = mgr:get_client()

        local res, err = client:brpoplpush("brpoplpush_src", "brpoplpush_dst", 1)
        ngx.say("res_is_null: " .. tostring(res == ngx.null or res == nil))

        mgr:shutdown()
    }
--- response_body
res_is_null: true
--- error_log
lua tcp socket read timed out
