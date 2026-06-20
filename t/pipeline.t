use t::Test;

repeat_each(1);
no_long_string();
plan tests => repeat_each() * (3 * blocks());

no_long_string();
run_tests();

__DATA__

=== TEST 1: basic pipeline (SET + GET)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:init_pipeline(2)
        client:set("pipe_basic", "hello")
        client:get("pipe_basic")
        local results, err = client:commit_pipeline()
        ngx.say("results: " .. tostring(type(results) == "table"))
        if results then
            ngx.say("num results: " .. #results)
            ngx.say("r1: " .. tostring(results[1]))
            ngx.say("r2: " .. tostring(results[2]))
        end

        client:del("pipe_basic")
        mgr:shutdown()
    }
--- response_body
results: true
num results: 2
r1: OK
r2: hello
--- no_error_log
[error]

=== TEST 2: pipeline with multiple SETs and GETs
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:init_pipeline(4)
        client:set("k1", "v1")
        client:set("k2", "v2")
        client:get("k1")
        client:get("k2")
        local results = client:commit_pipeline()
        ngx.say("num: " .. #results)
        ngx.say("r1: " .. tostring(results[1]))
        ngx.say("r2: " .. tostring(results[2]))
        ngx.say("r3: " .. tostring(results[3]))
        ngx.say("r4: " .. tostring(results[4]))

        client:del("k1")
        client:del("k2")
        mgr:shutdown()
    }
--- response_body
num: 4
r1: OK
r2: OK
r3: v1
r4: v2
--- no_error_log
[error]

=== TEST 3: cancel_pipeline
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:init_pipeline()
        client:set("pipe_cancelled", "yes")
        client:cancel_pipeline()
        local val = client:get("pipe_cancelled")
        ngx.say("after cancel, key exists: " .. tostring(val ~= ngx.null))

        mgr:shutdown()
    }
--- response_body
after cancel, key exists: false
--- no_error_log
[error]

=== TEST 4: commit_pipeline without init_pipeline
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        local results, err = client:commit_pipeline()
        ngx.say("results: " .. tostring(results))
        ngx.say("has_err: " .. tostring(err ~= nil))

        mgr:shutdown()
    }
--- response_body
results: nil
has_err: true
--- no_error_log
[error]

=== TEST 5: large pipeline (20+ commands)
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        local n = 20
        client:init_pipeline(n)
        for i = 1, 10 do
            client:set("lp_" .. i, "val_" .. i)
        end
        for i = 1, 10 do
            client:get("lp_" .. i)
        end
        local results = client:commit_pipeline()
        ngx.say("num results: " .. #results)
        local all_ok = true
        for i = 1, 10 do
            if results[i] ~= "OK" then all_ok = false end
        end
        for i = 11, 20 do
            if results[i] ~= "val_" .. (i - 10) then all_ok = false end
        end
        ngx.say("all_ok: " .. tostring(all_ok))

        for i = 1, 10 do
            client:del("lp_" .. i)
        end
        mgr:shutdown()
    }
--- response_body
num results: 20
all_ok: true
--- no_error_log
[error]

=== TEST 6: init_pipeline with capacity hint
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:init_pipeline(100)
        client:ping()
        client:ping()
        local results = client:commit_pipeline()
        ngx.say("num: " .. #results)
        ngx.say("r1: " .. tostring(results[1]))
        ngx.say("r2: " .. tostring(results[2]))

        mgr:shutdown()
    }
--- response_body
num: 2
r1: PONG
r2: PONG
--- no_error_log
[error]

=== TEST 7: pipeline with Redis error inside
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:set("str_key", "val")
        client:init_pipeline(2)
        client:get("str_key")
        client:lpush("str_key", "item")  -- wrong type error
        local results = client:commit_pipeline()
        ngx.say("num: " .. #results)
        ngx.say("r1: " .. tostring(results[1]))
        ngx.say("r2_type: " .. tostring(type(results[2]) == "table"))
        if type(results[2]) == "table" then
            ngx.say("r2_is_err: " .. tostring(results[2][1] == false))
        end

        client:del("str_key")
        mgr:shutdown()
    }
--- response_body
num: 2
r1: val
r2_type: true
r2_is_err: true
--- no_error_log
[error]

=== TEST 8: multiple pipelines from same client
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        -- first pipeline
        client:init_pipeline(1)
        client:set("mp1", "a")
        local r1 = client:commit_pipeline()

        -- second pipeline
        client:init_pipeline(1)
        client:get("mp1")
        local r2 = client:commit_pipeline()

        ngx.say("p1[1]: " .. tostring(r1[1]))
        ngx.say("p2[1]: " .. tostring(r2[1]))

        client:del("mp1")
        mgr:shutdown()
    }
--- response_body
p1[1]: OK
p2[1]: a
--- no_error_log
[error]

=== TEST 9: pipeline with SET and DEL
--- global_config eval: $::GlobalConfig
--- server_config
    content_by_lua_block {
        local redis_mux = require "resty.redis_mux"
        local redis_port = tonumber(os.getenv("TEST_NGINX_REDIS_PORT")) or 6379

        local mgr = redis_mux.new({port = redis_port})
        mgr:connect()
        local client = mgr:get_client()

        client:set("pd_key", "will_delete")
        client:init_pipeline(3)
        client:get("pd_key")
        client:del("pd_key")
        client:get("pd_key")
        local results = client:commit_pipeline()
        ngx.say("r1: " .. tostring(results[1]))
        ngx.say("r2: " .. tostring(results[2]))
        ngx.say("r3_null: " .. tostring(results[3] == ngx.null))

        mgr:shutdown()
    }
--- response_body
r1: will_delete
r2: 1
r3_null: true
--- no_error_log
[error]
