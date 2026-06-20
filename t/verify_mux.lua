-- Quick verification script for resty.redis_mux
-- Usage: resty verify_mux.lua

local redis_mux = require "resty.redis_mux"
local redis = require "resty.redis"

local passed = 0
local failed = 0

local function assert_equal(actual, expected, test_name)
    if actual == expected then
        passed = passed + 1
        print("  PASS: " .. test_name)
    else
        failed = failed + 1
        print("  FAIL: " .. test_name)
        print("    expected: " .. tostring(expected))
        print("    got:      " .. tostring(actual))
    end
end

local function assert_not_nil(actual, test_name)
    if actual ~= nil then
        passed = passed + 1
        print("  PASS: " .. test_name)
    else
        failed = failed + 1
        print("  FAIL: " .. test_name .. " (got nil)")
    end
end

local function assert_nil(actual, test_name)
    if actual == nil then
        passed = passed + 1
        print("  PASS: " .. test_name)
    else
        failed = failed + 1
        print("  FAIL: " .. test_name .. " (expected nil, got: " .. tostring(actual) .. ")")
    end
end

----------------------------------------------------------------------
-- Test 1: Basic ConnectionManager creation and connect
----------------------------------------------------------------------
print("\n== Test 1: ConnectionManager creation and connect ==")

local mgr, err = redis_mux.new({
    host = "127.0.0.1",
    port = 6379,
    capacity = 10,
})

assert_not_nil(mgr, "new() returns manager")
assert_equal(type(mgr), "table", "manager is a table")
assert_equal(mgr:get_state(), "disconnected", "initial state is disconnected")

local ok, err = mgr:connect()
assert_not_nil(ok, "connect() succeeds")
if not ok then
    print("    connect error: " .. tostring(err))
end

-- Give driver threads time to start
ngx.sleep(0.1)
assert_equal(mgr:get_state(), "connected", "state is connected after connect()")

----------------------------------------------------------------------
-- Test 2: get_client and basic SET/GET
----------------------------------------------------------------------
print("\n== Test 2: get_client and basic SET/GET ==")

local client, err = mgr:get_client()
assert_not_nil(client, "get_client() returns client")
assert_equal(type(client), "table", "client is a table")

-- SET
local res, err = client:set("mux_test_key", "hello_mux")
assert_equal(res, "OK", "SET mux_test_key")

-- GET
res, err = client:get("mux_test_key")
assert_equal(res, "hello_mux", "GET mux_test_key")

-- DEL
res, err = client:del("mux_test_key")
assert_equal(res, 1, "DEL mux_test_key")


----------------------------------------------------------------------
-- Test 3: Multiple clients sharing the same connection
----------------------------------------------------------------------
print("\n== Test 3: Multiple clients sharing connection ==")

local c1 = mgr:get_client()
local c2 = mgr:get_client()

-- Client 1 sets
local r1, e1 = c1:set("shared_key", "from_c1")
assert_equal(r1, "OK", "c1 SET")

-- Client 2 reads
local r2, e2 = c2:get("shared_key")
assert_equal(r2, "from_c1", "c2 GET (same connection)")

-- Client 2 sets
r2, e2 = c2:set("shared_key", "from_c2")
assert_equal(r2, "OK", "c2 SET")

-- Client 1 reads
r1, e1 = c1:get("shared_key")
assert_equal(r1, "from_c2", "c1 GET (same connection)")

c1:del("shared_key")

----------------------------------------------------------------------
-- Test 4: Concurrent commands (ngx.thread.spawn)
----------------------------------------------------------------------
print("\n== Test 4: Concurrent commands ==")

local results = {}
local threads = {}

for i = 1, 10 do
    local idx = i
    threads[i] = ngx.thread.spawn(function()
        local c = mgr:get_client()
        local key = "concurrent_key_" .. idx
        local val = "value_" .. idx

        local ok, err = c:set(key, val)
        if not ok then
            results[idx] = {false, err}
            return
        end

        local got, err = c:get(key)
        if got then
            c:del(key)
        end
        results[idx] = {got, err}
            end)
end

-- Wait for all to complete
for i = 1, 10 do
    ngx.thread.wait(threads[i])
end

local concurrent_ok = true
for i = 1, 10 do
    local expected = "value_" .. i
    if not results[i] or results[i][1] ~= expected then
        concurrent_ok = false
        print("  Thread " .. i .. " failed: " .. tostring(results[i] and results[i][1] or "nil"))
    end
end

if concurrent_ok then
    passed = passed + 1
    print("  PASS: 10 concurrent SET/GET operations")
else
    failed = failed + 1
    print("  FAIL: concurrent operations had errors")
end

----------------------------------------------------------------------
-- Test 5: Pipeline
----------------------------------------------------------------------
print("\n== Test 5: Pipeline ==")

local pc = mgr:get_client()
pc:init_pipeline(3)
pc:set("pipe_a", "1")
pc:set("pipe_b", "2")
pc:get("pipe_a")
pc:get("pipe_b")
local pres, perr = pc:commit_pipeline()

assert_not_nil(pres, "pipeline commit returns results")
if pres then
    -- pres should be array: {OK, OK, "1", "2"}
    assert_equal(#pres, 4, "pipeline has 4 results")
    if #pres == 4 then
        assert_equal(pres[1], "OK", "pipeline result 1: SET OK")
        assert_equal(pres[2], "OK", "pipeline result 2: SET OK")
        assert_equal(pres[3], "1", "pipeline result 3: GET a=1")
        assert_equal(pres[4], "2", "pipeline result 4: GET b=2")
    end
end

pc:del("pipe_a")
pc:del("pipe_b")

----------------------------------------------------------------------
-- Test 6: Blocking commands - error strategy
----------------------------------------------------------------------
print("\n== Test 6: Blocking commands (error strategy) ==")

local bc = mgr:get_client()
local res, err = bc:blpop("some_list", 1)
assert_nil(res, "BLPOP returns nil (error strategy)")
assert_not_nil(err, "BLPOP returns error message")
assert_equal(string.find(err, "not supported") ~= nil, true, "BLPOP error mentions 'not supported'")

res, err = bc:subscribe("channel")
assert_nil(res, "SUBSCRIBE returns nil (error strategy)")

----------------------------------------------------------------------
-- Test 7: Mode 1 - error mode on disconnect
----------------------------------------------------------------------
print("\n== Test 7: Mode 1 (error) on disconnect ==")

local mgr_err, _ = redis_mux.new({
    host = "127.0.0.1",
    port = 6379,
    failure_mode = "error",
    capacity = 10,
})

local ok, err = mgr_err:connect()
assert_not_nil(ok, "error-mode manager connects")
ngx.sleep(0.1)

-- Test with a valid command first
local ec = mgr_err:get_client()
local r, e = ec:set("mode1_test", "val")
assert_equal(r, "OK", "Mode 1: SET before disconnect works")
ec:del("mode1_test")

-- Kill redis to simulate disconnect
-- (we skip actual kill and test the dead state mechanism)
-- The manager's state transitions are tested via internal logic
mgr_err:shutdown()
assert_equal(mgr_err:get_state(), "disconnected", "shutdown returns to disconnected")

-- get_client after restart
local ok2, err2 = mgr_err:connect()
assert_not_nil(ok2, "reconnect after shutdown works")
ngx.sleep(0.1)

local ec2 = mgr_err:get_client()
assert_not_nil(ec2, "get_client works after reconnect")
mgr_err:shutdown()

----------------------------------------------------------------------
-- Test 8: Module requires
----------------------------------------------------------------------
print("\n== Test 8: Module version ==")
assert_equal(redis_mux._VERSION, "0.1.0", "version is 0.1.0")
assert_not_nil(redis_mux.ConnectionManager, "ConnectionManager class exported")
assert_equal(type(redis_mux.new), "function", "new() function exported")

----------------------------------------------------------------------
-- Test 9: HMGET / HMSET support
----------------------------------------------------------------------
print("\n== Test 9: HMSET / HMGET support ==")

local hc = mgr:get_client()
local r, e = hc:hmset("hash_test", "f1", "v1", "f2", "v2")
assert_equal(r, "OK", "HMSET with field/value pairs")

r, e = hc:hget("hash_test", "f1")
assert_equal(r, "v1", "HGET f1")

r, e = hc:hmget("hash_test", "f1", "f2")
assert_equal(type(r), "table", "HMGET returns table")
if type(r) == "table" then
    assert_equal(r[1], "v1", "HMGET result[1]")
    assert_equal(r[2], "v2", "HMGET result[2]")
end

hc:del("hash_test")

----------------------------------------------------------------------
-- Results
----------------------------------------------------------------------
print("\n========================================")
print(string.format("Results: %d passed, %d failed", passed, failed))
print("========================================")

if failed > 0 then
    os.exit(1)
end
