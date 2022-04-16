#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local client = require "resty.rocketmq.client"
local DLEDGER_REQUEST_CODE = {
    METADATA = 50000,
    APPEND = 50001,
    GET = 50002,
    VOTE = 51001,
    HEART_BEAT = 51002,
    PULL = 51003,
    PUSH = 51004,
    LEADERSHIP_TRANSFER = 51005,
}


local nameservers = { "127.0.0.1:9876" }
local p, err = client.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local function realTest(addr, code, h, body)
    local h, b, err = p:request(code, addr, h, body)
    if err then
        print('----err----', err)
    else
        print('----ok----', cjson.encode(h), b)
    end
    return h, b, err
end

--realTest("localhost:10001", DLEDGER_REQUEST_CODE.APPEND, {}, "1")
--realTest("localhost:10001", DLEDGER_REQUEST_CODE.PUSH, { index = 1 }, "1")
--realTest("localhost:10000", DLEDGER_REQUEST_CODE.PUSH, { index = 1 }, "1")

realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq1")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq2")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq3")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq4")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq5")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq6")
realTest("localhost:10000", DLEDGER_REQUEST_CODE.APPEND, {}, "1dafadewreq7")

realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 1 })
realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 2 })
realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 3 })
realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 4 })
realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 5 })
realTest("localhost:10000", DLEDGER_REQUEST_CODE.GET, { index = 6 })

realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 1 })
realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 2 })
realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 3 })
realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 4 })
realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 5 })
realTest("localhost:10001", DLEDGER_REQUEST_CODE.GET, { index = 6 })
