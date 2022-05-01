#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local client = require "resty.rocketmq.client"
local core = require "resty.rocketmq.core"
local broker = require "resty.rocketmq.server.broker"
local REQUEST_CODE = core.REQUEST_CODE
local bit = require("bit")
local bor = bit.bor

local nameservers = { "127.0.0.1:9876" }
local p, err = client.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local function realTest(code, h, body)
    local h, b, err = p:request(code, "localhost:10911", h, body)
    if err then
        print('----err----', err)
    else
        print('----ok----', cjson.encode(h), b)
    end
end

local n = broker.new({
    clusterName = "DefaultCluster",
    brokerName = 'broker-a',
    brokerId = 0,
    brokerAddr = 'localhost:10911',
})              .processor
local function mockTest(code, h, body)
    local sock = { send = function(self, data)
        data = string.sub(data, 5)
        local res, header_len = core.decodeHeader(data)
        print(cjson.encode(res), '----', string.sub(data, header_len + 5))
    end }
    n:processRequest(sock, nil, { code = code, extFields = h }, body)
end

realTest(REQUEST_CODE.GET_BROKER_RUNTIME_INFO, {})

realTest(REQUEST_CODE.UPDATE_AND_CREATE_TOPIC, {
    topic = "topic1",
    readQueueNums = 1,
    writeQueueNums = 1,
    perm = bor(core.PERM_READ, core.PERM_WRITE),
})

realTest(REQUEST_CODE.SEND_MESSAGE_V2, {
})

