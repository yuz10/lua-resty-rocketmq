#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"

local nameservers = { "127.0.0.1:9876" }

local p, err = producer.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local topic = "popTopic"
local group = "group1"
while true do
    local pullResult, err = p.client:pop({
        brokerName = 'broker-0',
        topic = topic,
        queueId = 0,
    }, 1000, 16, group, 5000, true, nil, "TAG", "*")
    if not pullResult then
        print("pop err:", err)
    else
        for _, msg in ipairs(pullResult.msgFoundList or {}) do
            print(cjson.encode(msg))
            p.client:ack(msg, group)
        end
    end
    print('-----')
end
