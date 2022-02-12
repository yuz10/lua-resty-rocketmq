#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local admin = require "resty.rocketmq.admin"

local nameservers = { "127.0.0.1:9876" }
local adm, err = admin.new(nameservers)
if not adm then
    print("new admin client err:", err)
    return
end
adm:createTopic("TopicTest", 8)
adm:createTopic("Trace", 8)

local p, err = producer.new(nameservers, "group", true, "Trace")
if not p then
    print("create producer err:", err)
    return
end
-- must call start() to send trace
p:start()

local res, err = p:send("TopicTest", "halo world")
if not res then
    print("send err:", err)
    return
end

print("send success: " .. cjson.encode(res.sendResult))
-- call stop() to wait to finish trace sending
p:stop()

-----------------viewMessage
local msg, err = adm:queryTraceByMsgId("Trace", res.sendResult.msgId)
if not msg then
    print("queryTraceByMsgId err:", err)
    return
end
print("queryTraceByMsgId: " .. cjson.encode(msg))
