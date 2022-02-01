#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local admin = require "resty.rocketmq.admin"

local nameservers = { "127.0.0.1:9876" }

local adm, err = admin.new({ "127.0.0.1:9876" })
if not adm then
    print("new admin client err:", err)
    return
end

local _, err = adm:createTopic("TBW102", "TopicTest", 8)
if err then
    print("create topic err:", err)
    return
end

local p, err = producer.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local res, err = p:send("TopicTest", "halo world")
if not res then
    print("send err:", err)
    return
end

print("send success: " .. cjson.encode(res.sendResult))

-----------------viewMessage
local msg, err = adm:viewMessage(res.sendResult.offsetMsgId)
if not msg then
    print("viewMessage err:", err)
    return
end
print("viewMessage: " .. cjson.encode(msg))
