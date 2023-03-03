#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local admin = require "resty.rocketmq.admin"
local utils = require("resty.rocketmq.utils")
local split = utils.split

local nameservers = { "127.0.0.1:9876" }

local adm, err = admin.new(nameservers)
if not adm then
    print("new admin client err:", err)
    return
end

local _, err = adm:createTopic("TopicTest", 8)
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

local res, err = p:batchSend {
    { topic = "TopicTest", body = "batch1", },
    { topic = "TopicTest", body = "batch2", }
}
if not res then
    print("send err:", err)
    return
end
print("send batch success: " .. cjson.encode(res.sendResult))

-----------------viewMessage
local msg, err = adm:viewMessage(split(res.sendResult.offsetMsgId, ',')[1])
if not msg then
    print("viewMessage err:", err)
    return
end
print("viewMessage: " .. cjson.encode(msg))
