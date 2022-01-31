#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local producer = require "resty.rocketmq.producer"
local admin = require "resty.rocketmq.admin"

local nameservers = { "127.0.0.1:9876" }

local adm, err = admin.new({ "127.0.0.1:9876" })
adm:createTopic("TBW102", "TopicTest", 8)

local message = "halo world"
local p, err = producer.new(nameservers)
if not p then
    ngx.say("create producer err:", err)
    return
end

local res, err = p:send("TopicTest", message)
if not res then
    ngx.say("send err:", err)
    return
end

ngx.say("send success: " .. require("cjson").encode(res.sendResult))

-----------------viewMessage
local adm, err = admin.new(nameservers)
if not adm then
    ngx.say("new admin client err:", err)
    return
end
local msg, err = adm:viewMessage(res.sendResult.offsetMsgId)
if not msg then
    ngx.say("viewMessage err:", err)
    return
end
ngx.say("viewMessage: " .. require("cjson").encode(msg))
