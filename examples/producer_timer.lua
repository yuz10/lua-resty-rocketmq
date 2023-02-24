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

local res, err = p:send("TopicTest", "halo world", nil, nil, {
    TIMER_DELAY_SEC = "5",
    --TIMER_DELAY_MS = "5000",
    --TIMER_DELIVER_MS = ngx.now() * 1000 + 5000,
})
if not res then
    print("send err:", err)
    return
end

print("send success: " .. cjson.encode(res.sendResult))

