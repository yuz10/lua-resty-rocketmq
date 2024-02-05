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

for i = 1, 20 do
    local orderId = i % 3
    local res, err = p:send("test", tostring(i), nil, tostring(orderId), nil, function(mqList, msg)
        local orderId = tonumber(msg.properties.KEYS)
        return mqList[(orderId % #mqList) + 1]
    end)
    if not res then
        print("send err:", err)
        return
    end
    print("send success: ", i, ' orderId=', orderId, ' ', cjson.encode(res.sendResult))
end



