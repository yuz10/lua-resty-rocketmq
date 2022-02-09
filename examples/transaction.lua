#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local core = require "resty.rocketmq.core"

local nameservers = { "127.0.0.1:9876" }

local p, err = producer.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end
p:start()

p:setTransactionListener({
    executeLocalTransaction = function(self, msg, arg)
        return core.TRANSACTION_NOT_TYPE
    end,
    checkLocalTransaction = function(self, msg)
        print('check:', cjson.encode(msg))
        return core.TRANSACTION_COMMIT_TYPE
    end
})

while true do
    local res, err = p:sendMessageInTransaction("TopicTest", nil, "halo world")
    if not res then
        print("send err:", err)
    else
        print("send success: " .. cjson.encode(res.sendResult))
    end

    ngx.sleep(10)
end



