#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local producer = require "resty.rocketmq.producer"
local core = require "resty.rocketmq.core"

local nameservers = { "127.0.0.1:9876" }

local message = "halo world"
local p, err = producer.new(nameservers)
if not p then
    ngx.say("create producer err:", err)
    return
end
p:start()

p:setTransactionListener({
    executeLocalTransaction = function(self, msg, arg)
        return core.TRANSACTION_COMMIT_TYPE
    end
})

while true do
    local res, err = p:sendMessageInTransaction("TopicTest", nil, message)
    if not res then
        ngx.say("send err:", err)
    else
        ngx.say("send success: " .. require("cjson").encode(res.sendResult))
    end

    ngx.sleep(1)
end



