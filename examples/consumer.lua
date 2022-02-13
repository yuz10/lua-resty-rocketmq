#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson_safe = require("cjson.safe")
local consumer = require "resty.rocketmq.consumer"

local c = consumer.new({ "127.0.0.1:9876" }, "group1")
c:subscribe("TopicTest")
c:registerMessageListener({
    consumeMessage = function(self, msgs, context)
        print('consume:', cjson_safe.encode(msgs))
        return consumer.CONSUME_SUCCESS
    end
})

c:start()

while true do
    ngx.sleep(100)
end
