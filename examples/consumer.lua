#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local consumer = require "resty.rocketmq.consumer"

local c = consumer.new({ "127.0.0.1:9876" }, "group1")
c:subscribe("TopicTest")
c:start()

while true do
    ngx.sleep(100)
end
