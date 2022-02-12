#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local consumer = require "resty.rocketmq.consumer"
local utils = require "resty.rocketmq.utils"

local cid = { 1, 2, 3, 4 }
for _, c in ipairs(cid) do
    local res = consumer.AllocateMessageQueueAveragely("", c, { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, cid)
    print(table.concat(res, ","))
end

print(utils.java_hash("32q"))

