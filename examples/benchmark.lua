#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local admin = require "resty.rocketmq.admin"
local producer = require "resty.rocketmq.producer"

local adm, err = admin.new({ "127.0.0.1:9876" })
adm:createTopic("TBW102", "TopicTest", 8)

local thread_num = 10
local msg_size = 120
local topic = "TopicTest"
local trace_enable = false
local delay_level = nil
local use_tls = false
local batch_size = 1

if batch_size > 1 and delay_level then
    ngx.say("delay_level not supported in batch")
    return
end

local total_success = 0
local total_error = 0
local running = true
for i = 1, thread_num do
    ngx.thread.spawn(function()
        local message_buffer = {}
        for i = 1, msg_size / 10 do
            table.insert(message_buffer, '0123456789')
        end
        local message = table.concat(message_buffer)
        local p = producer.new({ "127.0.0.1:9876" }, "produce_group", trace_enable)
        p:setUseTLS(use_tls)
        while running do
            local res, err
            if batch_size == 1 then
                res, err = p:send(topic, message, "tag", "key", { delayTimeLevel = delay_level })
            else
                local msgs = {}
                for i = 1, batch_size do
                    table.insert(msgs, { topic = topic, body = message, tags = "tag", keys = "key" })
                end
                res, err = p:batchSend(msgs)
            end
            if res then
                total_success = total_success + batch_size
            else
                total_error = total_error + 1
            end
        end
    end)
end

local last_time = ngx.now()
local last_success = total_success
for i = 1, 10 do
    ngx.sleep(1)
    ngx.update_time()
    local time = ngx.now()
    local success = total_success
    ngx.say("tps=", (success - last_success) / (time - last_time), ',total_error=', total_error)
    ngx.flush()
    last_time = time
    last_success = success
end
running = false
