#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local admin = require "resty.rocketmq.admin"
local producer = require "resty.rocketmq.producer"
local core = require "resty.rocketmq.core"

local thread_num = 10
local msg_size = 120
local topic = "BenchmarkTest"
local trace_enable = false
local delay_level = nil
local use_tls = false
local batch_size = 1
local transaction = false
local ak = nil
local sk = nil

local function is(x)
    return x and 1 or 0
end
if is(batch_size > 1) + is(delay_level) + is(transaction) > 1 then
    print("only one of delay, transaction and batch is supported")
    return
end

local adm, err = admin.new({ "127.0.0.1:9876" })
adm:createTopic(topic, 8)

local total_success = 0
local total_error = 0
local running = true

local message_buffer = {}
for i = 1, msg_size / 10 do
    table.insert(message_buffer, '0123456789')
end
local message = table.concat(message_buffer)

local p = producer.new({ "127.0.0.1:9876" }, "produce_group", trace_enable)
p:start()
p:setTransactionListener({
    executeLocalTransaction = function(self, msg, arg)
        return core.TRANSACTION_COMMIT_TYPE
    end
})
p:setUseTLS(use_tls)
if ak and sk then
    local aclHook = require("resty.rocketmq.acl_rpchook").new(ak, sk)
    p:addRPCHook(aclHook)
end

for i = 1, thread_num do
    ngx.thread.spawn(function()
        while running do
            local res, err
            if transaction then
                res, err = p:sendMessageInTransaction(topic, message, "tag", "key")
            elseif batch_size == 1 then
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
    print("tps=", (success - last_success) / (time - last_time), ',total_error=', total_error)
    last_time = time
    last_success = success
end
running = false
p:stop()
