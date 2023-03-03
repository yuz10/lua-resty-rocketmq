#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local utils = require("resty.rocketmq.utils")

local nameservers = { "127.0.0.1:9876" }

--enable lmq in broker.conf:
--enableLmq = true
--enableMultiDispatch = true

local p, err = producer.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local res, err = p:send("TopicTest", "hello lmq", "tag", "key", {
    INNER_MULTI_DISPATCH = "%LMQ%123,%LMQ%456"
})
if not res then
    print("send err:", err)
    return
end

ngx.sleep(0.1)
-----------------consumeMsg
print("send success: " .. cjson.encode(res.sendResult))
local mq = res.sendResult.messageQueue

for _, topic in ipairs { "%LMQ%123", "%LMQ%456" } do
    local pullResult, err = p.client:pullKernelImpl(mq.brokerName, {
        consumerGroup = "group1",
        topic = topic,
        queueId = 0,
        queueOffset = 0,
        maxMsgNums = 16,
        sysFlag = utils.buildSysFlag(false, false, true, false),
        commitOffset = 0,
        suspendTimeoutMillis = 5000,
        subscription = "*",
        subVersion = 0,
        expressionType = "TAG"
    }, 5000)
    if not pullResult then
        print("send err:", err)
        return
    end
    for _, msg in ipairs(pullResult.msgFoundList) do
        print(cjson.encode(msg))
    end
    print('-----')
end
