#!/usr/local/openresty/bin/resty

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local producer = require "resty.rocketmq.producer"
local admin = require "resty.rocketmq.admin"

--enable lmq in broker.conf:
--enableLmq = true
--enableMultiDispatch = true

local nameservers = { "127.0.0.1:9876" }

local topic = "TopicTest"
local liteTopic = "liteTopic1"
local group = "group1"
local brokerName = 'broker-0'
local clusterName = 'DefaultCluster'
local clientId = 'clientId1'

local adm, err = admin.new(nameservers)
local _, err = adm:createTopic(topic, 8, 0, {
    ["+message.type"] = "LITE"
})
if err then
    print("create topic err:", err)
    return
end

local _, err = adm:createGroup(clusterName, {
    groupName = group,
    attributes = {
        ["+lite.bind.topic"] = topic
    }
})
if err then
    print("create group err:", err)
    return
end

local p, err = producer.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end
local res, err = p:send(topic, "hello lmq", "tag", "key", {
    __LITE_TOPIC = liteTopic
})
if not res then
    print("send err:", err)
    return
end

local res, err = p.client:syncLiteSubscription(brokerName, {
    action = "PARTIAL_ADD",
    clientId = clientId,
    group = group,
    topic = topic,
    liteTopicSet = { liteTopic },
    offsetOption = {
        type = "POLICY",
        value = 1,
    },
    version = 0,
})
if not res then
    print("syncLiteSubscription err:", err)
    return
end

while true do
    local pullResult, err = p.client:popLiteMessage(brokerName, clientId, group, topic, 16, 5000, 1000, 0, "attempt0")
    if not pullResult then
        print("pop err:", err)
        ngx.sleep(1)
    else
        for _, msg in ipairs(pullResult.msgFoundList or {}) do
            print(cjson.encode(msg))
            p.client:doAck(topic, group, msg.properties['POP_CK'], liteTopic)
        end
    end
    print('-----')
end
