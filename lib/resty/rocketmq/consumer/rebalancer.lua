local core = require("resty.rocketmq.core")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local cjson_safe = require("cjson.safe")
local split = require("resty.rocketmq.utils").split
local ngx = ngx
local ngx_timer_at = ngx.timer.at
local log = ngx.log
local INFO = ngx.INFO
local WARN = ngx.WARN
local ERR = ngx.ERR
local RESPONSE_CODE = core.RESPONSE_CODE

---@class rebalancer
local _M = {}
_M.__index = _M

function _M.new(consumer)
    local rebalancer = setmetatable({
        consumer = consumer,
        client = consumer.client,
        clientID = consumer.clientID,
        topicSubscribeInfoTable = {},
        subscriptionInner = {},
        processQueueTable = {},
    }, _M)
    return rebalancer
end

local function buildMqKey(mq)
    return mq.topic .. '##' .. mq.brokerName .. '##' .. mq.queueId
end

local function buildMq(mqKey)
    local spl = split(mqKey, '##')
    return {
        topic = spl[1],
        brokerName = spl[2],
        queueId = tonumber(spl[3]),
    }
end

local function updateProcessQueueTableInRebalance(self, topic, allocateResultSet)
    local changed = false
    for mqKey, pq in pairs(self.processQueueTable) do
        local mq = buildMq(mqKey)
        if mq.topic == topic then
            if not allocateResultSet[mqKey] then
                self.consumer:removeUnnecessaryMessageQueue(mq, pq)
                changed = true
            end
        end
    end

    for mqKey, _ in pairs(allocateResultSet) do
        if not self.processQueueTable[mqKey] then
            local mq = buildMq(mqKey)
            self.consumer:removeDirtyOffset(mq)
            self.processQueueTable[mqKey] = {}
            changed = true
        end
    end
    return changed
end

local function rebalanceByTopic(self, topic)
    -- todo support BROADCASTING
    local mqList = self.client.topicSubscribeInfoTable[topic]
    local cidAll, err = self.client:findConsumerIdList(topic, self.consumer.consumerGroup);
    if not mqList or not cidAll then
        log(WARN, ("doRebalance, %s %s, get consumer id list failed:%s"):format(self.consumer.consumerGroup, topic, err))
        return
    end
    table.sort(mqList, function(a, b)
        if a.topic ~= b.topic then
            return a.topic < b.topic
        end
        if a.brokerName ~= b.brokerName then
            return a.brokerName < b.brokerName
        end
        return a.queueId < b.queueId
    end)
    table.sort(cidAll)
    print(cjson_safe.encode(self))
    local allocateResult, err = self.consumer.allocateMessageQueueStrategy(self.consumer.consumerGroup, self.clientId, mqList, cidAll)
    if not allocateResult then
        log(ERR, "allocateMessageQueueStrategy failed,", err)
        return
    end
    local allocateResultSet = {}
    for _, mq in ipairs(allocateResult) do
        allocateResultSet[buildMqKey(mq)] = true
    end
    local changed = updateProcessQueueTableInRebalance(self, topic, allocateResultSet)
    if changed then
        log(INFO, ("rebalanced result changed. group=%s, topic=%s, clientId=%s, mqAllSize=%s, cidAllSize=%s, rebalanceResultSize=%s, rebalanceResultSet=%s"):format(
                self.consumer.consumerGroup, topic, self.clientId, #mqList, #cidAll, #allocateResult, cjson_safe.encode(allocateResult)))
        self.consumer:messageQueueChanged(topic, mqList, allocateResult)
    end
end

local function doRebalance(self)
    for topic, _ in pairs(self.subscriptionInner) do
        rebalanceByTopic(self, topic)
    end
end

function _M:start()
    local self = self
    local loop
    loop = function()
        if self.exit then
            return
        end
        doRebalance(self)
        ngx_timer_at(20, loop)
    end
    ngx_timer_at(1, loop)
end

return _M
