local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local rebalancer = require("resty.rocketmq.consumer.rebalancer")
local offsetstore = require("resty.rocketmq.consumer.offsetstore")
local admin = require("resty.rocketmq.admin")
local queue = require("resty.rocketmq.queue")
local bit = require("bit")

local cjson_safe = require("cjson.safe")
local ngx = ngx
local ngx_timer_at = ngx.timer.at
local split = utils.split
local bor = bit.bor

local log = ngx.log
local WARN = ngx.WARN
local ERR = ngx.ERR

---@class consumer
local _M = {}
_M.__index = _M

_M.BROADCASTING = 'BROADCASTING'
_M.CLUSTERING = 'CLUSTERING'

_M.CONSUME_FROM_LAST_OFFSET = 'CONSUME_FROM_LAST_OFFSET'
_M.CONSUME_FROM_FIRST_OFFSET = 'CONSUME_FROM_FIRST_OFFSET'
_M.CONSUME_FROM_TIMESTAMP = 'CONSUME_FROM_TIMESTAMP'

_M.CONSUME_SUCCESS = 'CONSUME_SUCCESS'
_M.RECONSUME_LATER = 'RECONSUME_LATER'

_M.AllocateMessageQueueAveragely = function(consumerGroup, currentCID, mqAll, cidAll)
    if not currentCID or currentCID == '' then
        return nil, 'currentCID is empty'
    end
    if not mqAll or #mqAll == 0 then
        return nil, 'mqAll is null or mqAll empty'
    end
    if not cidAll or #cidAll == 0 then
        return nil, 'cidAll is null or cidAll empty'
    end
    local index = -1
    for i, cid in ipairs(cidAll) do
        if cid == currentCID then
            index = i - 1
            break
        end
    end
    if index == -1 then
        log(ERR, ('[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s'):format(consumerGroup, currentCID, table.concat(cidAll, ',')))
        return {}
    end
    local mod = #mqAll % #cidAll
    local average
    if #mqAll <= #cidAll then
        average = 1
    elseif index < mod then
        average = math.floor(#mqAll / #cidAll) + 1
    else
        average = math.floor(#mqAll / #cidAll)
    end
    local start
    if index < mod then
        start = index * average
    else
        start = index * average + mod
    end
    local range = math.min(average, #mqAll - start)
    local res = {}
    for i = 1, range do
        table.insert(res, mqAll[start + i])
    end
    return res
end

local PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50
local BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15
local CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30

local defaults = {
    allocateMessageQueueStrategy = _M.AllocateMessageQueueAveragely,
    enableMsgTrace = false,
    customizedTraceTopic = core.RMQ_SYS_TRACE_TOPIC,
    useTLS = false,
    timeout = 3000,
    messageModel = _M.CLUSTERING,
    consumeFromWhere = _M.CONSUME_FROM_LAST_OFFSET,
    consumeTimestamp = 0,
    pullThresholdForQueue = 1000,
    pullThresholdSizeForQueue = 100,
    pullTimeDelayMillsWhenException = 3000,
    pullBatchSize = 32,
    pullInterval = 0,
    consumeMessageBatchMaxSize = 1,
    maxReconsumeTimes = 16,
}

function _M.new(nameservers, consumerGroup, rpcHook)
    local cli, err = client.new(nameservers)
    if not cli then
        return nil, err
    end
    ---@type consumer
    local consumer = setmetatable({
        nameservers = nameservers,
        consumerGroup = consumerGroup,
        rpcHook = rpcHook,
        clientID = '127.0.0.1@' .. ngx.worker.pid() .. '#' .. (ngx.now() * 1000),
        consumeMessageHookList = {},
    }, _M)
    for k, default in pairs(defaults) do
        consumer[k] = default
    end
    consumer.client = cli
    consumer.rebalancer = rebalancer.new(consumer)
    consumer.offsetStore = offsetstore.new(consumer)
    consumer.admin = admin.new(nameservers, cli)
    consumer.pullRequestQueue = queue.new()
    return consumer
end

for k, default in pairs(defaults) do
    local setterFnName = 'set' .. k:sub(1, 1):upper() .. k:sub(2)
    local getterFnName = 'get' .. k:sub(1, 1):upper() .. k:sub(2)
    _M[setterFnName] = function(self, value)
        self[k] = value or default
    end
    _M[getterFnName] = function(self)
        return self[k]
    end
end

function _M:registerMessageListener(messageListener)
    if type(messageListener.consumeMessage) ~= 'function' then
        return nil, 'invalid callback'
    end
    self.messageListener = messageListener
end

function _M.registerConsumeMessageHook(self, hook)
    if type(hook) == 'table' and type(hook.consumeMessageBefore) == 'function' and type(hook.consumeMessageAfter) == 'function' then
        table.insert(self.consumeMessageHookList, hook)
    else
        error('hook should be functions')
    end
end

local function sendHeartbeatToAllBroker(self, sock_map)
    local subscriptionDataSet = {}
    for _, v in pairs(self.rebalancer.subscriptionInner) do
        table.insert(subscriptionDataSet, v)
    end
    local heartbeatData = {
        clientID = '' .. self.clientID,
        producerDataSet = setmetatable({}, cjson_safe.empty_array_mt),
        consumerDataSet = {
            {
                groupName = self.consumerGroup,
                consumeType = "CONSUME_PASSIVELY",
                messageModel = self.messageModel,
                consumeFromWhere = self.consumeFromWhere,
                subscriptionDataSet = subscriptionDataSet,
                unitMode = false,
            }
        }
    }
    self.client:sendHeartbeatToAllBroker(sock_map, heartbeatData)
end

local function buildSubscriptionData(topic, subExpression)
    if subExpression == nil or subExpression == '' then
        subExpression = '*'
    end
    local subscriptionData = {
        topic = topic,
        subString = subExpression,
        tagsSet = setmetatable({}, cjson_safe.empty_array_mt),
        codeSet = setmetatable({}, cjson_safe.empty_array_mt),
        subVersion = ngx.now() * 1000,
    }
    for _, tag in ipairs(split(subExpression, '||')) do
        if #tag > 0 then
            table.insert(subscriptionData.tagsSet, tag)
            table.insert(subscriptionData.codeSet, utils.java_hash(tag))
        end
    end
    return subscriptionData
end

local function updateTopicSubscribeInfoWhenSubscriptionChanged(self)
    for topic, _ in pairs(self.rebalancer.subscriptionInner) do
        local res, err = self.client:updateTopicRouteInfoFromNameserver(topic)
        if err then
            log(WARN, 'fail to updateTopicRouteInfoFromNameserver:', err)
        end
    end
end

function _M:subscribe(topic, subExpression)
    local subscriptionData = buildSubscriptionData(topic, subExpression)
    self.rebalancer.subscriptionInner[topic] = subscriptionData
end

local function setTraceDispatcher(self)
    if self.enableMsgTrace then
        local traceDispatcher = trace.new(self.nameservers, trace.CONSUME, self.customizedTraceTopic)
        traceDispatcher.producer:setUseTLS(self.useTLS)
        traceDispatcher.producer:setTimeout(self.timeout)
        if self.rpcHook then
            traceDispatcher.producer:addRPCHook(self.rpcHook)
        end
        self:registerConsumeMessageHook(traceDispatcher.hook)
        traceDispatcher:start()
        self.traceDispatcher = traceDispatcher
    end
end

local function initRebalanceImpl(self)
    self.rebalancer:start()
end

local function copySubscription(self)
    self:subscribe(core.RETRY_GROUP_TOPIC_PREFIX .. self.consumerGroup, "*")
end

local function executePullRequestImmediately(self, pullRequest)
    queue.push(self.pullRequestQueue, pullRequest)
end

local function executePullRequestLater(self, pullRequest, timeDelay)
    ngx_timer_at(timeDelay / 1000, function()
        executePullRequestImmediately(self, pullRequest)
    end)
end

local function buildSysFlag(commitOffset, suspend, subscription, classFilter)
    local flag = 0
    if commitOffset then
        flag = bor(flag, 1)
    end
    if suspend then
        flag = bor(flag, 2)
    end
    if subscription then
        flag = bor(flag, 4)
    end
    if classFilter then
        flag = bor(flag, 8)
    end
    return flag
end

local function submitConsumeRequest(self, msgFoundList, processQueue, messageQueue)
    if processQueue.dropped then
        return
    end
    local messageListener = self.messageListener
    local consumeMessageBatchMaxSize = self.consumeMessageBatchMaxSize
    local context = {
        messageQueue = messageQueue
    }
    local i = 1
    while i <= #msgFoundList do
        local msgs = {}
        for j = 1, consumeMessageBatchMaxSize do
            if i <= #msgFoundList then
                table.insert(msgs, msgFoundList[i])
                i = i + 1
            else
                break
            end
        end
        -- todo ConsumeMessageHook
        local status = messageListener:consumeMessage(msgs, context)
        local delayLevel = context.delayLevelWhenNextConsume or 0
        if status ~= _M.CONSUME_SUCCESS then
            for _, msg in ipairs(msgs) do
                local _, err = self.client:sendMessageBack(messageQueue.brokerName, msg, self.consumerGroup, delayLevel, self.maxReconsumeTimes)
                if err then
                    ngx.log(ngx.ERR, err)
                end
            end
        end
    end
end

local function pullMessage(self, pullRequest)
    local processQueue = pullRequest.processQueue
    local messageQueue = pullRequest.messageQueue
    if processQueue.dropped then
        return
    end
    processQueue.lastPullTimestamp = ngx.now() * 1000
    local cachedMessageCount = processQueue.msgCount
    local cachedMessageSizeInMiB = processQueue.msgSize / (1024 * 1024)
    if cachedMessageCount > self.pullThresholdForQueue then
        executePullRequestLater(self, pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL)
        return
    end
    if cachedMessageSizeInMiB > self.pullThresholdSizeForQueue then
        executePullRequestLater(self, pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL)
        return
    end

    local sd = self.rebalancer.subscriptionInner[messageQueue.topic]
    if not sd then
        executePullRequestLater(self, pullRequest, self.pullTimeDelayMillsWhenException)
        return
    end
    local commitOffsetValue = self.offsetStore:readOffset(messageQueue, offsetstore.READ_FROM_MEMORY)
    local sysFlag = buildSysFlag(commitOffsetValue > 0, true, sd.subExpression, false)
    local pullResult, err = self.client:pullKernelImpl(messageQueue.brokerName, {
        consumerGroup = self.consumerGroup,
        topic = messageQueue.topic,
        queueId = messageQueue.queueId,
        queueOffset = pullRequest.nextOffset,
        maxMsgNums = self.pullBatchSize,
        sysFlag = sysFlag,
        commitOffset = commitOffsetValue,
        suspendTimeoutMillis = BROKER_SUSPEND_MAX_TIME_MILLIS,
        subscription = sd.subExpression,
        subVersion = sd.subVersion,
        expressionType = sd.expressionType
    }, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND)
    if err then
        executePullRequestLater(self, pullRequest, self.pullTimeDelayMillsWhenException)
        return
    end
    if pullResult.pullStatus == client.FOUND then
        pullRequest.nextOffset = pullResult.nextBeginOffset
        local msgFoundList = pullResult.msgFoundList
        if not msgFoundList or #msgFoundList == 0 then
            executePullRequestImmediately(self, pullRequest)
        else
            processQueue:putMessage(msgFoundList)
            submitConsumeRequest(self, msgFoundList, processQueue, messageQueue)
            if self.pullInterval > 0 then
                executePullRequestLater(self, pullRequest, self.pullInterval)
            else
                executePullRequestImmediately(self, pullRequest)
            end
        end
    elseif pullResult.pullStatus == client.NO_NEW_MSG or pullResult.pullStatus == client.NO_MATCHED_MSG then
        pullRequest.nextOffset = pullResult.nextBeginOffset
        if processQueue.msgCount == 0 then
            self.offsetStore:updateOffset(messageQueue, pullRequest.nextOffset, true)
        end
        executePullRequestImmediately(self, pullRequest)
    else
        pullRequest.processQueue.dropped = true
        self.offsetStore:updateOffset(messageQueue, pullRequest.nextOffset, false)
        self.offsetStore:persist(messageQueue)
        self.rebalancer:removeProcessQueue(messageQueue)
    end

end

function _M:start()
    local self = self
    if not self.messageListener then
        return nil, 'messageListener is null'
    end
    self.running = true
    setTraceDispatcher(self)
    copySubscription(self)
    initRebalanceImpl(self)
    updateTopicSubscribeInfoWhenSubscriptionChanged(self)
    ngx_timer_at(0, function()
        local sock_map = {}
        while self.running do
            self.client:updateAllTopicRouteInfoFromNameserver()
            sendHeartbeatToAllBroker(self, sock_map)
            ngx.sleep(30)
        end
    end)
    if self.traceDispatcher then
        self.traceDispatcher:start()
    end
    ngx_timer_at(0, function()
        while self.running do
            local pullRequest = queue.pop(self.pullRequestQueue)
            if pullRequest then
                pullMessage(self, pullRequest)
            else
                ngx.sleep(1)
            end
        end
    end)
end

function _M:stop()
    self.running = false
    if self.traceDispatcher then
        self.traceDispatcher:stop()
    end
end

function _M:removeUnnecessaryMessageQueue(mq, pq)
    self.offsetStore:persist(mq);
    self.offsetStore:removeOffset(mq);
end

function _M:removeDirtyOffset(mq)
    self.offsetStore:removeOffset(mq)
end

function _M:messageQueueChanged(topic, mqList, allocateResult)
    log(WARN, 'messageQueueChanged:', topic, ',allocateResult:', cjson_safe.encode(allocateResult))
end

function _M:computePullFromWhere(mq)
    local lastOffset, err = self.offsetStore:readOffset(mq, offsetstore.READ_FROM_STORE)
    lastOffset = lastOffset or -1
    if lastOffset >= 0 then
        return lastOffset
    elseif lastOffset <= -2 then
        return -1
    end
    if self.consumeFromWhere == _M.CONSUME_FROM_LAST_OFFSET then
        if utils.startsWith(mq.topic, core.RETRY_GROUP_TOPIC_PREFIX) then
            return 0
        else
            return self.admin:maxOffset(mq)
        end
    elseif self.consumeFromWhere == _M.CONSUME_FROM_FIRST_OFFSET then
        return 0
    else
        if utils.startsWith(mq.topic, core.RETRY_GROUP_TOPIC_PREFIX) then
            return self.admin:maxOffset(mq)
        else
            return self.admin:searchOffset(mq, self.consumeTimestamp)
        end
    end
end

function _M:dispatchPullRequest(pullRequestList)
    for _, pullRequest in ipairs(pullRequestList) do
        executePullRequestImmediately(self, pullRequest)
    end
end

return _M
