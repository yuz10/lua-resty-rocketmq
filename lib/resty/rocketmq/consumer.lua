local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local rebalancer = require("resty.rocketmq.consumer.rebalancer")
local offsetstore = require("resty.rocketmq.consumer.offsetstore")
local admin = require("resty.rocketmq.admin")
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

_M.ALLOCATE_MESSAGE_QUEUE_STRATEGY_NAME = {
    [_M.AllocateMessageQueueAveragely] = "AVG"
}

local PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50
local BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15
local CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30
local MAX_POP_INVISIBLE_TIME = 300000
local MIN_POP_INVISIBLE_TIME = 5000
local popDelayLevel = { 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200 }

local defaults = {
    allocateMessageQueueStrategy = _M.AllocateMessageQueueAveragely,
    enableMsgTrace = false,
    customizedTraceTopic = core.RMQ_SYS_TRACE_TOPIC,
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
    clientRebalance = true,
    popThresholdForQueue = 96,
    popInvisibleTime = 60000,
    popBatchNums = 32,
}

function _M.new(nameservers, consumerGroup)
    local cli, err = client.new(nameservers)
    if not cli then
        return nil, err
    end
    ---@type consumer
    local consumer = setmetatable({
        nameservers = nameservers,
        consumerGroup = consumerGroup,
        clientID = '127.0.0.1@' .. ngx.worker.pid() .. '#' .. (ngx.now() * 1000),
        consumeMessageHookList = {},
        pullThreads = {},
        popThreads = {},
    }, _M)
    for k, default in pairs(defaults) do
        consumer[k] = default
    end
    consumer.client = cli
    consumer.rebalancer = rebalancer.new(consumer)
    consumer.offsetStore = offsetstore.new(consumer)
    consumer.admin = admin.new(nameservers, cli)
    return consumer
end

for k, v in pairs(defaults) do
    local getMove = type(v) == 'boolean' and "is" or "get"
    local setterFnName = 'set' .. k:sub(1, 1):upper() .. k:sub(2)
    local getterFnName = getMove .. k:sub(1, 1):upper() .. k:sub(2)
    _M[setterFnName] = function(self, value)
        if self.running then
            return nil, 'cant set property after start'
        end
        self[k] = value
        return true
    end
    _M[getterFnName] = function(self)
        return self[k]
    end
end

function _M.addRPCHook(self, hook)
    self.client:addRPCHook(hook)
end

function _M.setUseTLS(self, useTLS)
    self.client:setUseTLS(useTLS)
end

function _M.setTimeout(self, timeout)
    self.client:setTimeout(timeout)
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
        producerDataSet = setmetatable({}, cjson_safe.array_mt),
        consumerDataSet = setmetatable({
            {
                groupName = self.consumerGroup,
                consumeType = "CONSUME_PASSIVELY",
                messageModel = self.messageModel,
                consumeFromWhere = self.consumeFromWhere,
                subscriptionDataSet = subscriptionDataSet,
                unitMode = false,
            }
        }, cjson_safe.array_mt)
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
        tagsSet = setmetatable({}, cjson_safe.array_mt),
        codeSet = setmetatable({}, cjson_safe.array_mt),
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
        traceDispatcher.producer:setUseTLS(self.client.useTLS)
        traceDispatcher.producer:setTimeout(self.client.timeout)
        for _, hook in ipairs(self.client.RPCHook) do
            traceDispatcher.producer:addRPCHook(hook)
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

local function isPopTimeout(extraInfo)
    local extraInfoStrs = split(extraInfo, ' ')
    local popTime = tonumber(extraInfoStrs[2])
    local invisibleTime = tonumber(extraInfoStrs[3])
    if ngx.now() * 1000 - popTime >= invisibleTime then
        return true
    end
    return false
end

local function changePopInvisibleTime(self, msg, consumerGroup, delayLevel)
    if delayLevel == 0 then
        delayLevel = msg.reconsumeTimes or 0
    end
    delayLevel = delayLevel + 1
    local delaySecond = delayLevel > #popDelayLevel and popDelayLevel[#popDelayLevel] or popDelayLevel[delayLevel]
    local extraInfo = msg.properties['POP_CK']
    self.client:changePopInvisibleTime(msg.topic, consumerGroup, extraInfo, delaySecond * 1000)
end

local function checkNeedAckOrDelay(self, msg)
    local msgDelaytime = ngx.now() * 1000 - msg.bornTimestamp
    if msgDelaytime > popDelayLevel[#popDelayLevel] * 2 * 1000 then
        self.client:ack(msg, self.consumerGroup)
    else
        local delayLevel = 0
        for i = #popDelayLevel, 1, -1 do
            if msgDelaytime >= popDelayLevel[i] * 1000 then
                delayLevel = i + 1
                break
            end
        end
        changePopInvisibleTime(self, msg, self.consumerGroup, delayLevel)
        log(WARN, ('Consume too many times, but delay time %s not enough. changePopInvisibleTime to delayLevel: %s'):format(msgDelaytime, delayLevel))
    end
end

local function submitPopConsumeRequest(self, msgFoundList, processQueue, messageQueue)
    if processQueue.dropped then
        return
    end
    local messageListener = self.messageListener
    local consumeMessageBatchMaxSize = self.consumeMessageBatchMaxSize
    local context = {
        ackIndex = 0x7fffffff,
        messageQueue = messageQueue,
        delayLevelWhenNextConsume = 0,
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
        local extraInfo = msgs[1].properties["POP_CK"]
        if isPopTimeout(extraInfo) then
            log(WARN, 'the pop message time out so abort consume')
            processQueue:incFoundMsg(-#msgs)
            return
        end
        
        local consumeMessageContext
        if #self.consumeMessageHookList > 0 then
            consumeMessageContext = {
                consumerGroup = self.consumerGroup,
                mq = messageQueue,
                msgList = msgs,
                success = false,
            }
            for _, hook in ipairs(self.consumeMessageHookList) do
                hook:consumeMessageBefore(consumeMessageContext)
            end
        end
        local now = tostring(ngx.now() * 1000)
        for _, msg in ipairs(msgs) do
            msg.properties["CONSUME_START_TIME"] = now
        end
        local status = messageListener:consumeMessage(msgs, context)
        status = status or _M.RECONSUME_LATER
        if #self.consumeMessageHookList > 0 then
            consumeMessageContext.status = status
            consumeMessageContext.success = status == _M.CONSUME_SUCCESS
            consumeMessageContext.consumeContextType = status == _M.CONSUME_SUCCESS and 0 or 1
            for _, hook in ipairs(self.consumeMessageHookList) do
                hook:consumeMessageAfter(consumeMessageContext)
            end
        end
        if processQueue.dropped or isPopTimeout(extraInfo) then
            log(WARN, 'the pop message time out so abort consume')
            processQueue:incFoundMsg(-#msgs)
            return
        end
        local ackIndex = context.ackIndex
        if status == _M.CONSUME_SUCCESS then
            if ackIndex > #msgs then
                ackIndex = #msgs
            end
        else
            ackIndex = 0
        end
        for i = 1, ackIndex do
            self.client:ack(msgs[i], self.consumerGroup)
            processQueue:ack()
        end
        for i = ackIndex + 1, #msgs do
            local msg = msgs[i]
            processQueue:ack()
            if msg.reconsumeTimes >= self.maxReconsumeTimes then
                checkNeedAckOrDelay(msg)
            else
                local delayLevel = context.delayLevelWhenNextConsume
                changePopInvisibleTime(self, msg, self.consumerGroup, delayLevel);
            end
        end
    
    end
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
        local consumeMessageContext
        if #self.consumeMessageHookList > 0 then
            consumeMessageContext = {
                consumerGroup = self.consumerGroup,
                mq = messageQueue,
                msgList = msgs,
                success = false,
            }
            for _, hook in ipairs(self.consumeMessageHookList) do
                hook:consumeMessageBefore(consumeMessageContext)
            end
        end
        local status = messageListener:consumeMessage(msgs, context)
        status = status or _M.RECONSUME_LATER
        if #self.consumeMessageHookList > 0 then
            consumeMessageContext.status = status
            consumeMessageContext.success = status == _M.CONSUME_SUCCESS
            consumeMessageContext.consumeContextType = status == _M.CONSUME_SUCCESS and 0 or 1
            for _, hook in ipairs(self.consumeMessageHookList) do
                hook:consumeMessageAfter(consumeMessageContext)
            end
        end
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
    processQueue:removeMessage(msgFoundList)
end

local function pullMessage(self, messageQueue, processQueue)
    processQueue.lastPullTimestamp = ngx.now() * 1000
    local cachedMessageCount = processQueue.msgCount
    local cachedMessageSizeInMiB = processQueue.msgSize / (1024 * 1024)
    if cachedMessageCount > self.pullThresholdForQueue then
        log(WARN, 'reached msg count limit ', cjson_safe.encode(messageQueue))
        ngx.sleep(PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL / 1000)
        return
    end
    if cachedMessageSizeInMiB > self.pullThresholdSizeForQueue then
        log(WARN, 'reached msg size limit ', cjson_safe.encode(messageQueue))
        ngx.sleep(PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL / 1000)
        return
    end
    
    local sd = self.rebalancer.subscriptionInner[messageQueue.topic]
    if not sd then
        log(WARN, "no subscription for ", cjson_safe.encode(messageQueue))
        ngx.sleep(self.pullTimeDelayMillsWhenException / 1000)
        return
    end
    local commitOffsetValue = self.offsetStore:readOffset(messageQueue, offsetstore.READ_FROM_MEMORY)
    local sysFlag = utils.buildSysFlag(commitOffsetValue > 0, true, sd.subExpression, false)
    local pullResult, err = self.client:pullKernelImpl(messageQueue.brokerName, {
        consumerGroup = self.consumerGroup,
        topic = messageQueue.topic,
        queueId = messageQueue.queueId,
        queueOffset = processQueue.nextOffset,
        maxMsgNums = self.pullBatchSize,
        sysFlag = sysFlag,
        commitOffset = commitOffsetValue,
        suspendTimeoutMillis = BROKER_SUSPEND_MAX_TIME_MILLIS,
        subscription = sd.subExpression,
        subVersion = sd.subVersion,
        expressionType = sd.expressionType
    }, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND)
    if err then
        ngx.sleep(self.pullTimeDelayMillsWhenException / 1000)
        return
    end
    if pullResult.pullStatus == client.FOUND then
        processQueue.nextOffset = pullResult.nextBeginOffset
        local msgFoundList = pullResult.msgFoundList
        if not msgFoundList or #msgFoundList == 0 then
            return
        else
            processQueue:putMessage(msgFoundList)
            submitConsumeRequest(self, msgFoundList, processQueue, messageQueue)
            self.offsetStore:updateOffset(messageQueue, pullResult.maxOffset, true)
            if self.pullInterval > 0 then
                ngx.sleep(self.pullInterval / 1000)
            end
            return
        end
    elseif pullResult.pullStatus == client.NO_NEW_MSG or pullResult.pullStatus == client.NO_MATCHED_MSG then
        processQueue.nextOffset = pullResult.nextBeginOffset
        if processQueue.msgCount == 0 then
            self.offsetStore:updateOffset(messageQueue, processQueue.nextOffset, true)
        end
    else
        processQueue.dropped = true
        self.offsetStore:updateOffset(messageQueue, processQueue.nextOffset, false)
        self.offsetStore:persist(messageQueue)
        self.rebalancer:removeProcessQueue(messageQueue)
        ngx.log(ngx.WARN, 'pullStatus ', pullResult.pullStatus, ',removeProcessQueue ', utils.buildMqKey(messageQueue))
    end
end

local function popMessage(self, messageQueue, processQueue)
    processQueue.lastPopTimestamp = ngx.now() * 1000
    if processQueue.waitAckCounter > self.popThresholdForQueue then
        log(WARN, 'the messages waiting to ack exceeds the threshold, so do flow control')
        ngx.sleep(PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL / 1000)
        return
    end
    local sd = self.rebalancer.subscriptionInner[messageQueue.topic]
    if not sd then
        log(WARN, "no subscription for ", cjson_safe.encode(messageQueue))
        ngx.sleep(self.pullTimeDelayMillsWhenException / 1000)
        return
    end
    
    local invisibleTime = self.popInvisibleTime
    if invisibleTime < MIN_POP_INVISIBLE_TIME or invisibleTime > MAX_POP_INVISIBLE_TIME then
        invisibleTime = 60000
    end
    
    local initMode = self.consumeFromWhere == _M.CONSUME_FROM_FIRST_OFFSET and client.INIT_MODE_MIN or client.INIT_MODE_MAX
    local popResult, err = self.client:pop(messageQueue, invisibleTime, self.popBatchNums, self.consumerGroup,
            BROKER_SUSPEND_MAX_TIME_MILLIS, true, initMode, sd.expressionType, sd.expression)
    if err then
        ngx.sleep(self.pullTimeDelayMillsWhenException / 1000)
        return
    end
    if popResult.popStatus == client.FOUND then
        local msgFoundList = popResult.msgFoundList
        if not msgFoundList or #msgFoundList == 0 then
            return
        else
            processQueue:incFoundMsg(#msgFoundList)
            submitPopConsumeRequest(self, msgFoundList, processQueue, messageQueue)
            if self.pullInterval > 0 then
                ngx.sleep(self.pullInterval / 1000)
            end
            return
        end
    elseif popResult.popStatus == client.NO_NEW_MSG or popResult.popStatus == client.POLLING_NOT_FOUND then
        return
    else
        ngx.sleep(self.pullTimeDelayMillsWhenException / 1000)
        return
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
    ngx.timer.every(5, function()
        for mqKey, _ in pairs(self.rebalancer.processQueueTable) do
            local mq = utils.buildMq(mqKey)
            self.offsetStore:persist(mq)
        end
    end)
    if self.traceDispatcher then
        self.traceDispatcher:start()
    end

end

function _M:stop()
    self.running = false
    if self.traceDispatcher then
        self.traceDispatcher:stop()
    end
end

function _M:removeUnnecessaryMessageQueue(mq, pq)
    self.offsetStore:persist(mq)
    self.offsetStore:removeOffset(mq)
end

function _M:removeDirtyOffset(mq)
    self.offsetStore:removeOffset(mq)
end

function _M:messageQueueChanged(topic, mqList, allocateResult)
    for mqKey, _ in pairs(self.rebalancer.processQueueTable) do
        if not self.pullThreads[mqKey] then
            ngx_timer_at(0, function(_, mqKey)
                local messageQueue = utils.buildMq(mqKey)
                while self.running do
                    local processQueue = self.rebalancer.processQueueTable[mqKey]
                    if not processQueue then
                        local mqKeys = ''
                        for mqKey, _ in pairs(self.rebalancer.processQueueTable) do
                            mqKeys = mqKeys .. mqKey .. ';'
                        end
                        log(WARN, 'nil processQueue ', mqKey, ',avail:', mqKeys)
                        break
                    end
                    if processQueue.dropped then
                        log(WARN, 'dropped ', mqKey)
                        break
                    end
                    pullMessage(self, messageQueue, processQueue)
                end
                self.pullThreads[mqKey] = nil
            end, mqKey)
            self.pullThreads[mqKey] = true
        end
    end
    
    for mqKey, _ in pairs(self.rebalancer.popProcessQueueTable) do
        if not self.popThreads[mqKey] then
            ngx_timer_at(0, function(_, mqKey)
                local messageQueue = utils.buildMq(mqKey)
                while self.running do
                    local processQueue = self.rebalancer.popProcessQueueTable[mqKey]
                    if not processQueue then
                        local mqKeys = ''
                        for mqKey, _ in pairs(self.rebalancer.popProcessQueueTable) do
                            mqKeys = mqKeys .. mqKey .. ';'
                        end
                        log(WARN, 'nil popProcessQueueTable ', mqKey, ',avail:', mqKeys)
                        break
                    end
                    if processQueue.dropped then
                        log(WARN, 'dropped ', mqKey)
                        break
                    end
                    popMessage(self, messageQueue, processQueue)
                end
                self.popThreads[mqKey] = nil
            end, mqKey)
            self.popThreads[mqKey] = true
        end
    end
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

return _M
