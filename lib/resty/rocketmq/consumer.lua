local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local rebalancer = require("resty.rocketmq.consumer.rebalancer")
local RESPONSE_CODE = core.RESPONSE_CODE
local cjson_safe = require("cjson.safe")
local ngx = ngx
local ngx_timer_at = ngx.timer.at
local log = ngx.log
local ERR = ngx.ERR

---@class consumer
local _M = {}
_M.__index = _M

_M.BROADCASTING = 'BROADCASTING'
_M.CLUSTERING = 'CLUSTERING'

local defaults = {
    allocateMessageQueueStrategy = _M.AllocateMessageQueueAveragely,
    enableMsgTrace = false,
    customizedTraceTopic = core.RMQ_SYS_TRACE_TOPIC,
    useTLS = false,
    timeout = 3000,
    messageModel = _M.CLUSTERING,
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
        client = cli,
        consumeMessageHookList = {},
    }, _M)
    for k, default in pairs(defaults) do
        consumer[k] = default
    end
    return consumer
end

for k, default in pairs(defaults) do
    local setterFnName = 'set' .. k:sub(1, 1):upper() .. k:sub(2)
    _M[setterFnName] = function(self, value)
        self[k] = value or default
    end
end

function _M.registerConsumeMessageHook(self, hook)
    if type(hook) == 'table' and type(hook.consumeMessageBefore) == 'function' and type(hook.consumeMessageAfter) == 'function' then
        table.insert(self.consumeMessageHookList, hook)
    else
        error('hook should be functions')
    end
end

local function selectOneMessageQueue(topicPublishInfo)
    local index = topicPublishInfo.sendWhichQueue
    topicPublishInfo.sendWhichQueue = index + 1
    local pos = math.abs(index) % #topicPublishInfo.messageQueueList
    return topicPublishInfo.messageQueueList[pos + 1]
end

local function sendHeartbeatToAllBroker(self)
    local heartbeatData = {
        clientID = '' .. ngx.worker.pid(),
        producerDataSet = { { groupName = self.groupName } },
        consumerDataSet = setmetatable({}, cjson_safe.empty_array_mt)
    }
    for brokerName, brokers in pairs(self.client.brokerAddrTable) do
        local addr = brokers[0]
        if addr then
            local h, b, err = self.client:sendHeartbeat(addr, heartbeatData)
        end
    end
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
    self.rebalanceImpl = rebalancer.new()
end

function _M:start()
    local self = self
    setTraceDispatcher(self)
    sendHeartbeatToAllBroker(self)
    initRebalanceImpl(self)
    local loop
    loop = function()
        if self.exit then
            return
        end
        self.client:updateAllTopicRouteInfoFromNameserver()
        sendHeartbeatToAllBroker(self)
        ngx_timer_at(30, loop)
    end
    ngx_timer_at(10, loop)
    if self.traceDispatcher then
        self.traceDispatcher:start()
    end
end

function _M:stop()
    self.exit = true
    if self.traceDispatcher then
        self.traceDispatcher:stop()
    end
end

local function getSendResult(h, msg, mqSelected, err)
    if not h then
        return nil, err
    end
    if h.code ~= core.RESPONSE_CODE.SUCCESS then
        return nil, h.remark
    end
    return {
        messageQueue = {
            topic = msg.topic,
            brokerName = mqSelected.brokerName,
            queueId = tonumber(mqSelected.queueId),
        },
        offsetMsgId = h.extFields.msgId,
        msgId = msg.properties.UNIQ_KEY,
        queueOffset = tonumber(h.extFields.queueOffset),
    }
end

local function produce(self, msg)
    if not core.checkTopic(msg.topic) or not core.checkMessage(msg.body) then
        return nil, 'invalid topic or message'
    end
    local topicPublishInfo, err = self.client:tryToFindTopicPublishInfo(msg.topic)
    if not topicPublishInfo then
        return nil, err
    end
    local mqSelected = selectOneMessageQueue(topicPublishInfo)
    local brokerAddr = self.client:findBrokerAddressInPublish(mqSelected.brokerName, msg.topic)
    msg.queueId = mqSelected.queueId

    local context
    if #self.consumeMessageHookList > 0 then
        context = {
            producer = self,
            producerGroup = self.groupName,
            communicationMode = "SYNC",
            bornHost = "127.0.0.1",
            brokerAddr = brokerAddr,
            message = msg,
            mq = mqSelected,
            msgType = core.Normal_Msg,
        }
        if msg.properties.TRAN_MSG == 'true' then
            context.msgType = core.Trans_Msg_Half
        elseif msg.properties.DELAY then
            context.msgType = core.Delay_Msg
        end
        for _, hook in ipairs(self.consumeMessageHookList) do
            hook:sendMessageBefore(context)
        end
    end

    local h, _, err = self.client:sendMessage(brokerAddr, msg)
    local sendResult, err = getSendResult(h, msg, mqSelected, err)
    if #self.consumeMessageHookList > 0 then
        if sendResult then
            context.sendResult = sendResult
        else
            context.exception = err
        end
        for _, hook in ipairs(self.consumeMessageHookList) do
            hook:sendMessageAfter(context)
        end
    end

    if not sendResult then
        return nil, err
    end
    h.sendResult = sendResult
    return h
end

local function genMsg(groupName, topic, message, tags, keys, properties)
    properties = properties or {}
    return {
        producerGroup = groupName,
        topic = topic,
        defaultTopic = "TBW102",
        defaultTopicQueueNums = 4,
        sysFlag = 0,
        bornTimeStamp = ngx.now() * 1000,
        flag = 0,
        properties = {
            UNIQ_KEY = utils.genUniqId(),
            KEYS = keys,
            TAGS = tags,
            WAIT = properties.waitStoreMsgOk or 'true',
            DELAY = properties.delayTimeLevel,
        },
        reconsumeTimes = 0,
        unitMode = false,
        maxReconsumeTimes = 0,
        batch = false,
        body = message,
    }
end

function _M:send(topic, message, tags, keys, properties)
    return produce(self, genMsg(self.groupName, topic, message, tags, keys, properties))
end

function _M:setTransactionListener(transactionListener)
    if type(transactionListener.executeLocalTransaction) ~= 'function' then
        return nil, 'invalid callback'
    end
    self.transactionListener = transactionListener
end


-- todo add check callback
function _M:sendMessageInTransaction(topic, arg, message, tags, keys, properties)
    if not self.transactionListener then
        return nil, "TransactionListener is null"
    end
    local msg = genMsg(self.groupName, topic, message, tags, keys, properties)
    msg.properties.TRAN_MSG = 'true'
    msg.properties.PGROUP = self.groupName

    local h, err = produce(self, msg)
    if not h then
        return nil, err
    end
    local localTransactionState = core.TRANSACTION_NOT_TYPE
    if h.code == RESPONSE_CODE.SUCCESS then
        msg.properties.__transationId__ = h.transationId
        msg.transationId = msg.properties.UNIQ_KEY
        localTransactionState = self.transactionListener:executeLocalTransaction(msg, arg)
    else
        localTransactionState = core.TRANSACTION_ROLLBACK_TYPE
    end
    local _, commitLogOffset = utils.decodeMessageId(h.extFields.offsetMsgId or h.extFields.msgId)
    local brokerAddr = self.client:findBrokerAddressInPublish(h.sendResult.messageQueue.brokerName, topic)

    if #self.endTransactionHookList > 0 then
        local context = {
            producerGroup = self.groupName,
            brokerAddr = brokerAddr,
            message = msg,
            msgId = msg.properties.UNIQ_KEY,
            transactionId = msg.properties.UNIQ_KEY,
            transactionState = core.TRANSACTION_TYPE_MAP[localTransactionState],
            fromTransactionCheck = false,
        }
        for _, hook in ipairs(self.endTransactionHookList) do
            hook:endTransaction(context)
        end
    end

    self.client:endTransactionOneway(brokerAddr, {
        producerGroup = self.groupName,
        transationId = h.transationId,
        commitLogOffset = commitLogOffset,
        commitOrRollback = localTransactionState,
        tranStateTableOffset = h.extFields.queueOffset,
        msgId = h.extFields.msgId,
    })
    return h
end

function _M:batchSend(msgs)
    local first
    local batch_body = {}
    for _, m in ipairs(msgs) do
        if not core.checkTopic(m.topic) or not core.checkMessage(m.body) then
            return nil, 'invalid topic or message'
        end

        local msg = genMsg(self.groupName, m.topic, m.body, m.tags, m.keys, m.properties)
        if msg.properties.DELAY then
            return nil, 'TimeDelayLevel is not supported for batching'
        end

        if not first then
            first = msg
            if msg.topic:find(core.RETRY_GROUP_TOPIC_PREFIX, nil, true) == 1 then
                return nil, 'Retry Group is not supported for batching'
            end
        else
            if msg.topic ~= first.topic then
                return nil, 'The topic of the messages in one batch should be the same'
            end
            if msg.properties.WAIT ~= first.properties.WAIT then
                return nil, 'The waitStoreMsgOK of the messages in one batch should the same'
            end
        end
        table.insert(batch_body, core.encodeMsg(msg))
    end
    return produce(self, {
        producerGroup = self.groupName,
        topic = first.topic,
        defaultTopic = "TBW102",
        defaultTopicQueueNums = 4,
        sysFlag = 0,
        bornTimeStamp = ngx.now() * 1000,
        flag = 0,
        properties = {
            UNIQ_KEY = utils.genUniqId(),
            WAIT = first.waitStoreMsgOk,
        },
        reconsumeTimes = 0,
        unitMode = false,
        maxReconsumeTimes = 0,
        batch = true,
        body = table.concat(batch_body),
    })
end

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
return _M
