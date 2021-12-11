local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local RESPONSE_CODE = core.RESPONSE_CODE
local cjson_safe = require("cjson.safe")

---@class producer
local _M = {}
_M.__index = _M

function _M.new(nameservers, groupName, enableMsgTrace)
    local cli, err = client.new(nameservers)
    if not cli then
        return nil, err
    end
    ---@type producer
    local producer = setmetatable({
        client = cli,
        groupName = groupName or "DEFAULT_PRODUCER",
        sendMessageHookList = {},
        endTransactionHookList = {},
    }, _M)
    if enableMsgTrace then
        producer.traceDispatcher = trace.new(nameservers, trace.PRODUCE)
        producer:registerSendMessageHook(producer.traceDispatcher.hook)
        producer:registerEndTransactionHook(producer.traceDispatcher.hook)
    end
    return producer
end

function _M.addRPCHook(self, hook)
    self.client:addRPCHook(hook)
    if self.traceDispatcher then
        self.traceDispatcher.producer:addRPCHook(hook)
    end
end

function _M.setUseTLS(self, useTLS)
    self.client:setUseTLS(useTLS)
    if self.traceDispatcher then
        self.traceDispatcher.producer:setUseTLS(useTLS)
    end
end

function _M.setTimeout(self, timeout)
    self.client:setTimeout(timeout)
    if self.traceDispatcher then
        self.traceDispatcher.producer:setTimeout(timeout)
    end
end

function _M.registerSendMessageHook(self, hook)
    if type(hook) == 'table' and type(hook.sendMessageBefore) == 'function' and type(hook.sendMessageAfter) == 'function' then
        table.insert(self.sendMessageHookList, hook)
    else
        error('hook should be functions')
    end
end

function _M.registerEndTransactionHook(self, hook)
    if type(hook) == 'table' and type(hook.endTransaction) == 'function' then
        table.insert(self.endTransactionHookList, hook)
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

function _M:start()
    local self = self
    sendHeartbeatToAllBroker(self)
    local loop
    loop = function()
        if self.exit then
            return
        end
        self.client:updateAllTopicRouteInfoFromNameserver()
        sendHeartbeatToAllBroker(self)
        ngx.timer.at(30, loop)
    end
    ngx.timer.at(30, loop)
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
    if #self.sendMessageHookList > 0 then
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
        for _, hook in ipairs(self.sendMessageHookList) do
            hook:sendMessageBefore(context)
        end
    end

    local h, _, err = self.client:sendMessage(brokerAddr, msg)
    local sendResult, err = getSendResult(h, msg, mqSelected, err)
    if #self.sendMessageHookList > 0 then
        if sendResult then
            context.sendResult = sendResult
        else
            context.exception = err
        end
        for _, hook in ipairs(self.sendMessageHookList) do
            hook:sendMessageAfter(context)
        end
    end

    if not sendResult then
        return nil, err
    end
    h.sendResult = sendResult
    return h
end

local function genMsg(topic, message, tags, keys, properties)
    properties = properties or {}
    return {
        producerGroup = self.groupName,
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
    return produce(self, genMsg(topic, message, tags, keys, properties))
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
    local msg = genMsg(topic, message, tags, keys, properties)
    msg.properties.TRANS_MSG = 'true'
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

return _M
