local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local RESPONSE_CODE = core.RESPONSE_CODE

local _M = {}
_M.__index = _M

function _M.new(nameservers, groupName)
    local cli, err = client.new(nameservers)
    if not cli then
        return nil, err
    end
    return setmetatable({
        client = cli,
        groupName = groupName,
    }, _M)
end

function _M.addRPCHook(self, hook)
    self.client:addRPCHook(hook)
end

function _M.setUseTLS(self, useTLS)
    self.client:setUseTLS(useTLS)
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
        consumerDataSet = {
            {
                groupName = self.groupName,
                consumerType = self.groupName,
                messageModel = self.groupName,
                consumerFromWhere = self.groupName,
                subscriptionDataSet = self.groupName,
                unitMode = self.groupName,
            }
        }
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
end

function _M:stop()
    self.exit = true
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
    local h, _, err = self.client:sendMessage(brokerAddr, msg)
    if not h then
        return nil, err
    end
    if h.code ~= core.RESPONSE_CODE.SUCCESS then
        return nil, h.remark
    end
    h.sendResult = {
        messageQueue = {
            topic = msg.topic,
            brokerName = mqSelected.brokerName,
            queueId = tonumber(mqSelected.queueId),
        },
        offsetMsgId = h.extFields.msgId,
        queueOffset = tonumber(h.extFields.queueOffset),
    }
    return h
end

function _M:produce(topic, message, tags, keys, waitStoreMsgOk)
    return produce(self, {
        producerGroup = self.groupName,
        topic = topic,
        defaultTopic = "TBW102",
        defaultTopicQueueNums = 4,
        sysFlag = 0,
        bornTimeStamp = ngx.now() * 1000,
        flag = 0,
        properties = {
            UNIQ_KEY = utils.uuid(),
            KEYS = keys,
            TAGS = tags,
            WAIT = waitStoreMsgOk or 'true',
        },
        reconsumeTimes = 0,
        unitMode = false,
        maxReconsumeTimes = 0,
        batch = false,
        body = message,
    })
end


-- todo add check callback
function _M:transactionProduce(topic, execute, arg, message, tags, keys, waitStoreMsgOk)
    if type(execute) ~= 'function' then
        return nil, 'invalid callback'
    end
    local msg = {
        producerGroup = self.groupName,
        topic = topic,
        defaultTopic = "TBW102",
        defaultTopicQueueNums = 4,
        sysFlag = 0,
        bornTimeStamp = ngx.now() * 1000,
        flag = 0,
        properties = {
            UNIQ_KEY = utils.uuid(),
            KEYS = keys,
            TAGS = tags,
            WAIT = waitStoreMsgOk or 'true',
            TRANS_MSG = 'true',
            PGROUP = self.groupName,
        },
        reconsumeTimes = 0,
        unitMode = false,
        maxReconsumeTimes = 0,
        batch = false,
        body = message,
    }
    local h, err = produce(self, msg)
    if not h then
        return nil, err
    end
    local localTransactionState = core.TRANSACTION_NOT_TYPE
    if h.code == RESPONSE_CODE.SUCCESS then
        msg.properties.__transationId__ = h.transationId
        msg.transationId = msg.properties.UNIQ_KEY
        localTransactionState = execute(msg, arg)
    else
        localTransactionState = core.TRANSACTION_ROLLBACK_TYPE
    end
    local _, commitLogOffset = utils.decodeMessageId(h.extFields.offsetMsgId or h.extFields.msgId)
    local brokerAddr = self.client:findBrokerAddressInPublish(h.sendResult.messageQueue.brokerName, topic)
    self.client:endTransactionOneway(brokerAddr, {
        producerGroup = self.groupName,
        transationId = h.transationId,
        commitLogOffset = commitLogOffset,
        commitOrRollback = localTransactionState,
        tranStateTableOffset = h.extFields.queueOffset,
        msgId = h.extFields.msgId,
    })
end

return _M
