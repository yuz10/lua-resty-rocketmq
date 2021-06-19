local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local split = utils.split
local decode = require("resty.rocketmq.json").decode
local bit = require("bit")
local RESPONSE_CODE = core.RESPONSE_CODE
local band = bit.band
local tohex = bit.tohex
local char = string.char
local byte = string.byte

local _M = {}
_M.__index = _M

function _M.new(nameservers, groupName)
    local cli = client.new(nameservers)
    return setmetatable({
        client = cli,
        groupName = groupName,
        topicPublishInfoTable = {},
        brokerAddrTable = {},
        RPCHook = {}
    }, _M)
end

function _M.addRPCHook(self, hook)
    self.client:addRPCHook(hook)
end

function _M.setUseTLS(self, useTLS)
    self.client:setUseTLS(useTLS)
end

local function topicRouteData2TopicPublishInfo(topic, route)
    local info = {
        topicRouteData = route,
        messageQueueList = {},
        sendWhichQueue = 0,
    }
    if route.orderTopicConf then
        local brokers = split(route.orderTopicConf, ";")
        for _, broker in ipairs(brokers) do
            local item = split(broker, ':')
            for i = 0, item[2] - 1 do
                table.insert(info.messageQueueList, {
                    topic = topic,
                    brokerName = item[1],
                    queueId = i,
                })
            end
        end
        info.orderTopic = true
    else
        local qds = route.queueDatas
        table.sort(qds, function(qd1, qd2)
            return qd1.brokerName > qd2.brokerName
        end)
        for _, qd in ipairs(qds) do
            if band(qd.perm, core.PERM_WRITE) == core.PERM_WRITE then
                local brokerData
                for _, bd in ipairs(route.brokerDatas) do
                    if bd.brokerName == qd.brokerName then
                        brokerData = bd
                        break
                    end
                end
                if brokerData ~= nil and brokerData.brokerAddrs[0] then
                    for i = 0, qd.writeQueueNums - 1 do
                        table.insert(info.messageQueueList, {
                            topic = topic,
                            brokerName = qd.brokerName,
                            queueId = i,
                        })
                    end
                end
            end
        end
        info.orderTopic = false
    end
    return info
end

local function updateTopicRouteInfoFromNameserver(self, topic)
    local h, b, err = self.client:getTopicRouteInfoFromNameserver(topic)
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('getTopicRouteInfoFromNameserver return %s, %s'):format(core.respCodeName(h.code), h.remark or '')
    end
    local topicRouteData, err = decode(b)
    if not topicRouteData then
        return nil, err
    end
    local publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData)
    self.topicPublishInfoTable[topic] = publishInfo
    for _, bd in ipairs(topicRouteData.brokerDatas) do
        self.brokerAddrTable[bd.brokerName] = bd.brokerAddrs
    end
    return publishInfo
end

local function tryToFindTopicPublishInfo(self, topic)
    local info = self.topicPublishInfoTable[topic]
    if info then
        return info
    end
    return updateTopicRouteInfoFromNameserver(self, topic)
end

local function findBrokerAddressInPublish(self, brokerName, topic)
    local map = self.brokerAddrTable[brokerName]
    if map then
        return map[0]
    end
    tryToFindTopicPublishInfo(self, topic)
    local map = self.brokerAddrTable[brokerName]
    return map and map[0]
end

local function selectOneMessageQueue(topicPublishInfo)
    local index = topicPublishInfo.sendWhichQueue
    topicPublishInfo.sendWhichQueue = index + 1
    local pos = math.abs(index) % #topicPublishInfo.messageQueueList
    return topicPublishInfo.messageQueueList[pos + 1]
end

local function updateAllTopicRouteInfoFromNameserver(self)
    for topic, _ in pairs(self.topicPublishInfoTable) do
        local _, err = updateTopicRouteInfoFromNameserver(self, topic)
        if err then
            ngx.log(ngx.WASRN, 'updateAllTopicRouteInfoFromNameserver fail, topic:', topic, 'err:', err)
        end
    end
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
    for brokerName, brokers in pairs(self.brokerAddrTable) do
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
        updateAllTopicRouteInfoFromNameserver(self)
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
    local topicPublishInfo, err = tryToFindTopicPublishInfo(self, msg.topic)
    if not topicPublishInfo then
        return nil, err
    end
    local mqSelected = selectOneMessageQueue(topicPublishInfo)
    local brokerAddr = findBrokerAddressInPublish(self, mqSelected.brokerName, msg.topic)
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
            queueId = mqSelected.queueId,
        },
        offsetMsgId = h.extFields.msgId,
        queueOffset = h.extFields.queueOffset,
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
    local brokerAddr = findBrokerAddressInPublish(self, h.sendResult.messageQueue.brokerName, topic)
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
