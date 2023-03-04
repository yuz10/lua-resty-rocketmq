local bit = require("bit")
local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local decode = require("resty.rocketmq.json").decode
local utils = require("resty.rocketmq.utils")
local split = utils.split

local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE
local band = bit.band
local ngx = ngx
local log = ngx.log
local WARN = ngx.WARN
local random = math.random

---@class client
local _M = {}
_M.__index = _M

_M.INIT_MODE_MIN = 0
_M.INIT_MODE_MAX = 1

function _M.new(nameservers, processor)
    if #nameservers == 0 then
        return nil, 'no nameserver'
    end
    ---@type client
    local client = setmetatable({
        nameservers = nameservers,
        current_nameserver = 1,
        RPCHook = {},
        useTLS = false,
        timeout = 3000,
        
        topicPublishInfoTable = {},
        topicSubscribeInfoTable = {},
        topicRouteTable = {},
        brokerAddrTable = {},
        processor = processor,
    }, _M)
    return client
end

function _M.addRPCHook(self, hook)
    if type(hook) == 'table' and type(hook.doBeforeRequest) == 'function' and type(hook.doAfterResponse) == 'function' then
        table.insert(self.RPCHook, hook)
    else
        error('hook should be functions')
    end
end

function _M.setUseTLS(self, useTLS)
    self.useTLS = useTLS
end

function _M.setTimeout(self, timeout)
    self.timeout = timeout
end

function _M:request(code, addr, header, body, oneway, timeout)
    return core.request(code, addr, header, body, oneway, self.RPCHook, self.useTLS, timeout or self.timeout)
end

function _M:requestHeartbeat(code, addr, sock, header, body, processor)
    return core.requestHeartbeat(code, addr, sock, header, body, self.RPCHook, processor)
end

function _M:chooseNameserver()
    local nameserver = self.nameservers[self.current_nameserver]
    self.current_nameserver = self.current_nameserver + 1
    if self.current_nameserver > #self.nameservers then
        self.current_nameserver = 1
    end
    return nameserver
end

local function compatStandardJson(topicRouteData)
    local brokerDatas = topicRouteData.brokerDatas
    for _, bd in ipairs(brokerDatas) do
        local newBrokerAddrs = {}
        for k, v in pairs(bd.brokerAddrs) do
            if type(k) == 'number' then
                return
            end
            newBrokerAddrs[tonumber(k)] = v
        end
        bd.brokerAddrs = newBrokerAddrs
    end
end

function _M:getTopicRouteInfoFromNameserver(topic)
    local addr = self:chooseNameserver()
    local h, b, err = self:request(REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC, addr, { topic = topic }, nil, false)
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('getTopicRouteInfoFromNameserver return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    local topicRouteData, err = decode(b)
    if not topicRouteData then
        return nil, err
    end
    compatStandardJson(topicRouteData)
    return topicRouteData
end

function _M:sendMessage(brokerAddr, msg)
    return self:request(REQUEST_CODE.SEND_MESSAGE_V2, brokerAddr, {
        a = msg.producerGroup,
        b = msg.topic,
        c = msg.defaultTopic,
        d = msg.defaultTopicQueueNums,
        e = msg.queueId,
        f = msg.sysFlag,
        g = msg.bornTimeStamp,
        h = msg.flag,
        i = core.messageProperties2String(msg.properties),
        j = msg.reconsumeTimes,
        k = msg.unitMode,
        l = msg.maxReconsumeTimes,
        m = msg.batch,
        n = msg.bname,
    }, msg.body, false)
end

function _M:endTransactionOneway(brokerAddr, msg)
    return self:request(REQUEST_CODE.END_TRANSACTION, brokerAddr, msg, nil, true)
end

function _M:sendHeartbeat(addr, sock, heartbeatData, processor)
    return self:requestHeartbeat(REQUEST_CODE.HEART_BEAT, addr, sock, {}, cjson_safe.encode(heartbeatData), processor)
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

local function topicRouteData2TopicSubscribeInfo(topic, route)
    local mqList = {}
    local qds = route.queueDatas
    for _, qd in ipairs(qds) do
        if band(qd.perm, core.PERM_READ) == core.PERM_READ then
            for i = 0, qd.readQueueNums - 1 do
                table.insert(mqList, {
                    topic = topic,
                    brokerName = qd.brokerName,
                    queueId = i,
                })
            end
        end
    end
    return mqList;
end

local function updateTopicRouteInfoFromNameserver(self, topic)
    local topicRouteData, err = self:getTopicRouteInfoFromNameserver(topic)
    if not topicRouteData then
        return nil, err
    end
    local publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData)
    self.topicPublishInfoTable[topic] = publishInfo
    
    local subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData)
    self.topicSubscribeInfoTable[topic] = subscribeInfo
    
    self.topicRouteTable[topic] = topicRouteData
    
    for _, bd in ipairs(topicRouteData.brokerDatas) do
        self.brokerAddrTable[bd.brokerName] = bd.brokerAddrs
    end
    return publishInfo
end
_M.updateTopicRouteInfoFromNameserver = updateTopicRouteInfoFromNameserver

local function tryToFindTopicPublishInfo(self, topic)
    local info = self.topicPublishInfoTable[topic]
    if info then
        return info
    end
    return updateTopicRouteInfoFromNameserver(self, topic)
end
_M.tryToFindTopicPublishInfo = tryToFindTopicPublishInfo

local function findBrokerAddressInPublish(self, brokerName, topic)
    local map = self.brokerAddrTable[brokerName]
    if map then
        return map[0]
    end
    if topic then
        updateTopicRouteInfoFromNameserver(self, topic)
        local map = self.brokerAddrTable[brokerName]
        return map and map[0]
    end
end
_M.findBrokerAddressInPublish = findBrokerAddressInPublish

local function findBrokerAddressInSubscribe(self, brokerName)
    local map = self.brokerAddrTable[brokerName]
    if map then
        return map[0]
    end
end

local function updateAllTopicRouteInfoFromNameserver(self)
    for topic, _ in pairs(self.topicPublishInfoTable) do
        local _, err = updateTopicRouteInfoFromNameserver(self, topic)
        if err then
            log(WARN, 'updateAllTopicRouteInfoFromNameserver fail, topic:', topic, 'err:', err)
        end
    end
end
_M.updateAllTopicRouteInfoFromNameserver = updateAllTopicRouteInfoFromNameserver

function _M:findBrokerAddrByTopic(topic)
    local topicRouteData = self.topicRouteTable[topic]
    if topicRouteData == nil then
        return nil
    end
    local brokers = topicRouteData.brokerDatas
    if brokers == nil or #brokers == 0 then
        return nil
    end
    local bd = brokers[random(#brokers)]
    local addr = bd.brokerAddrs[0]
    if not addr then
        local _
        _, addr = next(bd.brokerAddrs)
    end
    return addr
end

function _M:findConsumerIdList(topic, consumerGroup)
    local brokerAddr = self:findBrokerAddrByTopic(topic)
    if brokerAddr == nil then
        updateTopicRouteInfoFromNameserver(self, topic)
        brokerAddr = self:findBrokerAddrByTopic(topic)
    end
    if brokerAddr == nil then
        return nil
    end
    local h, b, err = self:request(REQUEST_CODE.GET_CONSUMER_LIST_BY_GROUP, brokerAddr, {
        consumerGroup = consumerGroup
    })
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('findConsumerIdList return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    local body, err = cjson_safe.decode(b)
    if not body then
        return nil, err
    end
    return body.consumerIdList
end

function _M:updateConsumeOffsetToBroker(mq, offset)
    local brokerAddr = findBrokerAddressInSubscribe(self, mq.brokerName)
    
    return self:request(REQUEST_CODE.UPDATE_CONSUMER_OFFSET, brokerAddr, {
        topic = mq.topic,
        consumerGroup = mq.consumerGroup,
        queueId = mq.queueId,
        commitOffset = offset,
        bname = mq.brokerName,
    }, nil, true)
end

function _M:fetchConsumeOffsetFromBroker(consumerGroup, mq)
    local brokerAddr = findBrokerAddressInSubscribe(self, mq.brokerName)
    
    local h, b, err = self:request(REQUEST_CODE.QUERY_CONSUMER_OFFSET, brokerAddr, {
        topic = mq.topic,
        consumerGroup = consumerGroup,
        queueId = mq.queueId,
        bname = mq.brokerName,
    })
    if not h then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('fetchConsumeOffsetFromBroker return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    return tonumber(h.extFields.offset)
end

function _M:sendHeartbeatToAllBroker(sock_map, heartbeatData)
    for brokerName, brokers in pairs(self.brokerAddrTable) do
        local addr = brokers[0]
        if addr then
            local sock = sock_map[addr]
            if not sock then
                local err
                sock, err = core.newSocket(addr, self.useTLS, self.timeout, { pool_size = 1, backlog = 10, pool = 'heart' .. addr })
                if not sock then
                    log(WARN, 'fail to new socket when send heartbeat:', err)
                    return
                end
                sock_map[addr] = sock
            end
            local h, b, err = self:sendHeartbeat(addr, sock, heartbeatData, self.processor)
            if err then
                log(WARN, 'fail to send heartbeat:', err)
            elseif h.code ~= RESPONSE_CODE.SUCCESS then
                log(WARN, 'fail to send heartbeat, code:', core.RESPONSE_CODE_NAME[h.code] or h.code, ',remark:', h.remark)
            end
        end
    end
end

_M.FOUND = "FOUND"
_M.NO_NEW_MSG = "NO_NEW_MSG"
_M.NO_MATCHED_MSG = "NO_MATCHED_MSG"
_M.OFFSET_ILLEGAL = "OFFSET_ILLEGAL"

_M.POLLING_FULL = "POLLING_FULL"
_M.POLLING_NOT_FOUND = "POLLING_NOT_FOUND"

function _M:pullKernelImpl(brokerName, header, timeout)
    local brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    if not brokerAddr then
        updateTopicRouteInfoFromNameserver(self, header.topic)
        brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    end
    if not brokerAddr then
        return nil, ('broker name %s not found'):format(brokerName)
    end
    header.bname = brokerName
    local h, b, err = self:request(REQUEST_CODE.PULL_MESSAGE, brokerAddr, header, nil, false, timeout)
    if not h then
        return nil, err
    end
    local status
    if h.code == RESPONSE_CODE.SUCCESS then
        status = _M.FOUND
    elseif h.code == RESPONSE_CODE.PULL_NOT_FOUND then
        status = _M.NO_NEW_MSG
    elseif h.code == RESPONSE_CODE.PULL_RETRY_IMMEDIATELY then
        status = _M.NO_MATCHED_MSG
    elseif h.code == RESPONSE_CODE.PULL_OFFSET_MOVED then
        status = _M.OFFSET_ILLEGAL
    else
        return nil, ('pullKernelImpl return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    local messages = {}
    core.decodeMsgs(messages, b, true, false)
    return {
        suggestWhichBrokerId = tonumber(h.extFields.suggestWhichBrokerId),
        nextBeginOffset = tonumber(h.extFields.nextBeginOffset),
        minOffset = tonumber(h.extFields.minOffset),
        maxOffset = tonumber(h.extFields.maxOffset),
        pullStatus = status,
        msgFoundList = messages,
    }
end

function _M:sendMessageBack(brokerName, msg, consumerGroup, delayLevel, maxReconsumeTimes)
    local brokerAddr = findBrokerAddressInPublish(self, brokerName, msg.topic)
    local h, b, err = self:request(REQUEST_CODE.CONSUMER_SEND_MSG_BACK, brokerAddr, {
        group = consumerGroup,
        originTopic = msg.topic,
        offset = msg.commitLogOffset,
        delayLevel = delayLevel,
        originMsgId = msg.msgId,
        maxReconsumeTimes = maxReconsumeTimes,
    })
    if not h then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('sendMessageBack return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    return true
end

function _M:queryAssignment(topic, consumerGroup, strategyName, messageModel, clientId)
    local brokerAddr = self:findBrokerAddrByTopic(topic)
    if brokerAddr == nil then
        updateTopicRouteInfoFromNameserver(self, topic)
        brokerAddr = self:findBrokerAddrByTopic(topic)
    end
    if brokerAddr == nil then
        return nil
    end
    local h, b, err = self:request(REQUEST_CODE.QUERY_ASSIGNMENT, brokerAddr, {}, cjson_safe.encode {
        topic = topic,
        consumerGroup = consumerGroup,
        clientId = clientId,
        strategyName = strategyName,
        messageModel = messageModel,
    })
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('queryAssignment return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    local body, err = cjson_safe.decode(b)
    if not body then
        return nil, err
    end
    return body.messageQueueAssignments
end

local function parseStartOffsetInfo(startOffsetInfo)
    if not startOffsetInfo or startOffsetInfo == '' then
        return nil
    end
    local msgOffsetMap = {}
    for _, one in ipairs(split(startOffsetInfo, ';')) do
        local spl = split(one, ' ')
        if #spl ~= 3 then
            return nil, 'parse startOffsetInfo error'
        end
        local key = spl[1] .. '@' .. spl[2]
        if msgOffsetMap[key] then
            return nil, 'parse startOffsetInfo error, duplicate'
        end
        msgOffsetMap[key] = tonumber(spl[3])
    end
    return msgOffsetMap
end

local function parseMsgOffsetInfo(str)
    if not str or str == '' then
        return nil
    end
    local msgOffsetMap = {}
    for _, one in ipairs(split(str, ';')) do
        local spl = split(one, ' ')
        if #spl ~= 3 then
            return nil, 'parseMsgOffsetInfo error'
        end
        local key = spl[1] .. '@' .. spl[2]
        if msgOffsetMap[key] then
            return nil, 'parseMsgOffsetInfo error, duplicate'
        end
        local offsets = {}
        for _, offset in ipairs(split(spl[3], ',')) do
            table.insert(offsets, offset)
        end
        msgOffsetMap[key] = offsets
    end
    return msgOffsetMap
end

local function buildExtraInfo(ckQueueOffset, popTime, invisibleTime, reviveQid, topic, brokerName, queueId)
    return ("%d %d %d %d %s %s %d"):format(ckQueueOffset, popTime, invisibleTime, reviveQid, utils.startsWith(topic, core.RETRY_GROUP_TOPIC_PREFIX) and '1' or '0', brokerName, queueId)
end

local function getStartOffsetInfoMapKey(topic, key)
    return (utils.startsWith(topic, core.RETRY_GROUP_TOPIC_PREFIX) and '1' or '0') .. "@" .. key;
end

local function getQueueOffsetKeyValueKey(queueId, queueOffset)
    return "qo" .. queueId .. "%" .. queueOffset
end

local function getQueueOffsetMapKey(topic, queueId, queueOffset)
    return (utils.startsWith(topic, core.RETRY_GROUP_TOPIC_PREFIX) and '1' or '0') .. "@" .. getQueueOffsetKeyValueKey(queueId, queueOffset)
end

local function processPopResponse(mq, status, extFields, messages)
    local startOffsetInfo = parseStartOffsetInfo(extFields.startOffsetInfo)
    local msgOffsetInfo = parseMsgOffsetInfo(extFields.msgOffsetInfo)
    local sortMap = {}
    for _, msg in ipairs(messages) do
        local key = getStartOffsetInfoMapKey(msg.topic, msg.queueId)
        sortMap[key] = sortMap[key] or {}
        table.insert(sortMap[key], msg.queueOffset)
    end
    local map = {}
    for _, msg in ipairs(messages) do
        if startOffsetInfo == nil then
            local key = msg.topic .. msg.queueId
            if not map[key] then
                map[key] = buildExtraInfo(msg.queueOffset, extFields.popTime, extFields.invisibleTime, extFields.reviveQid, msg.topic, mq.brokerName, msg.queueId)
            end
            msg.properties["POP_CK"] = map[key] .. ' ' .. msg.queueOffset
        else
            if msg.properties["POP_CK"] == nil then
                local queueIdKey = getStartOffsetInfoMapKey(msg.topic, msg.queueId)
                local index = utils.indexOf(sortMap[queueIdKey], msg.queueOffset)
                local msgQueueOffset = msgOffsetInfo[queueIdKey][index]
                if msgQueueOffset ~= msg.queueOffset then
                    log(WARN, ('Queue offset[%s] of msg is strange, not equal to the stored in msg, %s'):format(msgQueueOffset, msg.queueOffset))
                end
                local extraInfo = buildExtraInfo(startOffsetInfo[queueIdKey], extFields.popTime, extFields.invisibleTime, extFields.reviveQid, msg.topic, mq.brokerName, msg.queueId)
                msg.properties["POP_CK"] = extraInfo .. ' ' .. msg.queueOffset
            end
        end
        msg.properties["1ST_POP_TIME"] = msg.properties["1ST_POP_TIME"] or tostring(extFields.popTime)
        msg.brokerName = mq.brokerName
    end
    
    return {
        restNum = tonumber(extFields.restNum),
        invisibleTime = tonumber(extFields.invisibleTime),
        popTime = tonumber(extFields.popTime),
        popStatus = status,
        msgFoundList = messages,
    }
end

function _M:pop(mq, invisibleTime, maxNums, consumerGroup, timeout, poll, initMode, expressionType, expression)
    local brokerAddr = findBrokerAddressInSubscribe(self, mq.brokerName)
    if not brokerAddr then
        local res, err = updateTopicRouteInfoFromNameserver(self, mq.topic)
        if not res then
            return nil, err
        end
        brokerAddr = findBrokerAddressInSubscribe(self, mq.brokerName)
    end
    if brokerAddr == nil then
        return nil, 'broker address not found for broker ' .. mq.brokerName
    end
    local header = {
        consumerGroup = consumerGroup,
        topic = mq.topic,
        queueId = mq.queueId,
        maxMsgNums = maxNums,
        invisibleTime = invisibleTime,
        initMode = initMode,
        expType = expressionType,
        exp = expression,
        order = false,
        bname = mq.brokerName,
    }
    if poll then
        header.pollTime = timeout
        timeout = timeout + 10000
    end
    local h, b, err = self:request(REQUEST_CODE.POP_MESSAGE, brokerAddr, header, nil, false, timeout)
    if not h then
        return nil, err
    end
    local status
    if h.code == RESPONSE_CODE.SUCCESS then
        status = _M.FOUND
    elseif h.code == RESPONSE_CODE.NO_NEW_MSG then
        status = _M.NO_NEW_MSG
    elseif h.code == RESPONSE_CODE.POLLING_FULL then
        status = _M.POLLING_FULL
    elseif h.code == RESPONSE_CODE.POLLING_TIMEOUT then
        status = _M.POLLING_NOT_FOUND
    elseif h.code == RESPONSE_CODE.PULL_NOT_FOUND then
        status = _M.POLLING_NOT_FOUND
    else
        log(WARN, ('pop return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or ''))
        return nil, ('pop return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    if status ~= _M.FOUND then
        return {
            status = status,
            restNum = tonumber(h.extFields.restNum),
        }
    end
    local messages = {}
    core.decodeMsgs(messages, b, true, false)
    return processPopResponse(mq, status, h.extFields, messages)
end

function _M:changePopInvisibleTime(topic, consumerGroup, extraInfo, invisibleTime)
    local extraInfoStrs = split(extraInfo, ' ')
    local brokerName = extraInfoStrs[6]
    local queueId = tonumber(extraInfoStrs[7])
    local offset = extraInfoStrs[8]
    local brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    if brokerAddr == nil then
        updateTopicRouteInfoFromNameserver(self, topic)
        brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    end
    if brokerAddr == nil then
        return nil
    end
    local h, b, err = self:request(REQUEST_CODE.CHANGE_MESSAGE_INVISIBLETIME, brokerAddr, {
        topic = topic,
        queueId = queueId,
        offset = offset,
        consumerGroup = consumerGroup,
        extraInfo = extraInfo,
        invisibleTime = invisibleTime,
        bname = brokerName,
    })
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('changePopInvisibleTime return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    return h.extFields
end

function _M:ack(message, consumerGroup)
    return self:doAck(message.topic, consumerGroup, message.properties['POP_CK'])
end

function _M:doAck(topic, consumerGroup, extraInfo)
    local extraInfoStrs = split(extraInfo, ' ')
    local brokerName = extraInfoStrs[6]
    local queueId = tonumber(extraInfoStrs[7])
    local offset = extraInfoStrs[8]
    local brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    if brokerAddr == nil then
        updateTopicRouteInfoFromNameserver(self, topic)
        brokerAddr = findBrokerAddressInSubscribe(self, brokerName)
    end
    if brokerAddr == nil then
        return nil
    end
    local h, b, err = self:request(REQUEST_CODE.ACK_MESSAGE, brokerAddr, {
        topic = topic,
        queueId = queueId,
        offset = offset,
        consumerGroup = consumerGroup,
        extraInfo = extraInfo,
        bname = brokerName,
    })
    if err then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('ack return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    return h.extFields
end

return _M
