local bit = require("bit")
local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local split = require("resty.rocketmq.utils").split
local decode = require("resty.rocketmq.json").decode

local unpack = unpack
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
function _M.new(nameservers)
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

function _M:request(code, addr, header, body, oneway)
    return core.request(code, addr, header, body, oneway, self.RPCHook, self.useTLS, self.timeout)
end

function _M:request1(code, sock, header, body, oneway)
    return core.request1(code, sock, header, body, oneway, self.RPCHook)
end

function _M:chooseNameserver()
    local nameserver = self.nameservers[self.current_nameserver]
    self.current_nameserver = self.current_nameserver + 1
    if self.current_nameserver > #self.nameservers then
        self.current_nameserver = 1
    end
    return nameserver
end

function _M:getTopicRouteInfoFromNameserver(topic)
    local addr = self:chooseNameserver()
    return self:request(REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC, addr, { topic = topic }, nil, false)
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
    }, msg.body, false)
end

function _M:endTransactionOneway(brokerAddr, msg)
    return self:request(REQUEST_CODE.END_TRANSACTION, brokerAddr, msg, nil, true)
end

function _M:sendHeartbeat(sock, heartbeatData)
    return self:request1(REQUEST_CODE.HEART_BEAT, sock, {}, cjson_safe.encode(heartbeatData), false)
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
    local h, b, err = self:getTopicRouteInfoFromNameserver(topic)
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
    tryToFindTopicPublishInfo(self, topic)
    local map = self.brokerAddrTable[brokerName]
    return map and map[0]
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
        topic = mq.offset,
        consumerGroup = mq.consumerGroup,
        queueId = mq.queueId,
        commitOffset = offset,
    }, nil, true)
end

function _M:fetchConsumeOffsetFromBroker(mq)
    local brokerAddr = findBrokerAddressInSubscribe(self, mq.brokerName)

    local h, b, err =  self:request(REQUEST_CODE.QUERY_CONSUMER_OFFSET, brokerAddr, {
        topic = mq.offset,
        consumerGroup = mq.consumerGroup,
        queueId = mq.queueId,
    })
    if not h then
        return nil, err
    end
    if h.code ~= RESPONSE_CODE.SUCCESS then
        return nil, ('fetchConsumeOffsetFromBroker return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    return h.extFields.offset
end

function _M:sendHeartbeatToAllBroker(sock_map, heartbeatData)
    for brokerName, brokers in pairs(self.brokerAddrTable) do
        local addr = brokers[0]
        if addr then
            local sock = sock_map[addr]
            if not sock then
                sock = core.newSocket(addr, self.useTLS, self.timeout, { pool_size = 1, backlog = 10, pool = 'heart' .. addr })
                sock_map[addr] = sock
            end
            local h, b, err = self:sendHeartbeat(sock, heartbeatData)
            if err then
                log(WARN, 'fail to send heartbeat:', err)
            elseif h.code ~= RESPONSE_CODE.SUCCESS then
                log(WARN, 'fail to send heartbeat, code:', core.RESPONSE_CODE_NAME[h.code] or h.code, ',remark:', h.remark)
            end
        end
    end
end

return _M
