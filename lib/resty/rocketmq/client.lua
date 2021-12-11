local bit = require("bit")
local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local split = require("resty.rocketmq.utils").split
local decode = require("resty.rocketmq.json").decode

local unpack = unpack
local char = string.char
local concat = table.concat
local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE
local band = bit.band

local _M = {}
_M.__index = _M
function _M.new(nameservers)
    if #nameservers == 0 then
        return nil, 'no nameserver'
    end
    local nameservers_parsed = {}
    for _, v in ipairs(nameservers) do

        local ip, port = unpack(split(v, ':'))
        if not port then
            return nil, 'nameserver no port:' .. v
        end
        table.insert(nameservers_parsed, {
            ip = ip,
            port = port
        })
    end
    return setmetatable({
        nameservers = nameservers_parsed,
        current_nameserver = 1,
        RPCHook = {},
        useTLS = false,
        timeout = 3000,

        topicPublishInfoTable = {},
        brokerAddrTable = {},
    }, _M)
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

function _M:chooseNameserver()
    local nameserver = self.nameservers[self.current_nameserver]
    self.current_nameserver = self.current_nameserver + 1
    if self.current_nameserver > #self.nameservers then
        self.current_nameserver = 1
    end
    return nameserver
end

function _M:getTopicRouteInfoFromNameserver(topic)
    local nameserver = self:chooseNameserver()
    local addr = nameserver.ip .. ':' .. nameserver.port
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

function _M:sendHeartbeat(brokerAddr, heartbeatData)
    return self:request(REQUEST_CODE.HEART_BEAT, brokerAddr, {}, cjson_safe.encode(heartbeatData), false)
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

local function updateAllTopicRouteInfoFromNameserver(self)
    for topic, _ in pairs(self.topicPublishInfoTable) do
        local _, err = updateTopicRouteInfoFromNameserver(self, topic)
        if err then
            ngx.log(ngx.WARN, 'updateAllTopicRouteInfoFromNameserver fail, topic:', topic, 'err:', err)
        end
    end
end
_M.updateAllTopicRouteInfoFromNameserver = updateAllTopicRouteInfoFromNameserver

return _M
