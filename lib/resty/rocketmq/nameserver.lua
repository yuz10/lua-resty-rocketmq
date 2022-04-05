local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local remoting = require("resty.rocketmq.remoting")
local utils = require("resty.rocketmq.utils")
local bit = require("bit")
local ngx = ngx

local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE
local band = bit.band
local bor = bit.bor
local bnot = bit.bnot

local _M = {}
_M.__index = _M

function _M.new()
    local nameserver = setmetatable({
        configTable = {},
        brokerLiveTable = {},
        clusterAddrTable = {},
        brokerAddrTable = {},
        topicQueueTable = {},
        filterServerTable = {},
    }, _M)
    return remoting.new(nameserver)
end

local function res(code, remark, body)
    return {
        code = code,
        header = {
            remark = remark
        },
        body = body and cjson_safe.encode(body) or '',
    }
end

local function queryBrokerTopicConfig(self, brokerAddr)
    local prev = self.brokerLiveTable[brokerAddr]
    return prev and prev.dataVersion
end

local function isBrokerTopicConfigChanged(self, brokerAddr, dataVersion)
    local prev = queryBrokerTopicConfig(self, brokerAddr)
    return not prev or prev.timestamp ~= dataVersion.timestamp or prev.counter ~= dataVersion.counter
end

local function updateBrokerInfoUpdateTimestamp(self, brokerAddr)
    local prev = self.brokerLiveTable[brokerAddr]
    if prev then
        prev.lastUpdateTimestamp = ngx.now() * 1000
    end
end

local function createAndUpdateQueueData(self, brokerName, config)
    local queueData = {
        brokerName = brokerName,
        writeQueueNums = config.writeQueueNums,
        readQueueNums = config.readQueueNums,
        perm = config.perm,
        topicSysFlag = config.topicSysFlag,
    }
    local queueDataMap = self.topicQueueTable[config.topicName]
    if not queueDataMap then
        queueDataMap = { [brokerName] = queueData }
        self.topicQueueTable[config.topicName] = queueDataMap
    else
        queueDataMap[brokerName] = queueData
    end

end

local processors = {}
processors[REQUEST_CODE.PUT_KV_CONFIG] = function(self, addr, h, body)
    local header = h.extFields
    if not header.namespace or not header.key then
        return res(RESPONSE_CODE.SYSTEM_ERROR, 'namespace or key is null')
    end
    self.configTable[header.namespace] = self.configTable[header.namespace] or {}
    self.configTable[header.namespace][header.key] = header.value
    return res(RESPONSE_CODE.SUCCESS)
end

processors[REQUEST_CODE.GET_KV_CONFIG] = function(self, addr, h, body)
    local header = h.extFields
    local value = self.configTable[header.namespace] and self.configTable[header.namespace][header.key]
    if value then
        local r = res(RESPONSE_CODE.SUCCESS)
        r.header.value = value
        return r
    else
        return res(RESPONSE_CODE.QUERY_NOT_FOUND, ('No config item, Namespace: %s Key: %s'):format(header.namespace, header.key))
    end
end

processors[REQUEST_CODE.DELETE_KV_CONFIG] = function(self, addr, h, body)
    local header = h.extFields
    if self.configTable[header.namespace] then
        self.configTable[header.namespace][header.key] = nil
    end
    return res(RESPONSE_CODE.SUCCESS)
end

processors[REQUEST_CODE.QUERY_DATA_VERSION] = function(self, addr, h, body)
    local header = h.extFields
    local brokerAddr = header.brokerAddr
    local dataVersion = cjson_safe.decode(body)
    local changed = isBrokerTopicConfigChanged(self, brokerAddr, dataVersion)
    if not changed then
        updateBrokerInfoUpdateTimestamp(self, brokerAddr)
    end
    local nameSeverDataVersion = queryBrokerTopicConfig(self, brokerAddr)
    return res(RESPONSE_CODE.SUCCESS, nil, nameSeverDataVersion)
end

processors[REQUEST_CODE.REGISTER_BROKER] = function(self, addr, h, body)
    local header = h.extFields
    local result = {}
    local registerBrokerBody
    if body then
        registerBrokerBody = cjson_safe.decode(body)
    else
        registerBrokerBody = {
            topicConfigSerializeWrapper = {
                dataVersion = {
                    counter = 0,
                    timestamp = 0,
                }
            }
        }
    end
    local clusterName, brokerAddr, brokerName, brokerId, haServerAddr = header.clusterName, header.brokerAddr, header.brokerName, tonumber(header.brokerId), header.haServerAddr
    local topicConfigWrapper, filterServerList = registerBrokerBody.topicConfigSerializeWrapper, registerBrokerBody.filterServerList
    local brokerNames = self.clusterAddrTable[clusterName]
    if not brokerNames then
        brokerNames = {}
        self.clusterAddrTable[clusterName] = brokerNames
    end
    brokerNames[brokerName] = true
    local registerFirst = false
    local brokerData = self.brokerAddrTable[brokerName]
    if not brokerData then
        registerFirst = true
        brokerData = {
            cluster = clusterName,
            brokerName = brokerName,
            brokerAddrs = {},
        }
        self.brokerAddrTable[brokerName] = brokerData
    end
    local brokerAddrsMap = brokerData.brokerAddrs
    for k, v in pairs(brokerAddrsMap) do
        if brokerAddr == v and brokerId ~= k then
            brokerAddrsMap[k] = nil
        end
    end
    if not brokerAddrsMap[brokerId] then
        registerFirst = true
    end
    brokerAddrsMap[brokerId] = brokerAddr
    if topicConfigWrapper and brokerId == 0 then
        if isBrokerTopicConfigChanged(self, brokerAddr, topicConfigWrapper.dataVersion) or registerFirst then
            for topic, config in pairs(topicConfigWrapper.topicConfigTable or {}) do
                createAndUpdateQueueData(self, brokerName, config)
            end
        end
    end
    self.brokerLiveTable[brokerAddr] = {
        lastUpdateTimestamp = ngx.now() * 1000,
        dataVersion = topicConfigWrapper.dataVersion,
        haServerAddr = haServerAddr,
    }
    if filterServerList then
        if #filterServerList == 0 then
            self.filterServerTable[brokerAddr] = nil
        else
            self.filterServerTable[brokerAddr] = filterServerList
        end
    end
    if brokerId ~= 0 then
        local masterAddr = brokerAddrsMap[0]
        if masterAddr then
            local brokerLiveInfo = self.brokerLiveTable[masterAddr]
            if brokerLiveInfo then
                result.haServerAddr = brokerLiveInfo.haServerAddr
                result.masterAddr = masterAddr
            end
        end
    end
    local order = self.configTable[core.ORDER_TOPIC_CONFIG]
    local r = res(RESPONSE_CODE.SUCCESS, nil, { table = order })
    r.header = result
    return r
end

processors[REQUEST_CODE.UNREGISTER_BROKER] = function(self, addr, h, body)
    local header = h.extFields
    local clusterName, brokerAddr, brokerName, brokerId = header.clusterName, header.brokerAddr, header.brokerName, tonumber(header.brokerId)
    self.brokerLiveTable[brokerAddr] = nil
    self.filterServerTable[brokerAddr] = nil
    local brokerData = self.brokerAddrTable[brokerName]
    local removeBrokerName = false
    if brokerData then
        brokerData.brokerAddrs[brokerId] = nil
        if not next(brokerData.brokerAddrs) then
            self.brokerAddrTable[brokerName] = nil
            removeBrokerName = true;
        end
    end
    if removeBrokerName then
        local nameSet = self.clusterAddrTable[clusterName]
        nameSet[brokerName] = nil
        if not next(nameSet) then
            self.clusterAddrTable[clusterName] = nil
        end
        for topic, queueDataMap in pairs(self.topicQueueTable) do
            queueDataMap[brokerName] = nil
            if not next(queueDataMap) then
                self.topicQueueTable[topic] = nil
            end
        end
    end
    return res(RESPONSE_CODE.SUCCESS)
end

local function pickupTopicRouteData(self, topic)
    local topicRouteData = {}
    local foundQueueData = false
    local foundBrokerData = false
    local brokerNameSet = {}
    local brokerDataList = {}
    topicRouteData.brokerDatas = brokerDataList
    local filterServerMap = {}
    topicRouteData.filterServerTable = filterServerMap
    local queueDataMap = self.topicQueueTable[topic]
    if queueDataMap then
        topicRouteData.queueDatas = utils.values(queueDataMap)
        foundQueueData = true
        for brokerName, qd in pairs(queueDataMap) do
            table.insert(brokerNameSet, brokerName)
            local brokerData = self.brokerAddrTable[brokerName];
            if brokerData then
                foundBrokerData = true
                table.insert(brokerDataList, brokerData)
                for _, brokerAddr in pairs(brokerData.brokerAddrs) do
                    local filterServerList = self.filterServerTable[brokerAddr]
                    filterServerMap[brokerAddr] = filterServerList
                end
            end
        end

    end
    if foundBrokerData and foundQueueData then
        return topicRouteData
    end
    return nil
end

processors[REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC] = function(self, addr, h, body)
    local topic = h.extFields.topic
    local topicRouteData = pickupTopicRouteData(self, topic)
    if topicRouteData then
        local orderTopicConf = self.configTable[core.ORDER_TOPIC_CONFIG]
        topicRouteData.orderTopicConf = orderTopicConf
        return res(RESPONSE_CODE.SUCCESS, nil, topicRouteData)
    end
    return res(RESPONSE_CODE.TOPIC_NOT_EXIST, 'No topic route info in name server for the topic:' .. topic)
end

processors[REQUEST_CODE.GET_BROKER_CLUSTER_INFO] = function(self, addr, h, body)
    local clusterAddrTableTrans = {}
    for k, v in pairs(self.clusterAddrTable) do
        clusterAddrTableTrans[k] = utils.keys(v)
    end
    return res(RESPONSE_CODE.SUCCESS, nil, {
        brokerAddrTable = self.brokerAddrTable,
        clusterAddrTable = clusterAddrTableTrans,
    })
end

local function operateWritePermOfBroker(self, code, brokerName)
    local topicCnt = 0
    for _, qdMap in pairs(self.topicQueueTable) do
        local qd = qdMap[brokerName]
        if qd then
            local perm = qd.perm
            if code == REQUEST_CODE.WIPE_WRITE_PERM_OF_BROKER then
                perm = band(perm, bnot(core.PERM_WRITE))
            else
                perm = bor(core.PERM_READ, core.PERM_WRITE)
            end
            qd.perm = perm
            topicCnt = topicCnt + 1
        end
    end
    return topicCnt
end

processors[REQUEST_CODE.WIPE_WRITE_PERM_OF_BROKER] = function(self, addr, h, body)
    local topicCnt = operateWritePermOfBroker(self, REQUEST_CODE.WIPE_WRITE_PERM_OF_BROKER, h.extFields.brokerName)
    local r = res(RESPONSE_CODE.SUCCESS)
    r.header.wipeTopicCount = topicCnt
    return r
end

processors[REQUEST_CODE.ADD_WRITE_PERM_OF_BROKER] = function(self, addr, h, body)
    local topicCnt = operateWritePermOfBroker(self, REQUEST_CODE.ADD_WRITE_PERM_OF_BROKER, h.extFields.brokerName)
    local r = res(RESPONSE_CODE.SUCCESS)
    r.header.addTopicCount = topicCnt
    return r
end

processors[REQUEST_CODE.GET_ALL_TOPIC_LIST_FROM_NAMESERVER] = function(self, addr, h, body)
    return res(RESPONSE_CODE.SUCCESS, nil, {
        topicList = utils.keys(self.topicQueueTable),
    })
end

processors[REQUEST_CODE.DELETE_TOPIC_IN_NAMESRV] = function(self, addr, h, body)
    local topic = h.extFields.topic
    self.topicQueueTable[topic] = nil
    return res(RESPONSE_CODE.SUCCESS)
end

processors[REQUEST_CODE.GET_KVLIST_BY_NAMESPACE] = function(self, addr, h, body)
    local namespace = h.extFields.namespace
    local t = self.configTable[namespace]
    if t then
        return res(RESPONSE_CODE.SUCCESS, nil, { table = t })
    else
        return res(RESPONSE_CODE.QUERY_NOT_FOUND, 'No config item, Namespace: ' .. namespace)
    end
end

processors[REQUEST_CODE.GET_TOPICS_BY_CLUSTER] = function(self, addr, h, body)
    local cluster = h.extFields.cluster
    local brokerNameSet = self.clusterAddrTable[cluster]
    local topicList = {}
    for brokerName, _ in pairs(brokerNameSet or {}) do
        for topic, queueDatas in pairs(self.topicQueueTable) do
            if queueDatas[brokerName] then
                topicList[topic] = true
            end
        end
    end
    return res(RESPONSE_CODE.SUCCESS, nil, { topicList = utils.keys(topicList) })
end

processors[REQUEST_CODE.GET_SYSTEM_TOPIC_LIST_FROM_NS] = function(self, addr, h, body)
    local topicList = {}
    for cluster, brokerNameSet in pairs(self.clusterAddrTable) do
        topicList[cluster] = true
        for brokerName, _ in pairs(brokerNameSet) do
            topicList[brokerName] = true
        end
    end
    return res(RESPONSE_CODE.SUCCESS, nil, { topicList = utils.keys(topicList) })
end

function _M:processRequest(sock, addr, h, body)
    local processor = processors[h.code]
    local resp
    if processor then
        resp = processor(self, addr, h, body)
    else
        resp = res(RESPONSE_CODE.REQUEST_CODE_NOT_SUPPORTED, 'request code not supported')
    end
    local send = core.encode(resp.code, resp.header, resp.body, false, h.opaque)
    sock:send(send)
end

return _M
