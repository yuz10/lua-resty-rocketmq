local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local remoting = require("resty.rocketmq.remoting")
local client = require("resty.rocketmq.client")
local acl_rpchook = require("resty.rocketmq.acl_rpchook")
local decode = require("resty.rocketmq.json").decode
local ngx = ngx

local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE
local ngx_timer_every = ngx.timer.every

local _M = {}
_M.__index = _M

local function updateClusterInfo(self)
    local respHeader, respBody, err = self.client:request(REQUEST_CODE.GET_BROKER_CLUSTER_INFO, self.client:chooseNameserver(), {}, nil, false, self.timeout)
    if not respHeader then
        ngx.log(ngx.ERR, 'fail to update cluster info', err)
        return
    end
    local dec, err = decode(respBody)
    if dec then
        self.clusterAddrTable = dec.clusterAddrTable
        self.brokerAddrTable = dec.brokerAddrTable
    end
end

function _M.new(config)
    local self = setmetatable({
        advertisedAddr = config.advertised_addr,
        timeout = config.timeout or 5000,
    }, _M)
    local cli = client.new(config.nameservers, self)
    cli:setUseTLS(config.use_tls)
    if config.access_key and config.secret_key then
        cli:addRPCHook(acl_rpchook.new(config.access_key, config.secret_key))
    end
    self.client = cli
    ngx_timer_every(30, function()
        updateClusterInfo(self)
    end)
    return remoting.new(self)
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

local function getBrokerAddr(self, bname)
    if self.brokerAddrTable == nil then
        updateClusterInfo(self)
    end
    return self.brokerAddrTable[bname].brokerAddrs[0]
end

local processors = {}

local brokerProcessor = function(self, addr, h, body)
    local header = h.extFields or {}
    if not header.bname and not header.n then
        ngx.log(ngx.ERR, 'bname not found, code ', core.REQUEST_CODE_NAME[h.code])
        return res(RESPONSE_CODE.SYSTEM_ERROR, "bname not fount, 1.make sure client version >= 5.0.0; 2.some commands not supported")
    end
    local brokerAddr = getBrokerAddr(self, header.bname or header.n)
    local respHeader, respBody, err = self.client:request(h.code, brokerAddr, header, body, false, self.timeout)
    if not respHeader then
        return res(RESPONSE_CODE.SYSTEM_ERROR, ("proxy to broker %s error: %s"):format(brokerAddr, err))
    end
    respHeader.extFields = respHeader.extFields or {}
    respHeader.extFields.remark = respHeader.remark
    return {
        code = respHeader.code,
        header = respHeader.extFields,
        body = respBody or '',
    }
end

local brokerBroadcastProcessor = function(self, addr, h, body)
    local header = h.extFields or {}
    if header.bname or header.n then
        return brokerProcessor(self, addr, h, body)
    end
    if not self.brokerAddrTable then
        updateClusterInfo(self)
    end
    local finalRespHeader, finalRespBody, finalErr, brokerAddr
    local count = 0
    for bname, bd in pairs(self.brokerAddrTable) do
        brokerAddr = bd.brokerAddrs[0]
        count = count + 1
        local respHeader, respBody, err = self.client:request(h.code, brokerAddr, header, body, false, self.timeout)
        if respHeader then
            finalRespHeader = respHeader
            finalRespBody = respBody
        else
            finalErr = err
        end
    end
    if count == 0 then
        return res(RESPONSE_CODE.SYSTEM_ERROR, "no broker available")
    end
    if not finalRespHeader then
        return res(RESPONSE_CODE.SYSTEM_ERROR, ("proxy to broker %s error: %s"):format(brokerAddr, finalErr))
    end
    finalRespHeader.extFields = finalRespHeader.extFields or {}
    finalRespHeader.extFields.remark = finalRespHeader.remark
    return {
        code = finalRespHeader.code,
        header = finalRespHeader.extFields,
        body = finalRespBody or '',
    }
end

local brokerSingleProcessor = function(self, addr, h, body)
    local header = h.extFields or {}
    if header.bname or header.n then
        return brokerProcessor(self, addr, h, body)
    end
    if not self.brokerAddrTable then
        updateClusterInfo(self)
    end
    for bname, bd in pairs(self.brokerAddrTable) do
        local brokerAddr = bd.brokerAddrs[0]
        local respHeader, respBody, err = self.client:request(h.code, brokerAddr, header, body, false, self.timeout)
        if not respHeader then
            return res(RESPONSE_CODE.SYSTEM_ERROR, ("proxy to broker %s error: %s"):format(brokerAddr, err))
        end
        respHeader.extFields = respHeader.extFields or {}
        respHeader.extFields.remark = respHeader.remark
        return {
            code = respHeader.code,
            header = respHeader.extFields,
            body = respBody or '',
        }
    end
end

local namesrvProcessor = function(self, addr, h, body)
    local header = h.extFields
    local namesrvAddr = self.client:chooseNameserver()
    local respHeader, respBody, err = self.client:request(h.code, namesrvAddr, header, body, false, self.timeout)
    if not respHeader then
        return res(RESPONSE_CODE.SYSTEM_ERROR, ("proxy to nameserver %s error: %s"):format(namesrvAddr, err))
    end
    respHeader.extFields = respHeader.extFields or {}
    respHeader.extFields.remark = respHeader.remark
    return {
        code = respHeader.code,
        header = h,
        body = respBody or '',
    }
end

local unsupportedProcessor = function(self, addr, h, body)
    return res(RESPONSE_CODE.SYSTEM_ERROR, "unsupported request code")
end

processors[REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC] = function(self, addr, h, body)
    local namesrvAddr = self.client:chooseNameserver()
    local respHeader, respBody, err = self.client:request(h.code, namesrvAddr, h.extFields, body, false, self.timeout)
    if not respHeader then
        return res(RESPONSE_CODE.SYSTEM_ERROR, ("proxy to nameserver %s error: %s"):format(namesrvAddr, err))
    end
    if respHeader.code ~= core.RESPONSE_CODE.SUCCESS then
        return res(respHeader.code, respHeader.remark, respBody)
    end
    local dec = decode(respBody)
    local brokerDatasTrans = {}
    for _, v in ipairs(dec.brokerDatas) do
        table.insert(brokerDatasTrans, {
            cluster = v.cluster,
            brokerName = v.brokerName,
            zoneName = v.zoneName,
            enableActingMaster = v.enableActingMaster,
            brokerAddrs = { ["0"] = self.advertisedAddr },
        })
    end
    return res(RESPONSE_CODE.SUCCESS, nil, {
        orderTopicConf = dec.orderTopicConf,
        queueDatas = dec.queueDatas,
        brokerDatas = brokerDatasTrans,
        filterServerTable = dec.filterServerTable,
        topicQueueMappingByBroker = dec.topicQueueMappingByBroker,
    })
end

processors[REQUEST_CODE.GET_BROKER_CLUSTER_INFO] = function(self, addr, h, body)
    local brokerAddrTableTrans = {}
    for k, v in pairs(self.brokerAddrTable) do
        brokerAddrTableTrans[k] = {
            cluster = v.cluster,
            brokerName = v.brokerName,
            zoneName = v.zoneName,
            enableActingMaster = v.enableActingMaster,
            brokerAddrs = { ["0"] = self.advertisedAddr },
        }
    end
    return res(RESPONSE_CODE.SUCCESS, nil, {
        brokerAddrTable = brokerAddrTableTrans,
        clusterAddrTable = self.clusterAddrTable,
    })
end

processors[REQUEST_CODE.PUT_KV_CONFIG] = unsupportedProcessor
processors[REQUEST_CODE.GET_KV_CONFIG] = namesrvProcessor
processors[REQUEST_CODE.DELETE_KV_CONFIG] = unsupportedProcessor
processors[REQUEST_CODE.QUERY_DATA_VERSION] = namesrvProcessor
processors[REQUEST_CODE.REGISTER_BROKER] = unsupportedProcessor
processors[REQUEST_CODE.UNREGISTER_BROKER] = unsupportedProcessor
processors[REQUEST_CODE.WIPE_WRITE_PERM_OF_BROKER] = unsupportedProcessor
processors[REQUEST_CODE.ADD_WRITE_PERM_OF_BROKER] = unsupportedProcessor
processors[REQUEST_CODE.GET_ALL_TOPIC_LIST_FROM_NAMESERVER] = namesrvProcessor
processors[REQUEST_CODE.DELETE_TOPIC_IN_NAMESRV] = unsupportedProcessor
processors[REQUEST_CODE.GET_KVLIST_BY_NAMESPACE] = namesrvProcessor
processors[REQUEST_CODE.GET_TOPICS_BY_CLUSTER] = namesrvProcessor
processors[REQUEST_CODE.GET_SYSTEM_TOPIC_LIST_FROM_NS] = namesrvProcessor
processors[REQUEST_CODE.HEART_BEAT] = brokerBroadcastProcessor
processors[REQUEST_CODE.UNREGISTER_CLIENT] = brokerBroadcastProcessor
processors[REQUEST_CODE.GET_CONSUMER_LIST_BY_GROUP] = brokerSingleProcessor
processors[REQUEST_CODE.UPDATE_AND_CREATE_TOPIC] = brokerBroadcastProcessor

function _M:processRequest(sock, addr, h, body)
    local processor = processors[h.code]
    local resp
    if processor then
        resp = processor(self, addr, h, body)
    else
        resp = brokerProcessor(self, addr, h, body)
    end
    local send = core.encode(resp.code, resp.header, resp.body, false, h.opaque)
    sock:send(send)
end

return _M
