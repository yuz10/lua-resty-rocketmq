local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local remoting = require("resty.rocketmq.remoting")
local utils = require("resty.rocketmq.utils")
local ngx = ngx

local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE

local _M = {}
_M.__index = _M

function _M.new(config)
    local broker = setmetatable({
        clusterName = config.clusterName,
        brokerName = config.brokerName,
        brokerId = tonumber(config.brokerId),
        brokerAddr = config.brokerAddr,
        nameservers = config.nameservers,
        topicConfigTable = {},
        dataVersion = { timestamp = ngx.now() * 1000, counter = 0 },
    }, _M)
    return remoting.new(broker)
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

local function registerBrokerAll(self)
    local body = cjson_safe.encode({
        topicConfigSerializeWrapper = {
            topicConfigTable = self.topicConfigTable,
            dataVersion = self.dataVersion,
        }
    })
    local header = {
        clusterName = self.clusterName,
        brokerName = self.brokerName,
        brokerAddr = self.brokerAddr,
        brokerId = self.brokerId,
        compressed = false,
        bodyCrc32 = 0,
    }
    for _, nameserver in ipairs(self.nameservers) do
        local h, b, err = core.request(REQUEST_CODE.REGISTER_BROKER, nameserver, header, body, false)
        ngx.log(ngx.WARN, 'register broker ', err)
    end
end

function _M:start()
    ngx.timer.every(60, function()
        registerBrokerAll(self)
    end)
end

local processors = {}
local sendMessageProcessor = function(self, addr, h, body)
    local header = h.extFields
    if h.code == REQUEST_CODE.SEND_MESSAGE_V2 or h.code == REQUEST_CODE.SEND_BATCH_MESSAGE then
        header.producerGroup = header.a
        header.topic = header.b
        header.defaultTopic = header.c
        header.defaultTopicQueueNums = header.d
        header.queueId = header.e
        header.sysFlag = header.f
        header.bornTimestamp = header.g
        header.flag = header.h
        header.properties = header.i
        header.reconsumeTimes = header.j
        header.unitMode = header.k
        header.maxReconsumeTimes = header.l
        header.batch = header.m
    end
    
    local resp = res(RESPONSE_CODE.SUCCESS, nil, nil)
    resp.header.msgId = "msgId"
    resp.header.queueId = header.queueId
    resp.header.queueOffset = "0"
    resp.header.MSG_REGION = "DefaultRegion"
    resp.header.TRACE_ON = "true"
    return resp
end

processors[REQUEST_CODE.SEND_BATCH_MESSAGE] = sendMessageProcessor
processors[REQUEST_CODE.SEND_MESSAGE_V2] = sendMessageProcessor
processors[REQUEST_CODE.SEND_MESSAGE] = sendMessageProcessor

processors[REQUEST_CODE.CONSUMER_SEND_MSG_BACK] = function(self, addr, h, body)
    return res(RESPONSE_CODE.SUCCESS, nil, nil)
end

processors[REQUEST_CODE.GET_BROKER_RUNTIME_INFO] = function(self, addr, h, body)
    return res(RESPONSE_CODE.SUCCESS, nil, {
        table = {
            putTps = 0,
            getTransferedTps = 0,
        }
    })
end

processors[REQUEST_CODE.UPDATE_AND_CREATE_TOPIC] = function(self, addr, h, body)
    local header = h.extFields
    local topic = header.topic
    if core.isTopicOrGroupIllegal(topic) or core.isSystemTopic(topic) then
        return res(RESPONSE_CODE.SYSTEM_ERROR, "Invalid topic", nil)
    end
    self.topicConfigTable[topic] = {
        topicName = topic,
        readQueueNums = header.readQueueNums,
        writeQueueNums = header.writeQueueNums,
        topicFilterType = header.topicFilterType,
        perm = header.perm,
        topicSysFlag = header.topicSysFlag or 0,
    }
    self.dataVersion.timestamp = ngx.now() * 1000
    self.dataVersion.counter = self.dataVersion.counter + 1
    registerBrokerAll(self)
    return res(RESPONSE_CODE.SUCCESS, nil, nil)
end

function _M:processRequest(sock, addr, h, body)
    local processor = processors[h.code]
    local resp
    if processor then
        resp = processor(self, addr, h, body)
    else
        resp = res(RESPONSE_CODE.REQUEST_CODE_NOT_SUPPORTED, ('request code %s not supported'):format(core.REQUEST_CODE_NAME[h.code]))
    end
    local send = core.encode(resp.code, resp.header, resp.body, false, h.opaque)
    sock:send(send)
end

return _M
