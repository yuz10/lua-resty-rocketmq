local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local rebalancer = require("resty.rocketmq.consumer.rebalancer")
local RESPONSE_CODE = core.RESPONSE_CODE
local cjson_safe = require("cjson.safe")
local ngx = ngx
local ngx_timer_at = ngx.timer.at
local split = utils.split

local log = ngx.log
local WARN = ngx.WARN
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
        clientID = '127.0.0.1@' .. ngx.worker.pid() .. '#' .. (ngx.now() * 1000),
        consumeMessageHookList = {},
    }, _M)
    for k, default in pairs(defaults) do
        consumer[k] = default
    end
    consumer.rebalancer = rebalancer.new(consumer)
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

local function sendHeartbeatToAllBroker(self)
    local subscriptionDataSet = {}
    for _, v in pairs(self.rebalancer.subscriptionInner) do
        table.insert(subscriptionDataSet, v)
    end
    local heartbeatData = {
        clientID = '' .. self.clientID,
        producerDataSet = setmetatable({}, cjson_safe.empty_array_mt),
        consumerDataSet = {
            {
                groupName = self.consumerGroup,
                consumeType = "CONSUME_ACTIVELY",
                messageModel = self.messageModel,
                consumeFromWhere = "CONSUME_FROM_LAST_OFFSET",
                subscriptionDataSet = subscriptionDataSet,
                unitMode = false,
            }
        }
    }
    print('sendHeartbeatToAllBroker', cjson_safe.encode(heartbeatData))
    for brokerName, brokers in pairs(self.client.brokerAddrTable) do
        local addr = brokers[0]
        print('sendHeartbeatToAllBroker1', addr)
        if addr then
            local h, b, err = self.client:sendHeartbeat(addr, heartbeatData)
            if err then
                log(WARN, 'fail to send heartbeat:', err)
            elseif h.code ~= RESPONSE_CODE.SUCCESS then
                log(WARN, 'fail to send heartbeat, code:', core.RESPONSE_CODE_NAME[h.code] or h.code, ',remark:', h.remark)
            end
        end
    end
end

local function buildSubscriptionData(topic, subExpression)
    if subExpression == nil or subExpression == '' then
        subExpression = '*'
    end
    local subscriptionData = {
        topic = topic,
        subString = subExpression,
        tagsSet = setmetatable({}, cjson_safe.empty_array_mt),
        codeSet = setmetatable({}, cjson_safe.empty_array_mt),
    }
    for _, tag in ipairs(split(subExpression, '||')) do
        if #tag > 0 then
            table.insert(subscriptionData.tagsSet, tag)
            table.insert(subscriptionData.codeSet, 0)
        end
    end
    return subscriptionData
end

local function updateTopicSubscribeInfoWhenSubscriptionChanged(self)
    for topic, _ in pairs(self.rebalancer.subscriptionInner) do
        local res, err = self.client:updateTopicRouteInfoFromNameserver(topic)
        if err then
            log(WARN, 'fail to updateTopicRouteInfoFromNameserver:', err)
        end
    end
end

function _M:subscribe(topic, subExpression)
    local subscriptionData = buildSubscriptionData(topic, subExpression)
    self.rebalancer.subscriptionInner[topic] = subscriptionData
    if self.running then
        sendHeartbeatToAllBroker(self)
        updateTopicSubscribeInfoWhenSubscriptionChanged(self)
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
    self.rebalancer:start()
end

function _M:start()
    local self = self
    self.running = true
    setTraceDispatcher(self)
    initRebalanceImpl(self)
    updateTopicSubscribeInfoWhenSubscriptionChanged(self)
    sendHeartbeatToAllBroker(self)
    local loop
    loop = function()
        if not self.running then
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
    self.running = false
    if self.traceDispatcher then
        self.traceDispatcher:stop()
    end
end

function _M:removeUnnecessaryMessageQueue(mq, pq)
    log(WARN, 'removeUnnecessaryMessageQueue')
end

function _M:removeDirtyOffset(mq)
    log(WARN, 'removeDirtyOffset')
end

function _M:messageQueueChanged(topic, mqList, allocateResult)
    log(WARN, 'messageQueueChanged')
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
