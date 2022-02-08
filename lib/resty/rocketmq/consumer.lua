local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local rebalancer = require("resty.rocketmq.consumer.rebalancer")
local offsetstore = require("resty.rocketmq.consumer.offsetstore")
local admin = require("resty.rocketmq.admin")
local queue = require("resty.rocketmq.queue")

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

_M.CONSUME_FROM_LAST_OFFSET = 'CONSUME_FROM_LAST_OFFSET'
_M.CONSUME_FROM_FIRST_OFFSET = 'CONSUME_FROM_FIRST_OFFSET'
_M.CONSUME_FROM_TIMESTAMP = 'CONSUME_FROM_TIMESTAMP'

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

local defaults = {
    allocateMessageQueueStrategy = _M.AllocateMessageQueueAveragely,
    enableMsgTrace = false,
    customizedTraceTopic = core.RMQ_SYS_TRACE_TOPIC,
    useTLS = false,
    timeout = 3000,
    messageModel = _M.CLUSTERING,
    consumeFromWhere = _M.CONSUME_FROM_LAST_OFFSET,
    consumeTimestamp = 0,
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
        clientID = '127.0.0.1@' .. ngx.worker.pid() .. '#' .. (ngx.now() * 1000),
        consumeMessageHookList = {},
    }, _M)
    for k, default in pairs(defaults) do
        consumer[k] = default
    end
    consumer.client = cli
    consumer.rebalancer = rebalancer.new(consumer)
    consumer.offsetStore = offsetstore.new(consumer)
    consumer.admin = admin.new(nameservers)
    admin.client = cli
    consumer.pullRequestQueue = queue.new()
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

local function sendHeartbeatToAllBroker(self, sock_map)
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
                consumeType = "CONSUME_PASSIVELY",
                messageModel = self.messageModel,
                consumeFromWhere = self.consumeFromWhere,
                subscriptionDataSet = subscriptionDataSet,
                unitMode = false,
            }
        }
    }
    self.client:sendHeartbeatToAllBroker(sock_map, heartbeatData)
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

local function copySubscription(self)
    self:subscribe(core.RETRY_GROUP_TOPIC_PREFIX .. self.consumerGroup, "*")
end

local function startPullMessageService(self)

end

function _M:start()
    local self = self
    self.running = true
    setTraceDispatcher(self)
    copySubscription(self)
    initRebalanceImpl(self)
    updateTopicSubscribeInfoWhenSubscriptionChanged(self)
    ngx_timer_at(0, function()
        local sock_map = {}
        while self.running do
            self.client:updateAllTopicRouteInfoFromNameserver()
            sendHeartbeatToAllBroker(self, sock_map)
            ngx.sleep(30)
        end
    end)
    if self.traceDispatcher then
        self.traceDispatcher:start()
    end
    startPullMessageService(self)
end

function _M:stop()
    self.running = false
    if self.traceDispatcher then
        self.traceDispatcher:stop()
    end
end

function _M:removeUnnecessaryMessageQueue(mq, pq)
    self.offsetStore:persist(mq);
    self.offsetStore:removeOffset(mq);
end

function _M:removeDirtyOffset(mq)
    self.offsetStore:removeOffset(mq)
end

function _M:messageQueueChanged(topic, mqList, allocateResult)
    log(WARN, 'messageQueueChanged:', topic, ',allocateResult:', cjson_safe.encode(allocateResult))
end

function _M:computePullFromWhere(mq)
    print(cjson_safe.encode(mq))
    local lastOffset, err = self.offsetStore:readOffset(mq, offsetstore.READ_FROM_STORE)
    lastOffset = lastOffset or -1
    if lastOffset >= 0 then
        return lastOffset
    elseif lastOffset <= -2 then
        return -1
    end
    if self.consumeFromWhere == _M.CONSUME_FROM_LAST_OFFSET then
        if utils.startsWith(mq.topic, core.RETRY_GROUP_TOPIC_PREFIX) then
            return 0
        else
            return self.admin:maxOffset(mq)
        end
    elseif self.consumeFromWhere == _M.CONSUME_FROM_FIRST_OFFSET then
        return 0
    else
        if utils.startsWith(mq.topic, core.RETRY_GROUP_TOPIC_PREFIX) then
            return self.admin:maxOffset(mq)
        else
            return self.admin:searchOffset(mq, self.consumeTimestamp)
        end
    end
end

function _M:dispatchPullRequest(pullRequestList)
    for _, pullRequest in ipairs(pullRequestList) do
        queue.push(self.pullRequestQueue, pullRequest)
    end
end

return _M
