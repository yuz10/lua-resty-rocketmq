local queue = require("resty.rocketmq.queue")
local core = require("resty.rocketmq.core")

local _M = {}
_M.__index = _M
local produceHook = {}
local produceMt = { __index = produceHook }
local consumeHook = {}
local consumeMt = { __index = consumeHook }
_M.PRODUCE = "PRODUCE"
_M.CONSUME = "CONSUME"
_M.Pub = "Pub"
_M.SubBefore = "SubBefore"
_M.SubAfter = "SubAfter"
_M.EndTransaction = "EndTransaction"
local CONTENT_SPLITOR = string.char(1)
local FIELD_SPLITOR = string.char(2)
local TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"

function _M.new(nameservers, type)
    local producer = require("resty.rocketmq.producer")
    local p = producer.new(nameservers, "_INNER_TRACE_PRODUCER-" .. type)
    local q = queue.new()
    return setmetatable({
        producer = p,
        queue = q,
        hook = setmetatable({ queue = q },
                type == _M.PRODUCE and produceMt or consumeMt)
    }, _M)
end

function produceHook.sendMessageBefore(self, context)
    context.traceContext = {
        traceType = _M.Pub,
        groupName = context.producerGroup,
        topic = context.message.topic,
        tags = context.message.tags or '',
        keys = context.message.keys or '',
        storeHost = context.brokerAddr,
        bodyLength = #context.message.body,
        msgType = context.msgType,
        timestamp = ngx.now() * 1000,
    }
end

function produceHook.sendMessageAfter(self, context)
    local traceContext = context.traceContext or {}
    traceContext.costTime = ngx.now() * 1000 - traceContext.timestamp
    if context.sendResult then
        traceContext.success = true
        traceContext.msgId = context.sendResult.msgId
        traceContext.offsetMsgId = context.sendResult.offsetMsgId
        traceContext.storeTime = traceContext.timestamp + traceContext.costTime / 2
    else
        traceContext.success = false
    end
    queue.push(self.queue, traceContext)
end

function produceHook.endTransaction(self, context)
    local traceContext = {
        traceType = _M.EndTransaction,
        groupName = context.producerGroup,
        topic = context.message.topic,
        tags = context.message.tags or '',
        keys = context.message.keys or '',
        storeHost = context.brokerAddr,
        msgType = core.Trans_msg_Commit,
        msgId = context.msgId,
        transactionState = context.transactionState,
        transactionId = context.transactionId,
        fromTransactionCheck = context.fromTransactionCheck,
        timestamp = ngx.now() * 1000,
    }
    queue.push(self.queue, traceContext)
end

function consumeHook.consumeMessageBefore(self, context)
    -- consumer not supported
end

function consumeHook.consumeMessageAfter(self, context)
    -- consumer not supported
end

local function encoderFromContextBean(ctx)
    local data
    local traceType = ctx.traceType
    if traceType == _M.Pub then
        data = table.concat({
            ctx.traceType,
            ctx.timestamp,
            "DefaultRegion",
            ctx.groupName,
            ctx.topic,
            ctx.msgId,
            ctx.tags,
            ctx.keys,
            ctx.storeHost,
            ctx.bodyLength,
            ctx.costTime,
            ctx.msgType,
            ctx.offsetMsgId,
            tostring(ctx.success)
        }, CONTENT_SPLITOR) .. FIELD_SPLITOR
    elseif traceType == _M.EndTransaction then
        data = table.concat({
            ctx.traceType,
            ctx.timestamp,
            "DefaultRegion",
            ctx.groupName,
            ctx.topic,
            ctx.msgId,
            ctx.tags,
            ctx.keys,
            ctx.storeHost,
            ctx.msgType,
            ctx.transactionId,
            ctx.transactionState,
            tostring(ctx.fromTransactionCheck)
        }, CONTENT_SPLITOR) .. FIELD_SPLITOR
    end
    return {
        key = { ctx.msgId, ctx.keys }, data = data
    }
end

local function sendTraceDataByMQ(self, keySet, data, topic)
    local keys = {}
    for k in pairs(keySet) do
        table.insert(keys, k)
    end
    local res, err = self.producer:produce(TRACE_TOPIC, data, "", table.concat(keys, ' '))
    if err then
        ngx.log(ngx.WARN, 'send msg trace fail, ', err)
    end
end

local function sendTrace(self)
    local ctxList = {}
    while true do
        local res = queue.pop(self.queue)
        if not res then
            break
        end
        table.insert(ctxList, res)
    end
    if #ctxList > 0 then
        local dataMap = {}
        for _, ctx in ipairs(ctxList) do
            local topic = ctx.topic
            dataMap[topic] = dataMap[topic] or {}
            table.insert(dataMap[topic], encoderFromContextBean(ctx))
        end
        for topic, transList in pairs(dataMap) do
            local buffer = {}
            local keySet = {}
            local len = 0
            for _, trans in ipairs(transList) do
                for _, key in ipairs(trans.key) do
                    if key ~= '' then
                        keySet[key] = true
                    end
                end
                table.insert(buffer, trans.data)
                len = len + #trans.data
                if len >= core.maxMessageSize then
                    sendTraceDataByMQ(self, keySet, table.concat(buffer), topic)
                    keySet = {}
                    buffer = {}
                end
            end
            if #buffer > 0 then
                sendTraceDataByMQ(self, keySet, table.concat(buffer), topic)
            end
        end
    end
end

function _M.start(self)
    self.producer:start()
    local loop
    loop = function()
        if self.exit then
            return
        end
        sendTrace(self)
        ngx.timer.at(1, loop)
    end
    ngx.timer.at(1, loop)
end

function _M.stop(self)
    self.exit = true
    sendTrace(self)
    self.producer:stop()
end

return _M
