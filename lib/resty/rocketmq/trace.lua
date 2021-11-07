local queue = require("resty.rocketmq.queue")
local core = require("resty.rocketmq.core")
local utils = require("resty.rocketmq.utils")

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
        tags = context.message.properties.TAGS or '',
        keys = context.message.properties.KEYS or '',
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
        tags = context.message.properties.TAGS or '',
        keys = context.message.properties.KEYS or '',
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
    local keySet = {}
    local keys = utils.split(ctx.keys, ' ')
    for _, k in ipairs(keys) do
        if k ~= '' then
            keySet[k] = true
        end
    end
    keySet[ctx.msgId] = true
    return {
        key = keySet, data = data
    }
end

function _M.decoderFromTraceDataString(traceData)
    local contextList = utils.split(traceData, FIELD_SPLITOR)
    local resList = {}
    for _, context in ipairs(contextList) do
        local line = utils.split(context, CONTENT_SPLITOR)
        if line[1] == _M.Pub then
            local pubContext = {
                traceType = _M.Pub,
                timeStamp = tonumber(line[2]),
                regionId = line[3],
                groupName = line[4],
                topic = line[5],
                msgId = line[6],
                tags = line[7],
                keys = line[8],
                storeHost = line[9],
                bodyLength = line[10],
                costTime = line[11],
                msgType = core.msgType[tonumber(line[12])],
            }
            if #line == 13 then
                pubContext.success = line[13] == 'true'
            else
                pubContext.offsetMsgId = line[13]
                pubContext.success = line[14] == 'true'
                if #line >= 15 then
                    pubContext.clientHost = line[15]
                end
            end
            table.insert(resList, pubContext)
        elseif line[1] == _M.SubBefore then
            table.insert(resList, {
                traceType = _M.SubBefore,
                timeStamp = tonumber(line[2]),
                regionId = line[3],
                groupName = line[4],
                requestId = line[5],
                msgId = line[6],
                retryTimes = tonumber(line[7]),
                keys = line[8],
            })
        elseif line[1] == _M.SubAfter then
            local subAfterContext = {
                traceType = _M.SubAfter,
                requestId = tonumber(line[2]),
                msgId = line[3],
                costTime = line[4],
                success = line[5],
                keys = line[6],
            }
            if #line >= 7 then
                subAfterContext.contextCode = tonumber(line[7])
            end
            if #line >= 9 then
                subAfterContext.timeStamp = line[8]
                subAfterContext.groupName = line[9]
            end
            table.insert(resList, subAfterContext)
        elseif line[1] == _M.EndTransaction then
            table.insert(resList, {
                TraceType = _M.EndTransaction,
                TimeStamp = tonumber(line[2]),
                RegionId = line[3],
                GroupName = line[4],
                Topic = line[5],
                MsgId = line[6],
                Tags = line[7],
                Keys = line[8],
                StoreHost = line[9],
                MsgType = core.msgType[tonumber(line[10])],
                TransactionId = line[11],
                TransactionState = line[12],
                FromTransactionCheck = line[13] == 'true',
            })
        end
    end
    return resList
end

local function sendTraceDataByMQ(self, keySet, data, topic)
    local keys = {}
    for k in pairs(keySet) do
        table.insert(keys, k)
    end
    local res, err = self.producer:send(core.RMQ_SYS_TRACE_TOPIC, data, "", table.concat(keys, ' '))
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
                for key in pairs(trans.key) do
                    keySet[key] = true
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
