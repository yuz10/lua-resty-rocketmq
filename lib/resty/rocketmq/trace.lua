local queue = require("resty.rocketmq.queue")
local core = require("resty.rocketmq.core")
local utils = require("resty.rocketmq.utils")
local ngx = ngx
local ngx_timer_at = ngx.timer.at

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
local CONTENT_SPLITTER = string.char(1)
local FIELD_SPLITTER = string.char(2)

function _M.new(nameservers, type, customizedTraceTopic)
    local producer = require("resty.rocketmq.producer")
    local p = producer.new(nameservers, "_INNER_TRACE_PRODUCER-" .. type)
    local q = queue.new()
    return setmetatable({
        producer = p,
        queue = q,
        trace_topic = customizedTraceTopic or core.RMQ_SYS_TRACE_TOPIC,
        hook = setmetatable({ queue = q },
            type == _M.PRODUCE and produceMt or consumeMt)
    }, _M)
end

function produceHook.sendMessageBefore(self, context)
    context.traceContext = {
        traceType = _M.Pub,
        groupName = context.producerGroup,
        traceBeans = {
            {
                topic = context.message.topic,
                tags = context.message.properties.TAGS or 'null',
                keys = context.message.properties.KEYS or 'null',
                storeHost = context.brokerAddr,
                bodyLength = #context.message.body,
                msgType = context.msgType,
            }
        },
        timestamp = ngx.now() * 1000,
    }
end

function produceHook.sendMessageAfter(self, context)
    local traceContext = context.traceContext
    traceContext.costTime = ngx.now() * 1000 - traceContext.timestamp
    if context.sendResult then
        traceContext.success = true
        local traceBean = traceContext.traceBeans[1]
        traceBean.msgId = context.sendResult.msgId
        traceBean.offsetMsgId = context.sendResult.offsetMsgId
        traceBean.storeTime = traceContext.timestamp + traceContext.costTime / 2
    else
        traceContext.success = false
    end
    queue.push(self.queue, traceContext)
end

function produceHook.endTransaction(self, context)
    local traceContext = {
        traceType = _M.EndTransaction,
        groupName = context.producerGroup,
        traceBeans = {
            {
                topic = context.message.topic,
                tags = context.message.properties.TAGS or 'null',
                keys = context.message.properties.KEYS or 'null',
                storeHost = context.brokerAddr,
                msgType = core.Trans_msg_Commit,
                msgId = context.msgId,
                transactionState = context.transactionState,
                transactionId = context.transactionId,
                fromTransactionCheck = context.fromTransactionCheck,
            }
        },
        timestamp = ngx.now() * 1000,
    }
    queue.push(self.queue, traceContext)
end

function consumeHook.consumeMessageBefore(self, context)
    if #context.msgList == 0 then
        return
    end
    context.traceContext = {
        traceType = _M.SubBefore,
        groupName = context.consumerGroup,
        timestamp = ngx.now() * 1000,
        requestId = utils.genUniqId(),
        traceBeans = {},
    }
    for _, message in ipairs(context.msgList) do
        table.insert(context.traceContext.traceBeans, {
            topic = message.topic,
            msgId = message.properties.UNIQ_KEY,
            tags = message.properties.TAGS or 'null',
            keys = message.properties.KEYS or 'null',
            storeTime = message.storeTimestamp,
            bodyLength = message.storeSize,
            retryTimes = message.reconsumeTimes,
        })
    end
    queue.push(self.queue, context.traceContext)
end

function consumeHook.consumeMessageAfter(self, context)
    local subBeforeContext = context.traceContext
    if #subBeforeContext.traceBeans == 0 then
        return
    end
    local subAfterContext = {
        traceType = _M.SubAfter,
        groupName = subBeforeContext.groupName,
        timestamp = ngx.now() * 1000,
        requestId = subBeforeContext.requestId,
        success = context.success,
        costTime = (ngx.now() * 1000 - subBeforeContext.timestamp) / #context.msgList,
        traceBeans = subBeforeContext.traceBeans,
        contextCode = context.consumeContextType,
    }
    queue.push(self.queue, subAfterContext)
end

local function encoderFromContextBean(ctx)
    local data = ''
    local traceType = ctx.traceType
    if traceType == _M.Pub then
        for _, bean in ipairs(ctx.traceBeans) do
            data = data .. table.concat({
                ctx.traceType,
                ctx.timestamp,
                "DefaultRegion",
                ctx.groupName,
                bean.topic,
                bean.msgId,
                bean.tags,
                bean.keys,
                bean.storeHost,
                bean.bodyLength,
                ctx.costTime,
                bean.msgType,
                bean.offsetMsgId,
                tostring(ctx.success)
            }, CONTENT_SPLITTER) .. FIELD_SPLITTER
        end
    elseif traceType == _M.EndTransaction then
        for _, bean in ipairs(ctx.traceBeans) do
            data = data .. table.concat({
                ctx.traceType,
                ctx.timestamp,
                "DefaultRegion",
                ctx.groupName,
                bean.topic,
                bean.msgId,
                bean.tags,
                bean.keys,
                bean.storeHost,
                bean.msgType,
                bean.transactionId,
                bean.transactionState,
                tostring(bean.fromTransactionCheck)
            }, CONTENT_SPLITTER) .. FIELD_SPLITTER
        end
    elseif traceType == _M.SubBefore then
        for _, bean in ipairs(ctx.traceBeans) do
            data = data .. table.concat({
                ctx.traceType,
                ctx.timestamp,
                "DefaultRegion",
                ctx.groupName,
                ctx.requestId,
                bean.msgId,
                bean.retryTimes,
                bean.keys,
            }, CONTENT_SPLITTER) .. FIELD_SPLITTER
        end
    elseif traceType == _M.SubAfter then
        for _, bean in ipairs(ctx.traceBeans) do
            data = data .. table.concat({
                ctx.traceType,
                ctx.requestId,
                bean.msgId,
                ctx.costTime,
                tostring(ctx.success),
                bean.keys,
                ctx.contextCode,
                ctx.timestamp,
                ctx.groupName,
            }, CONTENT_SPLITTER) .. FIELD_SPLITTER
        end
    end
    local keySet = {}
    for _, bean in ipairs(ctx.traceBeans) do
        local keys = utils.split(bean.keys, ' ')
        for _, k in ipairs(keys) do
            if k ~= '' then
                keySet[k] = true
            end
        end
        keySet[bean.msgId] = true
    end
    return {
        key = keySet, data = data
    }
end

function _M.decoderFromTraceDataString(msg)
    local traceData = msg.body
    local contextList = utils.split(traceData, FIELD_SPLITTER)
    local resList = {}
    for _, context in ipairs(contextList) do
        local line = utils.split(context, CONTENT_SPLITTER)
        if line[1] == _M.Pub then
            local pubContext = {
                clientHost = msg.bornHost,
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
                clientHost = msg.bornHost,
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
                clientHost = msg.bornHost,
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
                clientHost = msg.bornHost,
                traceType = _M.EndTransaction,
                timeStamp = tonumber(line[2]),
                regionId = line[3],
                groupName = line[4],
                topic = line[5],
                msgId = line[6],
                tags = line[7],
                keys = line[8],
                storeHost = line[9],
                msgType = core.msgType[tonumber(line[10])],
                transactionId = line[11],
                transactionState = line[12],
                fromTransactionCheck = line[13] == 'true',
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
    local res, err = self.producer:send(self.trace_topic, data, "", table.concat(keys, ' '))
    if err then
        ngx.log(ngx.WARN, 'send msg trace fail, ', err)
    end
end

local function sendTrace(self)
    local dataMap = {}
    while true do
        local ctx = queue.pop(self.queue)
        if not ctx then
            break
        end
        local topic = ctx.traceBeans[1].topic
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
            if len >= 11800 then
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

function _M.start(self)
    self.producer:start()
    local loop
    loop = function()
        if self.exit then
            return
        end
        sendTrace(self)
        ngx_timer_at(1, loop)
    end
    ngx_timer_at(1, loop)
end

function _M.stop(self)
    self.exit = true
    sendTrace(self)
    self.producer:stop()
end

return _M
