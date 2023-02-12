local cjson_safe = require("cjson.safe")
local acl_rpchook = require("resty.rocketmq.acl_rpchook")
local producer = require("resty.rocketmq.producer")
local utils = require("resty.rocketmq.utils")
local client = require("resty.rocketmq.client")

local ngx = ngx
local log = ngx.log
local WARN = ngx.WARN

local _M = {}
_M.__index = _M

function _M.new(config)
    local p, err = producer.new(config.nameservers)
    if not p then
        print("create producer err:", err)
        return
    end
    p:setUseTLS(config.use_tls)
    if config.access_key and config.secret_key then
        p:addRPCHook(acl_rpchook.new(config.access_key, config.secret_key))
    end
    return setmetatable({ p = p }, _M)
end

local function error(status, message)
    ngx.status = status
    ngx.say(message)
    ngx.exit(0)
end

function _M:message()
    local method = ngx.req.get_method()
    local args = ngx.req.get_uri_args()
    local topic = ngx.var.topic
    if method == 'GET' then
        if args.consumer == nil or args.consumer == '' then
            error(400, cjson_safe.encode { error_code = '003', error_msg = "no consumer group" })
            return
        end
        self:consume_message(topic, args.consumer, args.numOfMessages or 16, args.waitseconds or 10)
    elseif method == 'POST' then
        self:produce_message(topic)
    end
end

function _M:ack()
    local method = ngx.req.get_method()
    local topic = ngx.var.topic
    local args = ngx.req.get_uri_args()
    if method == 'PUT' then
        self:ack_message(topic, args.consumer)
    end
end

function _M:produce_message(topic)
    ngx.req.read_body()
    local data = ngx.req.get_body_data()
    local data_t, err = cjson_safe.decode(data)
    if err then
        error(400, cjson_safe.encode { error_code = '001', error_msg = "invalid json format" })
    end
    local body = data_t.body or data_t.messageBody
    if not body then
        error(400, cjson_safe.encode { error_code = '002', error_msg = "no message body" })
    end
    local properties = data_t.properties or {}
    properties.UNIQ_KEY = utils.genUniqId()
    properties.WAIT = properties.WAIT or 'true'
    local msg = {
        producerGroup = "proxy_producer",
        topic = topic,
        defaultTopic = "TBW102",
        defaultTopicQueueNums = 4,
        sysFlag = 0,
        bornTimeStamp = ngx.now() * 1000,
        flag = 0,
        properties = properties,
        reconsumeTimes = 0,
        unitMode = false,
        maxReconsumeTimes = 0,
        batch = false,
        body = body,
    }
    local res, err = self.p:produce(msg)
    if not res then
        error(400, err)
    end
    ngx.say(cjson_safe.encode({
        msg_id = res.sendResult.msgId,
        offset_msg_id = res.sendResult.offsetMsgId,
        broker = res.sendResult.messageQueue.brokerName,
        queue_id = res.sendResult.messageQueue.queueId,
        queue_offset = res.sendResult.queueOffset,
    }))
end

function _M:consume_message(topic, group, num, wait_seconds)
    local popResult, err = self.p.client:pop({ brokerName = 'broker-0', topic = topic, queueId = -1 }, 60000, num, group,
            wait_seconds * 1000, true, client.INIT_MODE_MIN, nil, "*")
    if not popResult then
        error(500, cjson_safe.encode { error_code = '004', error_msg = tostring(err) })
    end
    local msgFoundList = popResult.msgFoundList
    local messages = msgFoundList or setmetatable({}, cjson_safe.array_mt)
    for _, msg in ipairs(messages) do
        msg.receiptHandle = utils.toHex(msg.properties.POP_CK)
    end
    ngx.say(cjson_safe.encode {
        messages = messages
    })
end

function _M:ack_message(topic, group)
    ngx.req.read_body()
    local data = ngx.req.get_body_data()
    local data_t, err = cjson_safe.decode(data)
    if err then
        error(400, cjson_safe.encode { error_code = '001', error_msg = "invalid json format" })
    end
    for _, receiptHandle in ipairs(data_t.receiptHandles) do
        local res, err = self.p.client:doAck(topic, group, utils.fromHex(receiptHandle))
        if not res then
            error(500, cjson_safe.encode { error_code = '004', error_msg = tostring(err) })
        end
    end
    ngx.say(cjson_safe.encode {
        result = "ok"
    })
end

return _M
