local cjson_safe = require("cjson.safe")
local acl_rpchook = require("resty.rocketmq.acl_rpchook")
local producer = require("resty.rocketmq.producer")
local utils = require("resty.rocketmq.utils")
local client = require("resty.rocketmq.client")
local xml2lua = require("resty.rocketmq.xml2lua")
local resty_md5 = require("resty.md5")
local str = require "resty.string"
local ngx = ngx
local log = ngx.log
local ERR = ngx.ERR

local _M = {}
_M.__index = _M

local function md5(body)
    local instance = resty_md5:new()
    instance:update(body)
    return str.to_hex(instance:final())
end

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

--[[
<ErrorResponse>
    <Error>
        <Type>Sender</Type>
        <Code>SignatureDoesNotMatch</Code>
        <Message>message
        </Message>
        <Detail/>
    </Error>
    <RequestId>e2a13e28-8d1a-5d32-8370-612a7f1a46a9</RequestId>
</ErrorResponse>
]]
local function error(status, message)
    log(ERR, 'status:', status, ',message:', message)
    ngx.status = status
    local request_id = ngx.var.request_id
    ngx.header['x-amzn-RequestId'] = request_id
    ngx.say(xml2lua.toXml({
        ErrorResponse = {
            Error = {
                Type = "Sender",
                Message = message,
            },
            RequestId = request_id
        }
    }))
    ngx.exit(0)
end

function _M:process()
    local method = ngx.req.get_method()
    if method ~= 'POST' then
        error(405, 'method not allowed')
    end
    ngx.req.read_body()
    local data = ngx.req.get_body_data()
    local request = ngx.decode_args(data)
    local request_id = ngx.var.request_id
    local resp
    if request.Action == 'SendMessage' then
        resp = self:sendMessage(request)
    elseif request.Action == 'SendMessageBatch' then
        resp = self:sendMessageBatch(request)
    elseif request.Action == 'ReceiveMessage' then
        resp = self:receiveMessage(request)
    elseif request.Action == 'DeleteMessage' then
        resp = self:deleteMessage(request)
    elseif request.Action == 'DeleteMessageBatch' then
        resp = self:deleteMessageBatch(request)
    else
        error(400, 'action not supported')
    end
    local response = xml2lua.toXml(resp)
    ngx.header['x-amzn-RequestId'] = request_id
    ngx.say(response)
end

--[[
req
{
    "Action": "SendMessage",
    "QueueUrl": "TopicTest",
    "MessageBody": "body",
    "DelaySeconds": "10",
    "MessageGroupId": "string",
    "MessageAttribute.1.Name": "key",
    "MessageAttribute.1.Value.DataType": "String",
    "MessageAttribute.1.Value.StringValue": "value",
    "Version": "2012-11-05"
}
resp
<SendMessageResponse>
    <SendMessageResult>
        <MessageId>dd9b9d76-6219-4ff5-b8d4-92f653af59a6</MessageId>
        <MD5OfMessageBody>f95adbce0a51589cb6e87112eb6becd4</MD5OfMessageBody>
        <MD5OfMessageAttributes>xxx</MD5OfMessageBody>
        <MD5OfMessageSystemAttributes>xxx</MD5OfMessageBody>
        <SequenceNumber>xxx</MD5OfMessageBody>
    </SendMessageResult>
    <ResponseMetadata>
        <RequestId>ac4c18b3-c25b-566e-8576-47b133d77d9b</RequestId>
    </ResponseMetadata>
</SendMessageResponse>
]]
function _M:sendMessage(request)
    local body = request.MessageBody
    if not body then
        error(400, "no message body")
    end
    local properties = request.properties or {}
    properties.UNIQ_KEY = utils.genUniqId()
    properties.WAIT = properties.WAIT or 'true'
    properties.TIMER_DELAY_SEC = request.DelaySeconds
    local mqSelector = nil
    if request.MessageGroupId then
        properties.__SHARDINGKEY = request.MessageGroupId
        mqSelector = function(queueList, msg)
            local groupId =  msg.properties.__SHARDINGKEY
            local hash = utils.java_hash(groupId)
            return queueList[(hash % #queueList) + 1]
        end
    end
    local msg = {
        producerGroup = "sqs_producer",
        topic = request.QueueUrl,
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
    local res, err = self.p:produce(msg, mqSelector)
    if not res then
        error(400, err)
    end

    return {
        SendMessageResponse = {
            SendMessageResult = {
                MessageId = res.sendResult.msgId,
                MD5OfMessageBody = md5(body),
                --MD5OfMessageAttributes = "string", todo unsupported
                --MD5OfMessageSystemAttributes = "string",
                --SequenceNumber = i
            }
        }
    }
end
--[[
{
    "Action": "SendMessageBatch",
    "QueueUrl": "TopicTest",
    "SendMessageBatchRequestEntry.1.Id": "id1",
    "SendMessageBatchRequestEntry.1.MessageBody": "msg1",
    "SendMessageBatchRequestEntry.1.DelaySeconds": "10",
    "SendMessageBatchRequestEntry.1.MessageGroupId": "string",
    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name": "key",
    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue": "value",
    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType": "String",
    "Version": "2012-11-05"
}

<SendMessageBatchResponse>
    <SendMessageBatchResult>
        <SendMessageBatchResultEntry>
            <Id>id1</Id>
            <MessageId>02a94f14-9bb7-4dcf-9e55-8a9ce5f372f3</MessageId>
            <MD5OfMessageBody>3fccf7e8ef8bb6df5c1a77f579c5b914</MD5OfMessageBody>
            <MD5OfMessageAttributes>80176cdee1a774a6892d24cec267ac2f</MD5OfMessageAttributes>
        </SendMessageBatchResultEntry>
    </SendMessageBatchResult>
    <ResponseMetadata>
        <RequestId>e5270dac-4792-5d0e-b59a-52af57995ffe</RequestId>
    </ResponseMetadata>
</SendMessageBatchResponse>
]]
function _M:sendMessageBatch(request)
    local success = setmetatable({}, cjson_safe.array_mt)
    local fail = setmetatable({}, cjson_safe.array_mt)
    local i = 1
    while true do
        local id = request['SendMessageBatchRequestEntry.' .. i .. '.Id']
        if id == nil then
            break
        end
        local body = request['SendMessageBatchRequestEntry.' .. i .. '.MessageBody']
        if not body then
            error(400, "no message body")
        end
        local properties = request.properties or {}
        properties.UNIQ_KEY = utils.genUniqId()
        properties.WAIT = properties.WAIT or 'true'
        properties.TIMER_DELAY_SEC = request['SendMessageBatchRequestEntry.' .. i .. '.DelaySeconds']
        local mqSelector = nil
        local messageGroup = request['SendMessageBatchRequestEntry.' .. i .. '.MessageGroupId']
        if messageGroup then
            properties.__SHARDINGKEY = messageGroup
            mqSelector = function(queueList, msg)
                local groupId =  msg.properties.__SHARDINGKEY
                local hash = utils.java_hash(groupId)
                return queueList[(hash % #queueList) + 1]
            end
        end
        local msg = {
            producerGroup = "sqs_producer",
            topic = request.QueueUrl,
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
        local res, err = self.p:produce(msg, mqSelector)
        if not res then
            table.insert(fail, {
                Code = "SendFail",
                Id = id,
                Message = err,
                SenderFault = false
            })
        else
            table.insert(success, {
                Id = id,
                MessageId = res.sendResult.msgId,
                MD5OfMessageBody = md5(body),
                --MD5OfMessageAttributes = "string", todo unsupported
                --MD5OfMessageSystemAttributes = "string",
                --SequenceNumber = i
            })
        end
        i = i + 1
    end

    return {
        SendMessageBatchResponse = {
            SendMessageBatchResult = {
                SendMessageBatchResultEntry = success,
                BatchResultErrorEntry = fail,
            }
        }
    }
end

--[[
{
    "Action": "ReceiveMessage",
    "QueueUrl": "TopicTest",
    "MaxNumberOfMessages": "1",
    "VisibilityTimeout": "60",
    "WaitTimeSeconds": "60",
    "AttributeNames": [ "string" ],
    "MessageAttributeNames": [ "string" ],
    "MessageSystemAttributeNames": [ "string" ],
    "ReceiveRequestAttemptId": "string",
    "Version": "2012-11-05",
}
<ReceiveMessageResponse>
    <ReceiveMessageResult>
        <Message>
            <MessageId>3980e413-4935-4d86-9fec-17e6fe487eda</MessageId>
            <ReceiptHandle>
                AQEBxYnRHQPmFgcl7CZBa9L9BLk+FHlSCQfLoCS0tlGXb4XDRbI3gjZPFG3gnuLdjPDRXESqGOXFzzQBaZ/a30wJjEUMJvj+jwhfyh8gfAgt5CYxjqBXflavph4UUX5fdxltgNhMvyWp7Vf1W4s6HkVfkiNHAu3DYQ7ZZ+FEW/nM+aLwvFejhRz2Imp3Sy7Va9pH6iBOD4D5Pnpn2/VfEvIvP9rE49o9FfssLVc8gYZyXOBtLCH4RYB26FL69BmUHZvtjo7ChTspiGtBYYQn3bBf07XHDXuthcS1EpLRs07qSmQ+rXDbTeVzTmV/TLcdc/e6DqtcCWve5d4zEHPcvTrFx9jPYB7mdl8YWhKc3L4H+fqEzuRCFDc/VO5QEE9gCfU/YHCcchx5knFRMfCu7qYJ7Q==
            </ReceiptHandle>
            <MD5OfBody>841a2d689ad86bd1611447453c22c6fc</MD5OfBody>
            <Body>body</Body>
        </Message>
    </ReceiveMessageResult>
    <ResponseMetadata>
        <RequestId>f91f1b38-08e8-59cd-aace-443b1ae6f4e6</RequestId>
    </ResponseMetadata>
</ReceiveMessageResponse>
]]
function _M:receiveMessage(request)
    local wait_seconds = request.WaitTimeSeconds or 5
    local num = request.MaxNumberOfMessages or 1
    local invisibleTime = request.VisibilityTimeout or 60
    local topic = request.QueueUrl
    local group = "GID_SQS"
    local popResult, err = self.p.client:pop({ brokerName = 'broker-0', topic = topic, queueId = -1 },
            invisibleTime * 1000, num, group, wait_seconds * 1000, true, client.INIT_MODE_MIN, nil, "*")
    if not popResult then
        error(500, tostring(err))
    end
    --[[{
    "popStatus": "FOUND",
    "msgFoundList": [
        {
            "storeSize": 216,
            "bodyCRC": 1330857165,
            "sysFlag": 0,
            "bornTimeStamp": "1719986468533",
            "flag": 0,
            "reconsumeTimes": 0,
            "queueOffset": "0",
            "storeHost": "172.28.198.133:10100",
            "preparedTransactionOffset": "0",
            "brokerName": "broker-0",
            "queueId": 4,
            "body": "12",
            "properties": {
                "CLUSTER": "DefaultCluster",
                "TRACE_ON": "true",
                "UNIQ_KEY": "AC110001CFF14C8733300D4ECEB40000",
                "1ST_POP_TIME": "1720259846793",
                "WAIT": "true",
                "MSG_REGION": "DefaultRegion",
                "POP_CK": "0 1720259846793 60000 2 0 broker-0 4 0"
            },
            "msgId": "AC1CC685000027740000000058595CDF",
            "commitLogOffset": "5777218783",
            "storeTimestamp": "1719986468549",
            "bornHost": "127.0.0.1:35048",
            "topic": "TopicTest"
        }
    ],
    "popTime": 1720259846793,
    "invisibleTime": 60000,
    "restNum": 48
}
{"restNum":0,"status":"POLLING_NOT_FOUND"}
]]
    local messages = setmetatable({}, cjson_safe.array_mt)
    if popResult.msgFoundList == nil or #popResult.msgFoundList == 0 then
        return {
            ReceiveMessageResponse = {
                ReceiveMessageResult = {
                }
            }
        }
    end
    for _, msg in ipairs(popResult.msgFoundList) do
        table.insert(messages, {
            ReceiptHandle = str.to_hex(msg.properties.POP_CK),
            MessageId = msg.properties.UNIQ_KEY,
            MD5OfBody = md5(msg.body),
            Body = msg.body,
        })
    end
    return {
        ReceiveMessageResponse = {
            ReceiveMessageResult = {
                Message = messages
            }
        }
    }
end

--[[
{
    "Action": "DeleteMessage",
    "QueueUrl": "TopicTest",
    "ReceiptHandle": "302031373230323630313238303431203630303030203320302062726F6B65722D3020302030",
    "Version": "2012-11-05",
}
<DeleteMessageResponse>
    <ResponseMetadata>
        <RequestId>582ee1c2-8d47-50af-9369-bec19418dcf9</RequestId>
    </ResponseMetadata>
</DeleteMessageResponse>
]]
function _M:deleteMessage(request)
    local topic = request.QueueUrl
    local group = "GID_SQS"
    local handle = utils.fromHex(request.ReceiptHandle)
    if handle == nil then
        error(400, "The input receipt handle is invalid.")
    end
    local res, err = self.p.client:doAck(topic, group, handle)
    if not res then
        error(500, tostring(err))
    end
    return {
        DeleteMessageResponse = {}
    }
end
--[[
{
    "Action": "DeleteMessageBatch",
    "QueueUrl": "TopicTest",
    "DeleteMessageBatchRequestEntry.1.Id": "AC110001D07F4C8733300D4F10230000",
    "DeleteMessageBatchRequestEntry.1.ReceiptHandle": "31392031373230323631353636323334203630303030203720322062726F6B65722D302030203139",
    "Version": "2012-11-05",
}
<DeleteMessageBatchResponse>
  <DeleteMessageBatchResult>
    <DeleteMessageBatchResultEntry>
      <Id>ecfab988-27f3-4687-b1b3-debe7898f84c</Id>
    </DeleteMessageBatchResultEntry>
    <BatchResultErrorEntry>
      <Id>err</Id>
      <Code>ReceiptHandleIsInvalid</Code>
      <Message>The input receipt handle is invalid.</Message>
      <SenderFault>true</SenderFault>
    </BatchResultErrorEntry>
  </DeleteMessageBatchResult>
  <ResponseMetadata>
    <RequestId>717fae28-2003-5812-b68f-f12b001d2a6b</RequestId>
  </ResponseMetadata>
</DeleteMessageBatchResponse>
]]
function _M:deleteMessageBatch(request)
    local success = setmetatable({}, cjson_safe.array_mt)
    local fail = setmetatable({}, cjson_safe.array_mt)
    local i = 1
    while true do
        local id = request['DeleteMessageBatchRequestEntry.' .. i .. '.Id']
        if id == nil then
            break
        end
        self:deleteOneMessage(success, fail, request, i)
        i = i + 1
    end
    return {
        DeleteMessageBatchResponse = {
            DeleteMessageBatchResult = {
                DeleteMessageBatchResultEntry = success,
                BatchResultErrorEntry = fail
            }
        }
    }
end

function _M:deleteOneMessage(success, fail, request, i)
    local topic = request.QueueUrl
    local group = "GID_SQS"
    local id = request['DeleteMessageBatchRequestEntry.' .. i .. '.Id']
    local handle = utils.fromHex(request['DeleteMessageBatchRequestEntry.' .. i .. '.ReceiptHandle'])
    if handle == nil then
        table.insert(fail, {
            Code = "AckFail",
            Id = id,
            Message = "The input receipt handle is invalid.",
            SenderFault = true
        })
        return
    end
    local res, err = self.p.client:doAck(topic, group, handle)
    if not res then
        table.insert(fail, {
            Code = "AckFail",
            Id = id,
            Message = tostring(err),
            SenderFault = false
        })
    else
        table.insert(success, {
            Id = id
        })
    end
end
return _M
