Name
=====

lua-resty-rocketmq - Lua rocketmq client driver for the ngx_lua based on the cosocket API

[![Test](https://github.com/yuz10/lua-resty-rocketmq/actions/workflows/CI.yml/badge.svg)](https://github.com/yuz10/lua-resty-rocketmq/actions)

Table of Contents
=================

* [Name](#name)
* [Status](#status)
* [Description](#description)
* [Quick start](#quick-start)
* [Synopsis](#synopsis)
* [Modules](#modules)
    * [resty.rocketmq.producer](#restyrocketmqproducer)
        * [Methods](#methods)
            * [new](#new)
            * [addRPCHook](#addRPCHook)
            * [setUseTLS](#setUseTLS)
            * [setTimeout](#setTimeout)
            * [registerSendMessageHook](#registerSendMessageHook)
            * [registerEndTransactionHook](#registerEndTransactionHook)
            * [send](#send)
            * [sendMessageInTransaction](#sendMessageInTransaction)
            * [batchSend](#batchSend)
            * [start](#start)
            * [stop](#stop)
    * [resty.rocketmq.consumer](#restyrocketmqconsumer)
        * [Methods](#methods-1)
            * [new](#new-1)
            * [addRPCHook](#addRPCHook-1)
            * [setUseTLS](#setUseTLS-1)
            * [setTimeout](#setTimeout-1)
            * [registerMessageListener](#registerMessageListener)
            * [registerConsumeMessageHook](#registerConsumeMessageHook)
            * [subscribe](#subscribe)
            * [start](#start-1)
            * [stop](#stop-1)
            * [setAllocateMessageQueueStrategy](#setAllocateMessageQueueStrategy)
            * [getAllocateMessageQueueStrategy](#getAllocateMessageQueueStrategy)
            * [setEnableMsgTrace](#setEnableMsgTrace)
            * [getEnableMsgTrace](#getEnableMsgTrace)
            * [setCustomizedTraceTopic](#setCustomizedTraceTopic)
            * [getCustomizedTraceTopic](#getCustomizedTraceTopic)
            * [setConsumeFromWhere](#setConsumeFromWhere)
            * [getConsumeFromWhere](#getConsumeFromWhere)
            * [setConsumeTimestamp](#setConsumeTimestamp)
            * [getConsumeTimestamp](#getConsumeTimestamp)
            * [setPullThresholdForQueue](#setPullThresholdForQueue)
            * [getPullThresholdForQueue](#getPullThresholdForQueue)
            * [setPullThresholdSizeForQueue](#setPullThresholdSizeForQueue)
            * [getPullThresholdSizeForQueue](#getPullThresholdSizeForQueue)
            * [setPullTimeDelayMillsWhenException](#setPullTimeDelayMillsWhenException)
            * [getPullTimeDelayMillsWhenException](#getPullTimeDelayMillsWhenException)
            * [setPullBatchSize](#setPullBatchSize)
            * [getPullBatchSize](#getPullBatchSize)
            * [setPullInterval](#setPullInterval)
            * [getPullInterval](#getPullInterval)
            * [setConsumeMessageBatchMaxSize](#setConsumeMessageBatchMaxSize)
            * [getConsumeMessageBatchMaxSize](#getConsumeMessageBatchMaxSize)
            * [setMaxReconsumeTimes](#setMaxReconsumeTimes)
            * [getMaxReconsumeTimes](#getMaxReconsumeTimes)
    * [resty.rocketmq.admin](#restyrocketmqadmin)
      * [Methods](#methods-2)
          * [new](#new-2)
          * [addRPCHook](#addRPCHook-2)
          * [setUseTLS](#setUseTLS-2)
          * [setTimeout](#setTimeout-2)
          * [createTopic](#createTopic)
          * [createTopicForBroker](#createTopicForBroker)
          * [searchOffset](#searchOffset)
          * [maxOffset](#maxOffset)
          * [minOffset](#minOffset)
          * [earliestMsgStoreTime](#earliestMsgStoreTime)
          * [viewMessage](#viewMessage)
          * [queryMessage](#queryMessage)
          * [queryTraceByMsgId](#queryTraceByMsgId)
* [Installation](#installation)
* [See Also](#see-also)

Status
======

Production ready.

Description
===========

This Lua library is a RocketMq client driver for the ngx_lua nginx module:

This Lua library takes advantage of ngx_lua's cosocket API, which ensures
100% nonblocking behavior.

Quick start
===========

### Install OpenResty
for ubuntu:
```shell
wget -O - https://openresty.org/package/pubkey.gpg | sudo apt-key add -
echo "deb http://openresty.org/package/ubuntu $(lsb_release -sc) main" \
    | sudo tee /etc/apt/sources.list.d/openresty.list
sudo apt-get update
sudo apt-get -y install openresty
```
see https://openresty.org/cn/linux-packages.html for more distributions

### Install and start RocketMQ
```shell
wget https://archive.apache.org/dist/rocketmq/4.9.3/rocketmq-all-4.9.3-bin-release.zip
unzip rocketmq-all-4.9.3-bin-release.zip
cd rocketmq-4.9.3
nohup bash bin/mqnamesrv &
nohup bash bin/mqbroker -n localhost:9876 -c conf/broker.conf &
```

### Run examples of this project
```shell
cd examples
chmod +x producer.lua
./producer.lua
```

Synopsis
========

```lua
    lua_package_path "/path/to/lua-resty-rocketmq/lib/?.lua;;";

    server {
        location /test {
            content_by_lua_block {
                local cjson = require "cjson"
                local producer = require "resty.rocketmq.producer"
                local consumer = require "resty.rocketmq.consumer"

                local nameservers = { "127.0.0.1:9876" }

                local message = "halo world"

                local p = producer.new(nameservers, "produce_group")
                
                -- set acl
                local aclHook = require("resty.rocketmq.acl_rpchook").new("RocketMQ","123456781")
                p:addRPCHook(aclHook)
                
                -- use tls mode
                p:setUseTLS(true)
                
                local res, err = p:send("TopicTest", message)
                if not res then
                    ngx.say("send err:", err)
                    return
                end
                ngx.say("send success")
                
                -- consume
                local c = consumer.new(nameservers, "group1")
                c:subscribe("TopicTest", "*")
                c:registerMessageListener({
                    consumeMessage = function(self, msgs, context)
                        ngx.say("consume success:", cjson.encode(msgs))
                        return consumer.CONSUME_SUCCESS
                    end
                })
                
                c:start()
                ngx.sleep(5)
                c:stop()
            }
        }
    }
```


[Back to TOC](#table-of-contents)

Modules
=======


resty.rocketmq.producer
----------------------

To load this module, just do this

```lua
    local producer = require "resty.rocketmq.producer"
```

### Methods

#### new

`syntax: p = producer.new(nameservers, produce_group, enableMsgTrace, customizedTraceTopic)`

`nameservers` is list of nameserver addresses

#### addRPCHook

`syntax: p:addRPCHook(hook)`

`hook` is a table that contains two functions as follows:

 - `doBeforeRequest(self, addr, header, body)`
 - `doAfterResponse(self, addr, header, body, respHeader, respBody)`

there is an acl hook provided, usage is:
```lua
    local accessKey, secretKey = "RocketMQ", "12345678"
    local aclHook = require("resty.rocketmq.acl_rpchook").new(accessKey, secretKey)
    p:addRPCHook(aclHook)
```

#### setUseTLS

`syntax: p:setUseTLS(useTLS)`

`useTLS` is a boolean

#### setTimeout

`syntax: p:setTimeout(timeout)`

`timeout` is in milliseconds, default 3000

#### registerSendMessageHook

`syntax: p:registerSendMessageHook(hook)`

`hook` is a table that contains two functions as follows:
  - `sendMessageBefore(self, context)`
  - `sendMessageAfter(self, context)`

`context` is a table that contains:
  - producer
  - producerGroup
  - communicationMode
  - bornHost
  - brokerAddr
  - message
  - mq
  - msgType
  - sendResult
  - exception


#### registerEndTransactionHook

`syntax: p:registerEndTransactionHook(hook)`

`hook` is a table that contains a function as follows:

  - `endTransaction(self, context)`
 
`context` is a table that contains:
  - producerGroup
  - brokerAddr
  - message
  - msgId
  - transactionId
  - transactionState
  - fromTransactionCheck


#### send
`syntax: res, err = p:send(topic, message, tags, keys, properties)`

`properties` is a table that contains:
   - waitStoreMsgOk
   - delayTimeLevel

  In case of success, returns the a table of results.
  In case of errors, returns `nil` with a string describing the error.

#### setTransactionListener
`syntax: res, err = p:setTransactionListener(transactionListener)`

`transactionListener` is a table that contains two functions as follows:

- `executeLocalTransaction(self, msg, arg)`
- `checkLocalTransaction(self, msg)`

#### sendMessageInTransaction
`syntax: res, err = p:sendMessageInTransaction(topic, arg, message, tags, keys, properties)`

#### batchSend
`syntax: res, err = p:batchSend(msgs)`

`msgs` is a list of msgs, each msg is a table that contains:

- topic
- body
- tags
- keys
- properties

#### start
`syntax: p:start()`

note that if you don't call p:start() before sending messages, messages will be sent successfully, but the trace is not send.

#### stop
`syntax: p:stop()`


[Back to TOC](#table-of-contents)


resty.rocketmq.consumer
----------------------

To load this module, just do this

```lua
    local consumer = require "resty.rocketmq.consumer"
```

### Methods

#### new

`syntax: c = consumer.new(nameservers, consumerGroup)`

`nameservers` is list of nameserver addresses

#### addRPCHook

`syntax: c:addRPCHook(hook)`

`hook` is a table that contains two functions as follows:

- `doBeforeRequest(self, addr, header, body)`
- `doAfterResponse(self, addr, header, body, respHeader, respBody)`

there is an acl hook provided, usage is:
```lua
    local accessKey, secretKey = "RocketMQ", "12345678"
    local aclHook = require("resty.rocketmq.acl_rpchook").new(accessKey, secretKey)
    c:addRPCHook(aclHook)
```

#### setUseTLS

`syntax: c:setUseTLS(useTLS)`

`useTLS` is a boolean

#### setTimeout

`syntax: c:setTimeout(timeout)`

`timeout` is in milliseconds, default 3000

#### registerMessageListener

`syntax: c:registerMessageListener(messageListener)`

`messageListener` is a table that contains a function as follows:
- `consumeMessage(self, msgs, context)`

#### registerConsumeMessageHook

`syntax: c:registerConsumeMessageHook(hook)`

`hook` is a table that contains two functions as follows:
- `consumeMessageBefore(self, context)`
- `consumeMessageAfter(self, context)`

`context` is a table that contains:
- consumerGroup
- mq
- msgList
- success
- status
- consumeContextType

#### subscribe

`syntax: c:subscribe(topic, subExpression)`

#### start
`syntax: c:start()`

#### stop
`syntax: c:stop()`

#### setAllocateMessageQueueStrategy
`syntax: c:setAllocateMessageQueueStrategy(strategy)`

`strategy` is a functions as follows:
- `function(consumerGroup, currentCID, mqAll, cidAll)`

default value is `consumer.AllocateMessageQueueAveragely`

#### getAllocateMessageQueueStrategy
`syntax: local strategy = c:getAllocateMessageQueueStrategy()`

#### setEnableMsgTrace
`syntax: c:setEnableMsgTrace(enableMsgTrace)`

#### getEnableMsgTrace
`syntax: local enableMsgTrace = c:getEnableMsgTrace()`

#### setCustomizedTraceTopic
`syntax: c:setCustomizedTraceTopic(customizedTraceTopic)`

#### getCustomizedTraceTopic
`syntax: local customizedTraceTopic = c:getCustomizedTraceTopic()`

#### setConsumeFromWhere
`syntax: c:setConsumeFromWhere(consumeFromWhere)`

#### getConsumeFromWhere
`syntax: local consumeFromWhere = c:getConsumeFromWhere()`

#### setConsumeTimestamp
`syntax: c:setConsumeTimestamp(consumeTimestamp)`

#### getConsumeTimestamp
`syntax: local consumeTimestamp = c:getConsumeTimestamp()`

#### setPullThresholdForQueue
`syntax: c:setPullThresholdForQueue(pullThresholdForQueue)`

#### getPullThresholdForQueue
`syntax: local pullThresholdForQueue = c:getPullThresholdForQueue()`

#### setPullThresholdSizeForQueue
`syntax: c:setPullThresholdSizeForQueue(pullThresholdSizeForQueue)`

#### getPullThresholdSizeForQueue
`syntax: local pullThresholdSizeForQueue = c:getPullThresholdSizeForQueue()`

#### setPullTimeDelayMillsWhenException
`syntax: c:setPullTimeDelayMillsWhenException(pullTimeDelayMillsWhenException)`

#### getPullTimeDelayMillsWhenException
`syntax: local pullTimeDelayMillsWhenException = c:getPullTimeDelayMillsWhenException()`

#### setPullBatchSize
`syntax: c:setPullBatchSize(pullBatchSize)`

#### getPullBatchSize
`syntax: local pullBatchSize = c:getPullBatchSize()`

#### setPullInterval
`syntax: c:setPullInterval(pullInterval)`

#### getPullInterval
`syntax: local pullInterval = c:getPullInterval()`

#### setConsumeMessageBatchMaxSize
`syntax: c:setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize)`

#### getConsumeMessageBatchMaxSize
`syntax: local consumeMessageBatchMaxSize = c:getConsumeMessageBatchMaxSize()`

#### setMaxReconsumeTimes
`syntax: c:setMaxReconsumeTimes(maxReconsumeTimes)`

#### getMaxReconsumeTimes
`syntax: local maxReconsumeTimes = c:getMaxReconsumeTimes()`

[Back to TOC](#table-of-contents)

resty.rocketmq.admin
----------------------

To load this module, just do this

```lua
    local admin = require "resty.rocketmq.admin"
```

### Methods

#### new

`syntax: adm = admin.new(nameservers)`

`nameservers` is list of nameserver addresses

#### addRPCHook

`syntax: adm:addRPCHook(hook)`

`hook` is a table that contains two functions as follows:

- `doBeforeRequest(self, addr, header, body)`

- `doAfterResponse(self, addr, header, body, respHeader, respBody)`

there is an acl hook provided, usage is:
```lua
    local accessKey, secretKey = "RocketMQ", "12345678"
    local aclHook = require("resty.rocketmq.acl_rpchook").new(accessKey, secretKey)
    adm:addRPCHook(aclHook)
```

#### setUseTLS

`syntax: adm:setUseTLS(useTLS)`

`useTLS` is a boolean

#### setTimeout

`syntax: adm:setTimeout(timeout)`

`timeout` is in milliseconds, default 3000

#### createTopic
`syntax: res, err = adm:createTopic(newTopic, queueNum, topicSysFlag)`

- newTopic: the new topic name
- queueNum: read and write queue numbers
- topicSysFlag: system flag of the topic


#### createTopicForBroker
`syntax: res, err = adm:createTopicForBroker(addr, topicConfig)`

- addr: broker address
- topicConfig: a table containing:
  - topic
  - readQueueNums
  - writeQueueNums
  - perm
  - topicFilterType
  - topicSysFlag
  - order

#### searchOffset
`syntax: res, err = adm:searchOffset(mq, timestamp)`

- mq: a table containing:
   - brokerName
   - topic
   - queueId
- timestamp: search time

#### maxOffset
`syntax: res, err = adm:maxOffset(mq)`

- mq: a table containing:
   - brokerName
   - topic
   - queueId

#### minOffset
`syntax: res, err = adm:minOffset(mq)`

- mq: a table containing:
   - brokerName
   - topic
   - queueId

#### earliestMsgStoreTime
`syntax: res, err = adm:earliestMsgStoreTime(mq)`

- mq: a table containing:
   - brokerName
   - topic
   - queueId

#### viewMessage
`syntax: res, err = adm:viewMessage(offsetMsgId)`

#### queryMessage
`syntax: res, err = adm:queryMessage(topic, key, maxNum, beginTime, endTime, isUniqKey)`

#### queryTraceByMsgId
`syntax: res, err = adm:queryTraceByMsgId(traceTopic, msgId)`

[Back to TOC](#table-of-contents)

Installation
============

You need to configure
the lua_package_path directive to add the path of your lua-resty-rocketmq source
tree to ngx_lua's LUA_PATH search path, as in

```nginx
    # nginx.conf
    http {
        lua_package_path "/path/to/lua-resty-rocketmq/lib/?.lua;;";
        ...
    }
```

Ensure that the system account running your Nginx ''worker'' proceses have
enough permission to read the `.lua` file.


[Back to TOC](#table-of-contents)


See Also
========
* ngx_lua module: https://github.com/openresty/lua-nginx-module
* apache rocketmq: https://github.com/apache/rocketmq
* lua-resty-kafka: https://github.com/doujiang24/lua-resty-kafka
* luatz: https://github.com/daurnimator/luatz

[Back to TOC](#table-of-contents)
