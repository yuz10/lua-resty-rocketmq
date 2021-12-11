Name
=====

lua-resty-rocketmq - Lua rocketmq client driver for the ngx_lua based on the cosocket API

Table of Contents
=================

* [Name](#name)
* [Status](#status)
* [Description](#description)
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
    * [resty.rocketmq.admin](#restyrocketmqadmin)
      * [Methods](#methods)
          * [new](#new-1)
          * [addRPCHook](#addRPCHook-1)
          * [setUseTLS](#setUseTLS-1)
          * [setTimeout](#setTimeout-1)
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

This library is still under early development and is still experimental.

Description
===========

This Lua library is a RocketMq client driver for the ngx_lua nginx module:

This Lua library takes advantage of ngx_lua's cosocket API, which ensures
100% nonblocking behavior.

Synopsis
========

```lua
    lua_package_path "/path/to/lua-resty-rocketmq/lib/?.lua;;";

    server {
        location /test {
            content_by_lua '
                local cjson = require "cjson"
                local producer = require "resty.rocketmq.producer"

                local nameservers = {
                    "127.0.0.1:9876",
                }

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

            ';
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

[Back to TOC](#table-of-contents)

### Methods

#### new

`syntax: p = producer.new(nameservers, produce_group, enableMsgTrace)`

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


resty.rocketmq.admin
----------------------

To load this module, just do this

```lua
    local admin = require "resty.rocketmq.admin"
```

[Back to TOC](#table-of-contents)

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

`syntax: p:setTimeout(timeout)`

`timeout` is in milliseconds, default 3000

#### createTopic
`syntax: res, err = adm:createTopic(defaultTopic, newTopic, queueNum, topicSysFlag)`

- defaultTopic: usually "TBW102"
- newTopic: the new topic name
- queueNum: read and write queue numbers
- topicSysFlag: system flag of the topic


#### createTopicForBroker
`syntax: res, err = adm:createTopicForBroker(addr, defaultTopic, topicConfig)`

- addr: broker address
- defaultTopic: usually "TBW102"
- topicConfig: a table containing:
  - topic
  - defaultTopic
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
* [lua-resty-kafka](https://github.com/doujiang24/lua-resty-kafka) library

[Back to TOC](#table-of-contents)
