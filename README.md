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
            * [produce](#produce)
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
                local res, err = p:produce("TopicTest", message)
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

`syntax: p = producer.new(nameservers, produce_group)`

`nameservers` is list of nameserver addresses

#### addRPCHook

`syntax: p:addRPCHook(hook)`

`hook` is a table that contains two functions as follows:

 - `doBeforeRequest(addr, header, body)`

 - `doAfterResponse(addr, header, body, respHeader, respBody)`

there is an acl hook provided, usage is:
```lua
    local accessKey, secretKey = "RocketMQ", "12345678"
    local aclHook = require("resty.rocketmq.acl_rpchook").new(accessKey, secretKey)
    p:addRPCHook(aclHook)
```

#### produce
`syntax: res, err = p:produce(topic, message, tags, keys, waitStoreMsgOk)`

  In case of success, returns the a table of results.
  In case of errors, returns `nil` with a string describing the error.


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
