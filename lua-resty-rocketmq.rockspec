package = "lua-resty-rocketmq"
version = "0.3.0-0"
source = {
   url = "git://github.com/yuz10/lua-resty-rocketmq.git",
   tag = "0.3.0-0"
}
description = {
   summary = "Lua RocketMQ client driver for the ngx_lua based on the cosocket API",
   detailed = [[
     This Lua library is a RocketMQ client driver for the ngx_lua nginx module:
     http://wiki.nginx.org/HttpLuaModule
     This Lua library takes advantage of ngx_lua's cosocket API, which ensures 100% nonblocking behavior.
     Note that at least ngx_lua 0.9.3 or ngx_openresty 1.4.3.7 is required, and unfortunately only LuaJIT supported (--with-luajit).
   ]],
   homepage = "https://github.com/yuz10/lua-resty-rocketmq",
   license = "BSD"
}
dependencies = {
   "lua >= 5.1",
   "lua-resty-hmac-ffi >= 0.05-0",
}
build = {
   type = "builtin",
   modules = {
      ["resty.rocketmq.acl_rpchook"] = "lib/resty/rocketmq/acl_rpchook.lua",
      ["resty.rocketmq.admin"] = "lib/resty/rocketmq/admin.lua",
      ["resty.rocketmq.client"] = "lib/resty/rocketmq/client.lua",
      ["resty.rocketmq.core"] = "lib/resty/rocketmq/core.lua",
      ["resty.rocketmq.json"] = "lib/resty/rocketmq/json.lua",
      ["resty.rocketmq.producer"] = "lib/resty/rocketmq/producer.lua",
      ["resty.rocketmq.queue"] = "lib/resty/rocketmq/queue.lua",
      ["resty.rocketmq.trace"] = "lib/resty/rocketmq/trace.lua",
      ["resty.rocketmq.utils"] = "lib/resty/rocketmq/utils.lua",
   }
}
