# vim:set ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    init_by_lua_block{
        require 'resty.core'
    }
};

no_long_string();
#no_diff();

no_shuffle();
run_tests();

__DATA__

=== TEST 1: trace
--- http_config eval: $::HttpConfig
--- config
    location /trace {
        content_by_lua_block {
            local producer = require "resty.rocketmq.producer"
            local admin = require "resty.rocketmq.admin"

            local nameservers = { "127.0.0.1:9876" }
            local adm, err = admin.new(nameservers)
            if not adm then
                ngx.say("new admin client err:", err)
                return
            end
            adm:createTopic("TBW102", "TopicTest", 1)

            local p, err = producer.new(nameservers, "produce_group", true)
            if not p then
                ngx.say("create producer err:", err)
                return
            end
            p:start()
            local res, err = p:send("TopicTest", "halo world", "tags", "keys")
            if not res then
                ngx.say("send err:", err)
                return
            end
            p:stop()
            ngx.say("send success")
            ngx.log(ngx.WARN, require("cjson").encode(res.sendResult))

            -----------------viewMessage
            local trace, err = adm:queryTraceByMsgId(null, res.sendResult.msgId)
            if not trace then
                ngx.say("query trace err:", err)
                return
            end

            if #trace ~= 1 then
                ngx.say("trace len ".. #trace)
                return
            end

            for _, k in ipairs{"traceType","timeStamp","regionId","groupName","topic","msgId",
                "tags","keys","storeHost","bodyLength","costTime","msgType","offsetMsgId","success",} do
                ngx.say(k, ":", trace[1][k])
            end
        }
    }
--- request
GET /trace
--- response_body_like
send success
traceType:Pub
timeStamp:\d+
regionId:DefaultRegion
groupName:produce_group
topic:TopicTest
msgId:\w+
tags:tags
keys:keys
storeHost:.+
bodyLength:10
costTime:\d+
msgType:Normal_Msg
offsetMsgId:\w+
success:true
--- no_error_log
[error]


