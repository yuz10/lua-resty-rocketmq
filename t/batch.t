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

=== TEST 1: create topic
--- http_config eval: $::HttpConfig
--- config
    location /create_topic {
        content_by_lua_block {
            local admin = require "resty.rocketmq.admin"
            local adm, err = admin.new({ "127.0.0.1:9876" })
            if not adm then
                ngx.say("new admin client err:", err)
                return
            end

            local res, err = adm:createTopic("TopicTest", 1)
            if not res then
                ngx.say("create topic err:", err)
                return
            end

            ngx.say("ok")
        }
    }
--- request
GET /create_topic
--- response_body
ok
--- no_error_log
[error]


=== TEST 2: producer
--- http_config eval: $::HttpConfig
--- config
    location /producer {
        content_by_lua_block {
            local producer = require "resty.rocketmq.producer"

            local nameservers = { "127.0.0.1:9876" }

            local message = "halo world"
            local p, err = producer.new(nameservers, "produce_group")
            if not p then
                ngx.say("create producer err:", err)
                return
            end

            local msgs = {}
            table.insert(msgs, {topic = "TopicTest", body = "batch1", tags = "batch_tag1", keys = "batch_key1"})
            table.insert(msgs, {topic = "TopicTest", body = "batch2", tags = "batch_tag2", keys = "batch_key2"})
            local res, err = p:batchSend(msgs)
            if not res then
                ngx.say("send err:", err)
                return
            end

            ngx.say("send success")
            ngx.say("broker name: " .. res.sendResult.messageQueue.brokerName)
            ngx.say("queue id: " .. res.sendResult.messageQueue.queueId)
            ngx.say("queue offset: " .. res.sendResult.queueOffset)
            ngx.say("offsetMsgId: " .. res.sendResult.offsetMsgId)

            -----------------viewMessage
            local admin = require "resty.rocketmq.admin"
            local adm, err = admin.new(nameservers)
            if not adm then
                ngx.say("new admin client err:", err)
                return
            end
            local split = require("resty.rocketmq.utils").split
            for i, offsetMsgId in ipairs(split(res.sendResult.offsetMsgId, ',')) do
                local msg, err = adm:viewMessage(offsetMsgId)
                if not msg then
                    ngx.say("viewMessage err:", err)
                    return
                end
                assert(msg.queueId == res.sendResult.messageQueue.queueId)
                assert(msg.topic == "TopicTest")
                assert(msg.offsetMsgId == offsetMsgId)
                assert(msg.body == "batch"..i)
                assert(msg.properties.TAGS == "batch_tag"..i)
                assert(msg.properties.KEYS == "batch_key"..i)
                ngx.say("viewMessage: "..require("cjson").encode(msg))
            end
        }
    }
--- request
GET /producer
--- response_body_like
send success
broker name: .+
queue id: \d+
queue offset: \d+
offsetMsgId: \w+,\w+
viewMessage: .+
viewMessage: .+
--- no_error_log
[error]

