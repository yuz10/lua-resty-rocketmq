# vim:set ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: producer
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local producer = require "resty.rocketmq.producer"

            local nameservers = { "127.0.0.1:9876" }

            local message = "halo world"
            local p, err = producer.new(nameservers, "produce_group")
            if not p then
                ngx.say("create producer err:", err)
                return
            end
            
            local res, err = p:produce("TopicTest", message)
            if not res then
                ngx.say("send err:", err)
                return
            end
            
            ngx.say("send success")
            ngx.say("broker name: " .. res.sendResult.messageQueue.brokerName)
            ngx.say("queue id: " .. res.sendResult.messageQueue.queueId)
            ngx.say("queue offset: " .. res.sendResult.queueOffset)
        }
    }
--- request
GET /t
--- response_body_like chomp
send success
broker name: \w+
queue id: \d+
queue offset: \d+
--- no_error_log
[error]
