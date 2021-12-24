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

no_shuffle();
run_tests();

__DATA__

=== TEST 1: queryMessage
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
            adm:createTopic("TBW102", "AaTopic", 1)
            adm:createTopic("TBW102", "BBTopic", 1)

            local p, err = producer.new(nameservers, "produce_group")
            if not p then
                ngx.say("create producer err:", err)
                return
            end
            local res, err = p:send("AaTopic", "halo world", "tags", "Aa")
            local res, err = p:send("BBTopic", "halo world", "tags", "BB")
            ngx.log(ngx.WARN, require("cjson").encode(res.sendResult))

            -----------------queryMessage
            local time = ngx.now() * 1000
            local msgs, err = adm:queryMessage("AaTopic", "BB", 10, time - 10000, time + 50000, false)
            if not msgs then
                ngx.say("query msgs err:", err)
                return
            end
            ngx.say(#msgs)

            local msgs, err = adm:queryMessage("AaTopic", "Aa", 10, time - 10000, time + 50000, false)
            if not msgs then
                ngx.say("query msgs err:", err)
                return
            end
            ngx.say(#msgs)
        }
    }
--- request
GET /trace
--- response_body_like
0
1
--- no_error_log
[error]


