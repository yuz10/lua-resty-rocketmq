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

=== TEST 1: rpc hook
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local new_hook = require "resty.rocketmq.acl_rpchook".new
            local hook = new_hook("RocketMQ", "12345678", "87654321")
            local h = {topic="topicA",batch=false,unitMode=false}
            hook.doBeforeRequest("", h, nil)
            ngx.say(h.Signature)
        }
    }
--- request
GET /t
--- response_body
FQ2fq7p3PuzMwBD7QkNW7AU32/c=
--- no_error_log
[error]
