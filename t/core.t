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

=== TEST 1: encode and decode
--- http_config eval: $::HttpConfig
--- config
    location /core {
        content_by_lua_block {
            local core = require "resty.rocketmq.core"
            local encoded = core.encode(1234, {abcde=12345,fg=67}, "body", false)
            ngx.log(ngx.WARN,"encoded:",encoded)
            local h = string.sub(encoded, 5)
            local decoded = core.decodeHeader(h)
            ngx.say("code:",decoded.code)
            ngx.say("version:",decoded.version)
            ngx.say("opaque:",decoded.opaque)
            ngx.say("flag:",decoded.flag)
            ngx.say("remark:",decoded.remark)
            ngx.say("extFields.abcde:",decoded.extFields.abcde)
            ngx.say("extFields.fg:",decoded.extFields.fg)
        }
    }
--- request
GET /core
--- response_body_like
code:1234
version:399
opaque:\d+
flag:0
remark:nil
extFields.abcde:12345
extFields.fg:67
--- no_error_log
[error]


=== TEST 2: getLong
--- http_config eval: $::HttpConfig
--- config
    location /get_long {
        content_by_lua_block {
            local core = require "resty.rocketmq.core"
            local x = string.char(0) .. string.char(0) .. string.char(0) .. string.char(0) .. string.char(0x9e) .. string.char(0x1b) .. string.char(0xbf) .. string.char(0x7b)
            local a = core.getLong(x, 1)
            ngx.say(a)
        }
    }
--- request
GET /get_long
--- response_body
2652618619
--- no_error_log
[error]

