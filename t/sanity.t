# vim:set ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty/lualib/?.so;/usr/local/openresty/lualib/?.so;;";

    init_by_lua_block{
        local producer = require "resty.rocketmq.producer"
        local admin = require "resty.rocketmq.admin"
    }
    init_worker_by_lua_block{
        local producer = require "resty.rocketmq.producer"
        local admin = require "resty.rocketmq.admin"
    }
};

no_long_string();
#no_diff();

no_shuffle();
run_tests();

__DATA__

=== TEST 1: load in different phase
--- http_config eval: $::HttpConfig
--- config
    location /load {
        content_by_lua_block {
            local producer = require "resty.rocketmq.producer"
            local admin = require "resty.rocketmq.admin"
            ngx.say("ok")
        }
    }
--- request
GET /load
--- response_body
ok
--- no_error_log
[error]


