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

=== TEST 1: AllocateMessageQueueAveragely
--- http_config eval: $::HttpConfig
--- config
    location /allocate_mq {
        content_by_lua_block {
            local consumer = require "resty.rocketmq.consumer"
            local cid = { 1, 2, 3, 4 }
            for _, c in ipairs(cid) do
                local res = consumer.AllocateMessageQueueAveragely("", c, { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, cid)
                ngx.say(table.concat(res, ","))
            end
        }
    }
--- request
GET /allocate_mq
--- response_body
1,2,3
4,5,6
7,8
9,10
--- no_error_log
[error]
