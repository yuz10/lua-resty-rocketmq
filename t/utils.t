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

=== TEST 1: uniq_key
--- http_config eval: $::HttpConfig
--- config
    location /uniq_key {
        content_by_lua_block {
            local utils = require "resty.rocketmq.utils"
            ngx.say(utils.genUniqId())
        }
    }
--- request
GET /uniq_key
--- response_body_like
[0-9A-F]{32}
--- no_error_log
[error]


