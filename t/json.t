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

=== TEST 1: json decode
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local decode = require "resty.rocketmq.json".decode
            local encode = require "cjson".encode
            local tests = {
                [[{}]],
                [[+1.2e-3]],
                [["stri\"ng"]],
                [[{"a":[1,2,3]}]],
                [["string"xxx]],
                [[true]],
                [[false]],
                [[null]],
                [[{0:0}]],
            }
            for _, t in ipairs(tests) do
                local res, err = decode(t)
                if res == nil then
                    ngx.say("err:", err)
                else
                    ngx.say("ok:", encode(res))
                end
            end
        }
    }
--- request
GET /t
--- response_body
ok:{}
ok:0.0012
ok:"stri\"ng"
ok:{"a":[1,2,3]}
err:invalid token x in 9
ok:true
ok:false
ok:null
ok:{"0":0}
--- no_error_log
[error]


