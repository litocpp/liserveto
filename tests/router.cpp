#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;

auto router_request(ref<str> target, ref<str> method = "GET"_str) -> Request {
    auto line = rstd::format("{} {} HTTP/1.1\r\nHost: local\r\n\r\n", method, target);
    lihttpto::RequestHeadDecoder decoder;
    decoder.feed(line.as_str().as_bytes()).unwrap();
    return Request(
        decoder.take().unwrap(), IncomingBody::from_body(Body {}), Shutdown::make().unwrap());
}
auto router_text(ref<str> value) -> Endpoint {
    return Endpoint::make(
        [value = String::make(value)](Request) -> rstd::async::coro<ResponseResult> {
            co_return Ok(
                Response::bytes(rstd::bytes::Bytes::copy_from_slice(value.as_str().as_bytes())));
        });
}
TEST(Router, RegistrationErrorsAndMethodDispatch) {
    Router router;
    router.get("/users/{id}"_str, router_text("id"_str)).unwrap();
    EXPECT_EQ(router.get("/users/{id}"_str, router_text("again"_str)).unwrap_err(),
              RouteError::Duplicate);
    EXPECT_EQ(router.post("/users/{name}"_str, router_text("name"_str)).unwrap_err(),
              RouteError::ParameterConflict);
    EXPECT_EQ(router.get("/a/{*rest}/b"_str, router_text("bad"_str)).unwrap_err(),
              RouteError::InvalidPattern);
    EXPECT_EQ(router.get("/a/{id}/{id}"_str, router_text("bad"_str)).unwrap_err(),
              RouteError::ParameterConflict);
    router.get("/users/new"_str, router_text("new"_str)).unwrap();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto response = runtime.block_on(router.call(router_request("/users/new"_str))).unwrap();
    EXPECT_EQ(runtime.block_on(response.body.collect(u64(20))).unwrap().data.as_slice(),
              "new"_str.as_bytes());
    auto wrong =
        runtime.block_on(router.call(router_request("/users/new"_str, "POST"_str))).unwrap();
    EXPECT_EQ(wrong.head.status.value(), u16(405));
    EXPECT_EQ(*wrong.head.headers.get_unique_text("Allow"_str).unwrap(), "GET, HEAD, OPTIONS"_str);
    auto head =
        runtime.block_on(router.call(router_request("/users/new"_str, "HEAD"_str))).unwrap();
    EXPECT_EQ(head.head.status.value(), u16(200));
    auto options =
        runtime.block_on(router.call(router_request("/users/new"_str, "OPTIONS"_str))).unwrap();
    EXPECT_EQ(options.head.status.value(), u16(204));
    auto missing = runtime.block_on(router.call(router_request("/missing"_str))).unwrap();
    EXPECT_EQ(missing.head.status.value(), u16(404));
}
TEST(Router, DecodedParametersNestedMountAndOriginalTarget) {
    Router child;
    child
        .get("/{id}"_str, Endpoint::make([](Request request) -> rstd::async::coro<ResponseResult> {
                 auto value = request.parameter("id"_str).unwrap();
                 if (value != "a/b"_str.as_bytes() || request.target() != "/api/a%2Fb?q=yes"_str ||
                     request.path_size() != usize(1))
                     co_return Err(Error { ErrorKind::Internal });
                 co_return Ok(Response {});
             }))
        .unwrap();
    Router parent;
    parent.nest("/api"_str, rstd::move(child)).unwrap();
    parent.fallback(router_text("fallback"_str));
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(parent.call(router_request("/api/a%2Fb?q=yes"_str))).is_ok());
    auto boundary = runtime.block_on(parent.call(router_request("/apix/item"_str))).unwrap();
    EXPECT_EQ(runtime.block_on(boundary.body.collect(u64(20))).unwrap().data.as_slice(),
              "fallback"_str.as_bytes());
    auto nested_missing = runtime.block_on(parent.call(router_request("/api/a/b"_str))).unwrap();
    EXPECT_EQ(nested_missing.head.status.value(), u16(404));
}

TEST(Router, MiddlewareOrderShortCircuitAndNextIsSingleUse) {
    auto     runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    Vec<int> trace;
    auto     endpoint =
        middleware(router_text("ok"_str),
                   [&](Request request, Next next) -> rstd::async::coro<ResponseResult> {
                       trace.push(1);
                       auto response = co_await next.call(rstd::move(request));
                       trace.push(2);
                       EXPECT_TRUE((co_await next.call(router_request("/"_str))).is_err());
                       co_return response;
                   });
    endpoint = middleware(rstd::move(endpoint),
                          [&](Request request, Next next) -> rstd::async::coro<ResponseResult> {
                              trace.push(0);
                              auto result = co_await next.call(rstd::move(request));
                              trace.push(3);
                              co_return result;
                          });
    EXPECT_TRUE(runtime.block_on(endpoint.call(router_request("/"_str))).is_ok());
    EXPECT_EQ(trace.as_slice(), (array<int, 4> { 0, 1, 2, 3 }.as_slice()));
    auto shorted =
        middleware(rstd::move(endpoint), [](Request, Next) -> rstd::async::coro<ResponseResult> {
            Response response;
            response.head.status = lihttpto::StatusCode::make(u16(401)).unwrap();
            co_return Ok(rstd::move(response));
        });
    EXPECT_EQ(runtime.block_on(shorted.call(router_request("/"_str))).unwrap().head.status.value(),
              u16(401));
    EXPECT_EQ(trace.len(), usize(4));
}
TEST(Router, EndpointCallOwnsCapturedStateBeforeFirstPoll) {
    Option<rstd::async::coro<ResponseResult>> pending;
    {
        auto endpoint = router_text("owned"_str);
        pending.insert(endpoint.call(router_request("/"_str)));
    }
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto response = runtime.block_on(pending.take().unwrap()).unwrap();
    EXPECT_EQ(runtime.block_on(response.body.collect(u64(10))).unwrap().data.as_slice(),
              "owned"_str.as_bytes());
    auto request = router_request("/"_str);
    EXPECT_TRUE(request.take_body().is_ok());
    EXPECT_EQ(request.take_body().unwrap_err(), BodyError::Protocol);
}

TEST(Router, ExplicitHeadOptionsAndCatchAllRootMount) {
    Router child;
    child.get("/"_str, router_text("index"_str)).unwrap();
    child
        .get("/{*path}"_str,
             Endpoint::make([](Request request) -> rstd::async::coro<ResponseResult> {
                 co_return Ok(Response::bytes(
                     rstd::bytes::Bytes::copy_from_slice(request.parameter("path"_str).unwrap())));
             }))
        .unwrap();
    child.route("HEAD"_str, "/"_str, router_text("explicit-head"_str)).unwrap();
    child.route("OPTIONS"_str, "/"_str, router_text("explicit-options"_str)).unwrap();
    Router parent;
    parent.nest("/assets"_str, rstd::move(child)).unwrap();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (auto target : array<ref<str>, 2> { "/assets"_str, "/assets/"_str }) {
        auto index = runtime.block_on(parent.call(router_request(target))).unwrap();
        EXPECT_EQ(runtime.block_on(index.body.collect(u64(32))).unwrap().data.as_slice(),
                  "index"_str.as_bytes());
    }
    for (auto method : array<ref<str>, 2> { "HEAD"_str, "OPTIONS"_str }) {
        auto result = runtime.block_on(parent.call(router_request("/assets"_str, method))).unwrap();
        auto expected = method == "HEAD"_str ? "explicit-head"_str : "explicit-options"_str;
        EXPECT_EQ(runtime.block_on(result.body.collect(u64(32))).unwrap().data.as_slice(),
                  expected.as_bytes());
    }
    auto rest = runtime.block_on(parent.call(router_request("/assets/a/b"_str))).unwrap();
    EXPECT_EQ(runtime.block_on(rest.body.collect(u64(32))).unwrap().data.as_slice(),
              "a/b"_str.as_bytes());
}

struct ReadyEndpointResponse {
    using Output = ResponseResult;
    auto poll(mut_ref<ReadyEndpointResponse>, rstd::task::Context&) -> rstd::task::Poll<Output> {
        return rstd::task::Poll<Output>::Ready(Output(Ok(Response {})));
    }
};
TEST(Router, EndpointAcceptsPollableFutureWithoutCoroWrapperAtCallSite) {
    auto endpoint = Endpoint::make([](Request) {
        return ReadyEndpointResponse {};
    });
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(endpoint.call(router_request("/"_str))).is_ok());
}
TEST(Router, AsteriskTargetDoesNotAliasRootPath) {
    Router router;
    router.get("/"_str, router_text("root"_str)).unwrap();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto response = runtime.block_on(router.call(router_request("*"_str, "OPTIONS"_str))).unwrap();
    EXPECT_EQ(response.head.status.value(), u16(404));
    router.fallback(router_text("asterisk"_str));
    auto handled = runtime.block_on(router.call(router_request("*"_str, "OPTIONS"_str))).unwrap();
    EXPECT_EQ(runtime.block_on(handled.body.collect(u64(32))).unwrap().data.as_slice(),
              "asterisk"_str.as_bytes());
}
