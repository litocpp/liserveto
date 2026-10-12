#include <rstd/test/gtest.hpp>
import liserveto;

using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::task::Context;
using rstd::task::Poll;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;

struct MemoryWire {
    Bytes    input;
    BytesMut output;
    bool     fail_write {};
    bool     pending_read {};
    usize    partial_write {};
    int      writes_left { -1 };
};
struct MemoryIO {
    rstd::sync::Arc<MemoryWire> state;
    auto                        poll_read(mut_ref<MemoryIO> self, Context& cx, BytesMut& output)
        -> Poll<rstd::io::Result<usize>> {
        if (self->state->pending_read) {
            cx.waker().wake_by_ref();
            return Poll<rstd::io::Result<usize>>::Pending();
        }
        auto data = rstd::exchange(self->state->input, Bytes {});
        output.put_slice(data.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(data.len()));
    }
    auto poll_write(mut_ref<MemoryIO> self, Context&, const Bytes& bytes)
        -> Poll<rstd::io::Result<usize>> {
        if (self->state->fail_write || self->state->writes_left == 0)
            return Poll<rstd::io::Result<usize>>::Ready(Err(rstd::io::error::Error::from_kind(
                rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::BrokenPipe })));
        if (self->state->writes_left > 0) --self->state->writes_left;
        auto count = self->state->partial_write == usize()
                         ? bytes.len()
                         : rstd::cmp::min(bytes.len(), self->state->partial_write);
        self->state->output.put_slice(
            slice<u8>::from_raw_parts(bytes.as_slice().as_raw_ptr(), count));
        return Poll<rstd::io::Result<usize>>::Ready(Ok(count));
    }
    auto poll_flush(mut_ref<MemoryIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
    auto poll_shutdown(mut_ref<MemoryIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
};
auto memory_exchange(rstd::sync::Arc<MemoryWire> wire) -> rstd::async::coro<bool> {
    liserveto::BasicConnection<MemoryIO> connection(MemoryIO { wire.clone() });
    auto                                 first = co_await connection.read_request();
    if (first.is_err() || first->is_none()) co_return false;
    auto data = co_await connection.read_body();
    if (data.is_err() || data->is_none() || (**data).as_slice() != "abc"_str.as_bytes())
        co_return false;
    if ((co_await connection.read_body()).unwrap().is_some()) co_return false;
    lihttpto::Response response;
    response.body = Bytes::copy_from_slice("ok"_str.as_bytes());
    if ((co_await connection.respond(response)).is_err()) co_return false;
    auto second = co_await connection.read_request();
    if (second.is_err() || second->is_none() || (*second)->line.target.as_str() != "/next"_str)
        co_return false;
    co_return (co_await connection.respond(response)).is_ok() && connection.is_closed();
}
TEST(Transport, GenericConnectionReusesBufferedPipelineWithoutAnotherParser) {
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET /next HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str
            .as_bytes());
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(memory_exchange(wire.clone())));
    auto output = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.contains("Connection: keep-alive\r\n"_str));
    EXPECT_TRUE(output.contains("Connection: close\r\n"_str));
}
auto failed_write(rstd::sync::Arc<MemoryWire> wire) -> rstd::async::coro<bool> {
    liserveto::BasicConnection<MemoryIO> connection(MemoryIO { wire.clone() });
    auto                                 request = co_await connection.read_request();
    if (request.is_err() || request->is_none()) co_return false;
    lihttpto::Response response;
    auto               result = co_await connection.respond(response);
    co_return result.is_err() && result.unwrap_err().cause.is_some() && connection.is_closed();
}
TEST(Transport, WriteFailurePreservesCauseAndClosesConnection) {
    auto wire        = rstd::sync::Arc<MemoryWire>::make();
    wire->input      = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_str.as_bytes());
    wire->fail_write = true;
    auto runtime     = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(failed_write(wire.clone())));
}

struct MigratingMemoryHandler {
    liserveto::Endpoint endpoint;
    int*                selections;
    int*                legacy_calls;
    const void**        selected_path;
    auto select_endpoint(const lihttpto::RequestHead& head) const -> Option<liserveto::Endpoint> {
        ++*selections;
        if (head.line.resource.escaped_path.as_str() != "/new"_str) return None();
        *selected_path = head.line.resource.escaped_path.as_str().as_bytes().as_raw_ptr();
        return Some(endpoint.clone());
    }
    auto operator()(liserveto::BasicConnection<MemoryIO>& connection,
                    const lihttpto::RequestHead&          head,
                    const liserveto::Shutdown&) const
        -> rstd::async::coro<Result<empty, liserveto::ConnectionError>> {
        ++*legacy_calls;
        EXPECT_EQ(head.line.target.as_str(), "/old"_str);
        lihttpto::Response response;
        response.body = Bytes::copy_from_slice("old"_bytes);
        co_return co_await connection.respond(response);
    }
};
TEST(EndpointTransport, SelectedEndpointAndLegacyHandlerShareParsedPipeline) {
    using namespace liserveto;
    auto wire = rstd::sync::Arc<MemoryWire>::make();
    wire->input =
        Bytes::copy_from_slice("POST /new HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc"
                               "GET /old HTTP/1.1\r\nHost: x\r\n\r\n"
                               "GET /new HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_bytes);
    int         selections {}, legacy_calls {}, calls {}, completed {};
    const void* selected_path {};
    auto        peer = rstd::net::SocketAddr::ipv4_loopback(u16(4321));
    auto endpoint    = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
        ++calls;
        EXPECT_TRUE(request.head().line.resource.escaped_path.as_str().as_bytes().as_raw_ptr() ==
                    selected_path);
        EXPECT_TRUE(request.peer == Some(peer));
        EXPECT_FALSE(request.shutdown.requested());
        auto collected = co_await request.collect(u64(3));
        EXPECT_TRUE(collected.is_ok());
        if (collected.is_err()) co_return Err(Error { ErrorKind::Body });
        EXPECT_TRUE(collected->data.as_slice() == (calls == 1 ? "abc"_bytes : ""_bytes));
        auto response = Response::bytes(Bytes::copy_from_slice("new"_bytes));
        response.on_finish([&](const ResponseReport& report) {
            EXPECT_EQ(report.outcome, ResponseOutcome::Complete);
            ++completed;
        });
        co_return Ok(rstd::move(response));
    });
    auto handler     = rstd::sync::Arc<MigratingMemoryHandler>::make(MigratingMemoryHandler {
        rstd::move(endpoint), &selections, &legacy_calls, &selected_path });
    auto runtime     = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                                  rstd::move(handler),
                                                  Shutdown::make().unwrap(),
                                                  {},
                                                  Some(peer))));
    EXPECT_EQ(selections, 3);
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(legacy_calls, 1);
    EXPECT_EQ(completed, 2);
    auto output = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.contains("\r\n\r\nnewHTTP/1.1 200 "_str));
    EXPECT_TRUE(output.contains("\r\n\r\noldHTTP/1.1 200 "_str));
    EXPECT_TRUE(output.ends_with("\r\n\r\nnew"_str));
}
TEST(EndpointTransport, SelectedEndpointErrorUsesStandardRejectionAndCloses) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "GET /new HTTP/1.1\r\nHost: x\r\n\r\nGET /old HTTP/1.1\r\nHost: x\r\n\r\n"_bytes);
    int         selections {}, legacy_calls {};
    const void* selected_path {};
    auto        endpoint = Endpoint::make([](Request) -> rstd::async::coro<ResponseResult> {
        co_return Err(Error { ErrorKind::InvalidRequest });
    });
    auto        handler  = rstd::sync::Arc<MigratingMemoryHandler>::make(MigratingMemoryHandler {
        rstd::move(endpoint), &selections, &legacy_calls, &selected_path });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(serve_connection(
        MemoryIO { wire.clone() }, rstd::move(handler), Shutdown::make().unwrap(), {})));
    EXPECT_EQ(selections, 1);
    EXPECT_EQ(legacy_calls, 0);
    auto output = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.starts_with("HTTP/1.1 400 "_str));
    EXPECT_TRUE(output.contains("Connection: close\r\n"_str));
}
struct DirectMemoryHandler {
    liserveto::Endpoint endpoint;
    int*                selections;
    auto call(liserveto::Request request) -> rstd::async::coro<liserveto::ResponseResult> {
        co_return co_await endpoint.call(rstd::move(request));
    }
    auto select_endpoint(const lihttpto::RequestHead&) const -> Option<liserveto::Endpoint> {
        ++*selections;
        return None();
    }
};
TEST(EndpointTransport, ConnectionFailureDoesNotStartResponseOrReuseInput) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto errors  = array<Option<BodyError>, 4> {
        Some(BodyError::Protocol), Some(BodyError::Timeout), Some(BodyError::Cancelled), None()
    };
    auto outcomes = array<ResponseOutcome, 4> { ResponseOutcome::SourceError,
                                                ResponseOutcome::Timeout,
                                                ResponseOutcome::Cancelled,
                                                ResponseOutcome::SourceError };
    for (usize index {}; index < errors.len(); ++index) {
        auto wire   = rstd::sync::Arc<MemoryWire>::make();
        wire->input = Bytes::copy_from_slice(
            "GET /new HTTP/1.1\r\nHost: x\r\n\r\nGET /old HTTP/1.1\r\nHost: x\r\n\r\n"_bytes);
        int                     selections {}, legacy_calls {};
        const void*             selected_path {};
        Option<RequestLifetime> lifetime;
        auto endpoint = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
            lifetime.insert(request.lifetime.clone());
            co_return Err(Error { ErrorKind::Connection, errors[index] });
        });
        auto handler  = rstd::sync::Arc<MigratingMemoryHandler>::make(MigratingMemoryHandler {
            rstd::move(endpoint), &selections, &legacy_calls, &selected_path });
        EXPECT_FALSE(runtime.block_on(serve_connection(
            MemoryIO { wire.clone() }, rstd::move(handler), Shutdown::make().unwrap(), {})));
        EXPECT_EQ(selections, 1);
        EXPECT_EQ(legacy_calls, 0);
        EXPECT_TRUE(wire->output.is_empty());
        ASSERT_TRUE(lifetime.is_some());
        auto report = lifetime->report();
        ASSERT_TRUE(report.is_some());
        EXPECT_EQ(report->outcome, outcomes[index]);
    }
}
TEST(EndpointTransport, DirectEndpointTakesPrecedenceOverSelector) {
    using namespace liserveto;
    auto wire = rstd::sync::Arc<MemoryWire>::make();
    wire->input =
        Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_bytes);
    int  calls {}, selections {};
    auto endpoint = Endpoint::make([&](Request) -> rstd::async::coro<ResponseResult> {
        ++calls;
        co_return Ok(Response {});
    });
    auto handler  = rstd::sync::Arc<DirectMemoryHandler>::make(
        DirectMemoryHandler { rstd::move(endpoint), &selections });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(serve_connection(
        MemoryIO { wire.clone() }, rstd::move(handler), Shutdown::make().unwrap(), {})));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(selections, 0);
}

struct DeferredRequestState {
    bool                               registered {}, released {};
    int                                retired {}, observed {};
    Option<liserveto::RequestLifetime> lifetime;
};
struct DeferredRequestWork {
    using Output = empty;
    rstd::sync::Arc<DeferredRequestState> state;
    auto poll(mut_ref<DeferredRequestWork> self, Context& cx) -> Poll<empty> {
        if (! self->state->released) {
            cx.waker().wake_by_ref();
            return Poll<empty>::Pending();
        }
        ++self->state->retired;
        return Poll<empty>::Ready(empty {});
    }
};
auto deferred_request_completion(bool cancel) -> rstd::async::coro<bool> {
    using namespace liserveto;
    auto state = rstd::sync::Arc<DeferredRequestState>::make();
    auto wire  = rstd::sync::Arc<MemoryWire>::make();
    wire->input =
        Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_bytes);
    auto endpoint = Endpoint::make(
        [state = state.clone(), cancel](Request request) -> rstd::async::coro<ResponseResult> {
            auto cleanup = request.cleanup().unwrap();
            cleanup.clone().defer(DeferredRequestWork { state.clone() });
            state->lifetime.insert(request.lifetime.clone());
            state->registered = true;
            if (cancel)
                for (;;) co_await rstd::async::yield_now();
            auto response = Response::bytes(Bytes::copy_from_slice("ok"_bytes));
            response.on_finish([state = state.clone()](const ResponseReport&) {
                EXPECT_EQ(state->retired, 1);
                ++state->observed;
            });
            co_return Ok(rstd::move(response));
        });
    auto task = ConnectionTask::spawn(MemoryIO { wire.clone() },
                                      rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                      Shutdown::make().unwrap());
    while (! state->registered) co_await rstd::async::yield_now();
    if (cancel) task.abort();
    auto premature = co_await rstd::async::timeout(rstd::future::by_ref(task),
                                                   rstd::time::Duration::from_millis(u64(1)));
    EXPECT_TRUE(premature.is_err());
    EXPECT_TRUE(state->lifetime->report().is_none());
    EXPECT_EQ(state->retired, 0);
    EXPECT_EQ(state->observed, 0);
    state->released = true;
    if (premature.is_ok()) co_return false;
    auto result = co_await rstd::future::by_ref(task);
    auto report = state->lifetime->report();
    co_return state->retired == 1 && state->observed == (cancel ? 0 : 1) && report.is_some() &&
        report->outcome == (cancel ? ResponseOutcome::Cancelled : ResponseOutcome::Complete) &&
        (cancel ? result.is_err() && result.unwrap_err().is_aborted() : result.is_ok() && *result);
}
TEST(EndpointTransport, DeferredCleanupDelaysNormalAndCancelledCompletion) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (bool cancel : array<bool, 2> { false, true })
        EXPECT_TRUE(runtime.block_on(deferred_request_completion(cancel)));
    lihttpto::RequestHeadDecoder decoder;
    decoder.feed("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_bytes).unwrap();
    auto request = liserveto::Request(decoder.take().unwrap(),
                                      liserveto::IncomingBody::from_body(liserveto::Body {}),
                                      liserveto::Shutdown::make().unwrap());
    auto bound   = request.cleanup();
    ASSERT_TRUE(bound.is_err());
    EXPECT_EQ(bound.unwrap_err().kind().code, rstd::io::error::ErrorKind::NotConnected);
}

struct SelectedCancellationState {
    bool started {};
    int  endpoint_dropped {}, body_dropped {}, completed {};
};
struct SelectedEndpointHold {
    rstd::sync::Arc<SelectedCancellationState> state;
    explicit SelectedEndpointHold(rstd::sync::Arc<SelectedCancellationState> state)
        : state(rstd::move(state)) {}
    ~SelectedEndpointHold() { ++state->endpoint_dropped; }
};
struct PendingSelectedBody {
    using Item = liserveto::BodyItem;
    rstd::sync::Arc<SelectedCancellationState> state;
    explicit PendingSelectedBody(rstd::sync::Arc<SelectedCancellationState> state)
        : state(rstd::move(state)) {}
    PendingSelectedBody(PendingSelectedBody&&) noexcept = default;
    ~PendingSelectedBody() {
        if (state) ++state->body_dropped;
    }
    auto poll_next(mut_ref<PendingSelectedBody> self, Context&)
        -> Poll<Option<liserveto::BodyItem>> {
        self->state->started = true;
        return Poll<Option<liserveto::BodyItem>>::Pending();
    }
};
struct CancellingSelectedHandler {
    rstd::sync::Arc<SelectedCancellationState> state;
    auto select_endpoint(const lihttpto::RequestHead&) const -> Option<liserveto::Endpoint> {
        auto endpoint = liserveto::Endpoint::make(
            [hold = Box<SelectedEndpointHold>::make(state.clone()), state = state.clone()](
                liserveto::Request) -> rstd::async::coro<liserveto::ResponseResult> {
                EXPECT_EQ(hold->state->endpoint_dropped, 0);
                auto response = liserveto::Response::stream(
                    liserveto::Body::from_stream(PendingSelectedBody(state.clone())));
                response.on_finish(
                    [state = state.clone()](const liserveto::ResponseReport& report) {
                        EXPECT_EQ(report.outcome, liserveto::ResponseOutcome::Cancelled);
                        EXPECT_EQ(state->body_dropped, 1);
                        ++state->completed;
                    });
                co_return Ok(rstd::move(response));
            });
        return Some(rstd::move(endpoint));
    }
    auto operator()(liserveto::BasicConnection<MemoryIO>&,
                    const lihttpto::RequestHead&,
                    const liserveto::Shutdown&) const
        -> rstd::async::coro<Result<empty, liserveto::ConnectionError>> {
        ADD_FAILURE();
        co_return Ok(empty {});
    }
};
auto cancel_selected_endpoint(rstd::sync::Arc<SelectedCancellationState> state)
    -> rstd::async::coro<bool> {
    using namespace liserveto;
    auto wire    = rstd::sync::Arc<MemoryWire>::make();
    wire->input  = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_bytes);
    auto handler = rstd::sync::Arc<CancellingSelectedHandler>::make(
        CancellingSelectedHandler { state.clone() });
    auto task = ConnectionTask::spawn(
        MemoryIO { wire.clone() }, rstd::move(handler), Shutdown::make().unwrap());
    for (int index = 0; index < 100 && ! state->started; ++index) co_await rstd::async::yield_now();
    EXPECT_TRUE(state->started);
    EXPECT_EQ(state->endpoint_dropped, 0);
    task.abort();
    auto result = co_await rstd::future::by_ref(task);
    co_return result.is_err() && result.unwrap_err().is_aborted();
}
TEST(EndpointTransport, SelectedEndpointCancellationWaitsForBodyAndObserver) {
    auto state   = rstd::sync::Arc<SelectedCancellationState>::make();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(cancel_selected_endpoint(state.clone())));
    EXPECT_EQ(state->endpoint_dropped, 1);
    EXPECT_EQ(state->body_dropped, 1);
    EXPECT_EQ(state->completed, 1);
}

TEST(RepresentationResponse, EndpointKeepsPartialHeadAndNotModifiedResponsesDelimited) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "GET /file HTTP/1.1\r\nHost: x\r\nRange: bytes=2-4\r\n\r\n"
        "HEAD /file HTTP/1.1\r\nHost: x\r\nRange: bytes=2-4\r\n\r\n"
        "GET /file HTTP/1.1\r\nHost: x\r\nIf-None-Match: W/\"content\"\r\nConnection: close\r\n\r\n"_bytes);
    int  requests {}, sources {};
    auto endpoint = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
        ++requests;
        auto conditions = lihttpto::RequestConditions::parse(request.head().headers).unwrap();
        lihttpto::RepresentationMetadata metadata;
        metadata.length = u64(10);
        metadata.etag   = Some(lihttpto::EntityTag::parse("\"content\""_bytes).unwrap());
        co_return Ok(representation_response(
            request.head().line.method.as_str(),
            conditions,
            metadata,
            [&](lihttpto::ByteRange range) {
                ++sources;
                auto bytes = "0123456789"_bytes;
                return Body::from_bytes(Bytes::copy_from_slice(
                    slice<u8>::from_raw_parts(bytes.as_raw_ptr() + range.offset.to_primitive(),
                                              usize(range.length.to_primitive()))));
            }));
    });
    auto owner    = rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint));
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(serve_connection(
        MemoryIO { wire.clone() }, rstd::move(owner), Shutdown::make().unwrap(), {})));
    EXPECT_EQ(requests, 3);
    EXPECT_EQ(sources, 1);
    auto output = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.contains("HTTP/1.1 206 "_str));
    EXPECT_TRUE(output.contains("content-range: bytes 2-4/10\r\n"_str));
    EXPECT_TRUE(output.contains("\r\n\r\n234HTTP/1.1 200 "_str));
    EXPECT_TRUE(output.contains("Content-Length: 10\r\n"_str));
    EXPECT_TRUE(output.contains("\r\n\r\nHTTP/1.1 304 "_str));
    EXPECT_TRUE(output.ends_with("Connection: close\r\n\r\n"_str));
}

auto owned_memory_connection(int mode) -> rstd::async::coro<bool> {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str.as_bytes());
    wire->fail_write = mode == 1;
    int  calls       = 0;
    auto endpoint    = Endpoint::make([&](Request) -> rstd::async::coro<ResponseResult> {
        ++calls;
        co_return Ok(Response {});
    });
    auto task        = ConnectionTask::spawn(MemoryIO { wire.clone() },
                                             rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                             Shutdown::make().unwrap());
    if (mode == 2) task.abort();
    auto result = co_await rstd::future::by_ref(task);
    if (mode == 2) co_return result.is_err() && result.unwrap_err().is_aborted() && calls == 0;
    co_return result.is_ok() && *result == (mode == 0) && calls == 1;
}
TEST(EndpointTransport, ConnectionTaskPreservesResultAndAbortsBeforeFirstPoll) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 3; ++mode)
        EXPECT_TRUE(runtime.block_on(owned_memory_connection(mode)));
}

TEST(EndpointTransport, IncomingTrailersAndUnknownLengthResponseKeepPipelineAlive) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST /one HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nX-End: yes\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str
            .as_bytes());
    int  calls    = 0;
    int  finished = 0;
    auto endpoint = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
        ++calls;
        if (calls == 1) {
            auto moved     = rstd::move(request);
            auto collected = co_await moved.collect(u64(3));
            if (collected.is_err() || collected->data.as_slice() != "abc"_str.as_bytes() ||
                collected->trailers.get_unique_text("x-end"_str).unwrap().unwrap() != "yes"_str)
                co_return Err(Error { ErrorKind::Internal });
            lihttpto::Headers trailers;
            trailers.add("X-Out"_str, "done"_str).unwrap();
            Vec<BodyItem> parts;
            parts.push(
                Ok(lihttpto::BodyFrame::Data(Bytes::copy_from_slice("hello"_str.as_bytes()))));
            parts.push(Ok(lihttpto::BodyFrame::Trailers(rstd::move(trailers))));
            auto response =
                Response::stream(Body::from_stream(rstd::stream::iter(rstd::move(parts))));
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_EQ(report.outcome, ResponseOutcome::Complete);
                EXPECT_EQ(report.bytes, u64(5));
                ++finished;
            });
            co_return Ok(rstd::move(response));
        }
        if (request.target() != "/next"_str) co_return Err(Error { ErrorKind::Internal });
        co_return Ok(Response::bytes(Bytes::copy_from_slice("next"_str.as_bytes())));
    });
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(
        runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                          rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                          Shutdown::make().unwrap(),
                                          {})));
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(finished, 1);
    auto text = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(text.contains("Transfer-Encoding: chunked\r\nConnection: keep-alive\r\n"_str));
    EXPECT_TRUE(text.contains("5\r\nhello\r\n0\r\nX-Out: done\r\n\r\nHTTP/1.1 200"_str));
    EXPECT_TRUE(text.ends_with("Connection: close\r\n\r\nnext"_str));
}
struct ObservedBody {
    using Item = liserveto::BodyItem;
    int* polls;
    auto poll_next(mut_ref<ObservedBody> self, Context&) -> Poll<Option<Item>> {
        ++*self->polls;
        return Poll<Option<Item>>::Pending();
    }
};
TEST(EndpointTransport, HeadDoesNotStartSourceAndUnreadBodyClosesWithoutContinue) {
    using namespace liserveto;
    auto runtime     = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  polls       = 0;
    auto head_wire   = rstd::sync::Arc<MemoryWire>::make();
    head_wire->input = Bytes::copy_from_slice(
        "HEAD / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str.as_bytes());
    auto head = Endpoint::make([&](Request) -> rstd::async::coro<ResponseResult> {
        co_return Ok(Response::stream(Body::from_stream(ObservedBody { &polls }), Some(u64(100))));
    });
    EXPECT_TRUE(runtime.block_on(serve_connection(MemoryIO { head_wire.clone() },
                                                  rstd::sync::Arc<Endpoint>::make(rstd::move(head)),
                                                  Shutdown::make().unwrap(),
                                                  {})));
    EXPECT_EQ(polls, 0);
    EXPECT_TRUE(rstd::str_::from_utf8(head_wire->output.as_slice())
                    .unwrap()
                    .contains("Content-Length: 100\r\n"_str));
    auto reject_wire   = rstd::sync::Arc<MemoryWire>::make();
    reject_wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\nExpect: 100-continue\r\n\r\n"_str
            .as_bytes());
    auto reject = Endpoint::make([](Request) -> rstd::async::coro<ResponseResult> {
        Response response;
        response.head.status = lihttpto::StatusCode::make(u16(403)).unwrap();
        co_return Ok(rstd::move(response));
    });
    EXPECT_TRUE(
        runtime.block_on(serve_connection(MemoryIO { reject_wire.clone() },
                                          rstd::sync::Arc<Endpoint>::make(rstd::move(reject)),
                                          Shutdown::make().unwrap(),
                                          {})));
    auto output = rstd::str_::from_utf8(reject_wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.starts_with("HTTP/1.1 403"_str));
    EXPECT_TRUE(output.contains("Connection: close\r\n"_str));
    EXPECT_FALSE(output.contains("100 Continue"_str));
}
TEST(EndpointTransport, IncomingBodyCannotDriveResponseAfterHandlerReturns) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nx"_str.as_bytes());
    auto endpoint = Endpoint::make([](Request request) -> rstd::async::coro<ResponseResult> {
        co_return Ok(Response::stream(Body::from_stream(request.take_body().unwrap())));
    });
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_FALSE(
        runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                          rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                          Shutdown::make().unwrap(),
                                          {})));
    EXPECT_TRUE(wire->output.is_empty());
}
auto retry_incoming(rstd::sync::Arc<MemoryWire> wire) -> rstd::async::coro<bool> {
    using namespace liserveto;
    auto connection = rstd::sync::Arc<BasicConnection<MemoryIO>>::make(MemoryIO { wire.clone() });
    auto head       = co_await connection->read_request();
    if (head.is_err() || head->is_none()) co_return false;
    auto incoming      = IncomingBody::from_connection(connection.clone());
    auto scope         = incoming.scope();
    wire->pending_read = true;
    auto waited =
        co_await rstd::async::timeout(rstd::stream::next(incoming), rstd::time::Duration {});
    if (waited.is_ok() || connection->is_closed()) co_return false;
    auto moved         = rstd::move(incoming);
    wire->pending_read = false;
    wire->input        = Bytes::copy_from_slice("x"_str.as_bytes());
    auto next          = co_await rstd::stream::next(moved);
    if (next.is_none() || next->is_err() ||
        next->unwrap().as_Data().bytes.as_slice() != "x"_str.as_bytes())
        co_return false;
    if ((co_await rstd::stream::next(moved)).is_some() || ! scope.complete()) co_return false;
    co_return ! connection->is_closed();
}
TEST(EndpointTransport, NextTimeoutAndMoveRetainPendingInput) {
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n"_str.as_bytes());
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(retry_incoming(wire.clone())));
}
TEST(EndpointTransport, ScopeRevokesEscapedInputAndCancelsPendingRead) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n"_str.as_bytes());
    auto runtime    = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto connection = rstd::sync::Arc<BasicConnection<MemoryIO>>::make(MemoryIO { wire.clone() });
    ASSERT_TRUE(runtime.block_on(connection->read_request()).is_ok());
    auto input = IncomingBody::from_connection(connection.clone());
    {
        auto scope         = input.scope();
        wire->pending_read = true;
        EXPECT_TRUE(
            runtime
                .block_on(rstd::async::timeout(rstd::stream::next(input), rstd::time::Duration {}))
                .is_err());
        EXPECT_FALSE(connection->is_closed());
    }
    EXPECT_TRUE(connection->is_closed());
    auto error = runtime.block_on(rstd::stream::next(input));
    ASSERT_TRUE(error.is_some());
    EXPECT_EQ(error->unwrap_err(), BodyError::Cancelled);
    EXPECT_TRUE(runtime.block_on(rstd::stream::next(input)).is_none());
}

TEST(EndpointTransport, SourceFailureFixedLengthMismatchAndWriteErrorReportOnce) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 3; ++mode) {
        auto wire   = rstd::sync::Arc<MemoryWire>::make();
        wire->input = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_str.as_bytes());
        wire->fail_write = mode == 2;
        int  notified    = 0;
        auto endpoint    = Endpoint::make([&](Request) -> rstd::async::coro<ResponseResult> {
            Vec<BodyItem> parts;
            parts.push(Ok(lihttpto::BodyFrame::Data(Bytes::copy_from_slice("x"_str.as_bytes()))));
            if (mode == 0) parts.push(Err(BodyError::Source));
            auto response = Response::stream(
                Body::from_stream(rstd::stream::iter(rstd::move(parts))), Some(u64(2)));
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_EQ(report.outcome,
                          mode == 2 ? ResponseOutcome::WriteError : ResponseOutcome::SourceError);
                EXPECT_EQ(report.bytes, mode == 2 ? u64() : u64(1));
                ++notified;
            });
            co_return Ok(rstd::move(response));
        });
        EXPECT_FALSE(
            runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                              rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                              Shutdown::make().unwrap(),
                                              {})));
        EXPECT_EQ(notified, 1);
    }
}
TEST(EndpointTransport, IdleDeadlineAndCancellationHaveDistinctCompletionEvents) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 2; ++mode) {
        auto wire     = rstd::sync::Arc<MemoryWire>::make();
        wire->input   = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_str.as_bytes());
        int  polls    = 0;
        int  notified = 0;
        auto endpoint = Endpoint::make([&](Request) -> rstd::async::coro<ResponseResult> {
            auto response = Response::stream(Body::from_stream(ObservedBody { &polls }));
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_EQ(report.outcome,
                          mode == 0 ? ResponseOutcome::Timeout : ResponseOutcome::Cancelled);
                ++notified;
            });
            co_return Ok(rstd::move(response));
        });
        ConnectionLimits limits;
        limits.stream_idle_timeout = Some(rstd::time::Duration::from_millis(u64(2)));
        auto run = serve_connection(MemoryIO { wire.clone() },
                                    rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                    Shutdown::make().unwrap(),
                                    limits);
        if (mode == 0)
            EXPECT_FALSE(runtime.block_on(rstd::move(run)));
        else {
            auto cancel = [&]() -> rstd::async::coro<bool> {
                auto task = rstd::async::spawn(rstd::move(run));
                co_await rstd::async::sleep(rstd::time::Duration::from_millis(u64(1)));
                task.abort();
                co_return (co_await rstd::move(task)).is_err();
            };
            EXPECT_TRUE(runtime.block_on(cancel()));
        }
        EXPECT_EQ(notified, 1);
        EXPECT_TRUE(polls > 0);
    }
}

TEST(Lifecycle, RequestEndsAfterResponseAndKeepsOutcomeForLateObservers) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 3; ++mode) {
        auto wire   = rstd::sync::Arc<MemoryWire>::make();
        wire->input = Bytes::copy_from_slice(
            "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str.as_bytes());
        wire->fail_write = mode == 1;
        Option<RequestLifetime> lifetime;
        int                     polls = 0;
        auto endpoint = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
            lifetime.insert(request.lifetime.clone());
            EXPECT_TRUE(lifetime->report().is_none());
            if (mode == 2)
                co_return Ok(Response::stream(Body::from_stream(ObservedBody { &polls })));
            auto response = Response::bytes(Bytes::copy_from_slice("ok"_str.as_bytes()));
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_TRUE(lifetime->report().is_some());
                EXPECT_EQ(lifetime->report()->outcome, report.outcome);
            });
            co_return Ok(rstd::move(response));
        });
        auto run      = serve_connection(MemoryIO { wire.clone() },
                                         rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                         Shutdown::make().unwrap(),
                                         {});
        if (mode == 2) {
            auto timed =
                runtime.block_on(rstd::async::timeout(rstd::async::into_future(rstd::move(run)),
                                                      rstd::time::Duration::from_millis(u64(2))));
            EXPECT_TRUE(timed.is_err());
        } else
            EXPECT_EQ(runtime.block_on(rstd::move(run)), mode == 0);
        ASSERT_TRUE(lifetime.is_some());
        auto report = runtime.block_on(lifetime->finished()).unwrap();
        EXPECT_EQ(report.outcome,
                  mode == 0   ? ResponseOutcome::Complete
                  : mode == 1 ? ResponseOutcome::WriteError
                              : ResponseOutcome::Cancelled);
        EXPECT_EQ(runtime.block_on(lifetime->finished()).unwrap().outcome, report.outcome);
    }
}

TEST(EndpointUpgrade, DelayedActionUsesFinalHeadersAndBufferedBytesAfterKeepAlive) {
    using namespace liserveto;
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /upgrade HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: sample\r\n\r\nfirst"_str
            .as_bytes());
    int  calls   = 0;
    auto handler = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
        if (request.target() == "/one"_str) co_return Ok(Response {});
        auto response =
            Response::on_upgrade(
                request.upgrade.take().unwrap(),
                lihttpto::UpgradeProtocol::make("sample"_str).unwrap(),
                [&](UpgradedStream stream, auto) -> rstd::async::coro<Result<empty, BodyError>> {
                    ++calls;
                    EXPECT_TRUE(rstd::str_::from_utf8(wire->output.as_slice())
                                    .unwrap()
                                    .ends_with("X-Final: yes\r\n\r\n"_str));
                    BytesMut first;
                    auto     received = co_await rstd::async::io::read(stream, first);
                    if (received.is_err() || first.as_slice() != "first"_str.as_bytes())
                        co_return Err(BodyError::Protocol);
                    co_return Ok(empty {});
                })
                .unwrap();
        EXPECT_EQ(calls, 0);
        response.head.headers.add("X-Final"_str, "yes"_str).unwrap();
        co_return Ok(rstd::move(response));
    });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(
        runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                          rstd::sync::Arc<Endpoint>::make(rstd::move(handler)),
                                          Shutdown::make().unwrap(),
                                          {})));
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(
        rstd::str_::from_utf8(wire->output.as_slice())
            .unwrap()
            .contains("Connection: keep-alive\r\n\r\nHTTP/1.1 101 Switching Protocols"_str));
}

TEST(EndpointUpgrade, DiscardInvalidMetadataAndPartialWriteNeverActivateAction) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 3; ++mode) {
        auto wire   = rstd::sync::Arc<MemoryWire>::make();
        wire->input = Bytes::copy_from_slice(
            "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: sample\r\n\r\n"_str
                .as_bytes());
        if (mode == 2) {
            wire->partial_write = usize(8);
            wire->writes_left   = 1;
        }
        int  calls    = 0;
        int  discards = 0;
        auto handler  = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
            auto response =
                Response::on_upgrade(
                    request.upgrade.take().unwrap(),
                    lihttpto::UpgradeProtocol::make("sample"_str).unwrap(),
                    [&](UpgradedStream, auto) -> rstd::async::coro<Result<empty, BodyError>> {
                        ++calls;
                        co_return Ok(empty {});
                    })
                    .unwrap();
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_EQ(report.outcome,
                          mode == 0   ? ResponseOutcome::Cancelled
                          : mode == 1 ? ResponseOutcome::SourceError
                                      : ResponseOutcome::WriteError);
                ++discards;
            });
            if (mode == 0) {
                Response replacement;
                replacement.close_after = true;
                co_return Ok(rstd::move(replacement));
            }
            if (mode == 1) response.head.headers.set("Upgrade"_str, "other"_str).unwrap();
            co_return Ok(rstd::move(response));
        });
        EXPECT_EQ(
            runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                              rstd::sync::Arc<Endpoint>::make(rstd::move(handler)),
                                              Shutdown::make().unwrap(),
                                              {})),
            mode == 0);
        EXPECT_EQ(calls, 0);
        EXPECT_EQ(discards, 1);
        if (mode == 2) EXPECT_EQ(wire->output.len(), usize(8));
    }
}

#ifdef LITO_FEAT_WEBSOCKET
TEST(WebSocketEndpoint, ValidatesFinalHandshakeBeforeWritingOrStartingDriver) {
    using namespace liserveto;
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (int mode = 0; mode < 8; ++mode) {
        auto wire   = rstd::sync::Arc<MemoryWire>::make();
        wire->input = Bytes::copy_from_slice(
            "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: one, two\r\n\r\n"_str
                .as_bytes());
        int  activated = 0, notified = 0;
        auto endpoint = Endpoint::make([&](Request request) -> rstd::async::coro<ResponseResult> {
            auto upgrade  = WebSocketUpgrade::from_request(request, Some("one"_str)).unwrap();
            auto response = rstd::move(upgrade)
                                .accept([&](WebSocket&, Shutdown, RequestLifetime)
                                            -> rstd::async::coro<Result<empty, BodyError>> {
                                    ++activated;
                                    co_return Ok(empty {});
                                })
                                .unwrap();
            switch (mode) {
            case 0:
                response.head.headers.set("Sec-WebSocket-Accept"_str, "wrong"_str).unwrap();
                break;
            case 1: response.head.headers.remove("Sec-WebSocket-Accept"_str); break;
            case 2:
                response.head.headers
                    .add("Sec-WebSocket-Accept"_str, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="_str)
                    .unwrap();
                break;
            case 3:
                response.head.headers.set("Sec-WebSocket-Protocol"_str, "two"_str).unwrap();
                break;
            case 4: response.head.headers.remove("Sec-WebSocket-Protocol"_str); break;
            case 5:
                response.head.headers.add("Sec-WebSocket-Extensions"_str, "permessage-deflate"_str)
                    .unwrap();
                break;
            case 6:
                response.head.headers.add("Sec-WebSocket-Protocol"_str, "one"_str).unwrap();
                break;
            case 7: response.head.headers.add("X-Final"_str, "yes"_str).unwrap(); break;
            }
            response.on_finish([&](const ResponseReport& report) {
                EXPECT_EQ(report.outcome,
                          mode == 7 ? ResponseOutcome::Complete : ResponseOutcome::SourceError);
                ++notified;
            });
            co_return Ok(rstd::move(response));
        });
        EXPECT_EQ(
            runtime.block_on(serve_connection(MemoryIO { wire.clone() },
                                              rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                              Shutdown::make().unwrap(),
                                              {})),
            mode == 7);
        EXPECT_EQ(activated, mode == 7 ? 1 : 0);
        EXPECT_EQ(notified, 1);
        if (mode == 7)
            EXPECT_TRUE(rstd::str_::from_utf8(wire->output.as_slice())
                            .unwrap()
                            .contains("X-Final: yes\r\n"_str));
        else
            EXPECT_TRUE(wire->output.is_empty());
    }
}
#endif
