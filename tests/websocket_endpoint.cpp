#ifdef LITO_FEAT_WEBSOCKET
#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liwebsocketo;
using namespace liserveto;
using rstd::async::coro;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;

struct EndpointSocket {
    struct Release {
        int* count;
        explicit Release(int& value): count(&value) {}
        Release(Release&& other) noexcept: count(rstd::exchange(other.count, nullptr)) {}
        ~Release() {
            if (count) ++*count;
        }
    } release;
    rstd::net::TcpStream stream;
    EndpointSocket(rstd::net::TcpStream value, int& released)
        : release(released), stream(rstd::move(value)) {}
    auto poll_read(mut_ref<EndpointSocket> self, rstd::task::Context& cx, BytesMut& bytes)
        -> rstd::task::Poll<rstd::io::Result<usize>> {
        return rstd::async::io::poll_read(self->stream, cx, bytes);
    }
    auto poll_write(mut_ref<EndpointSocket> self, rstd::task::Context& cx, const Bytes& bytes)
        -> rstd::task::Poll<rstd::io::Result<usize>> {
        return rstd::async::io::poll_write(self->stream, cx, bytes);
    }
    auto poll_flush(mut_ref<EndpointSocket> self, rstd::task::Context& cx)
        -> rstd::task::Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_flush(self->stream, cx);
    }
    auto poll_shutdown(mut_ref<EndpointSocket> self, rstd::task::Context& cx)
        -> rstd::task::Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_shutdown(self->stream, cx);
    }
};

auto managed_endpoint_socket(bool force,
                             bool embedded        = false,
                             bool delayed_cleanup = false,
                             bool idle_pause      = false) -> coro<bool> {
    auto listener =
        rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
    auto                    address         = listener.local_addr().unwrap();
    auto                    stop            = Shutdown::make().unwrap();
    auto                    entered         = Shutdown::make().unwrap();
    auto                    handler_waiting = Shutdown::make().unwrap();
    auto                    handler_release = Shutdown::make().unwrap();
    int                     dropped         = 0;
    int                     completed       = 0;
    int                     released        = 0;
    Option<RequestLifetime> lifetime;
    Router                  routes;
    routes
        .get("/api"_str, Endpoint::make([](Request) -> coro<ResponseResult> {
                 co_return Ok(liserveto::Response {});
             }))
        .unwrap();
    routes
        .get("/ws"_str, Endpoint::make([&](Request request) -> coro<ResponseResult> {
                 auto upgrade = WebSocketUpgrade::from_request(request, Some("test"_str));
                 if (upgrade.is_err())
                     co_return Err(liserveto::Error { ErrorKind::InvalidRequest });
                 lifetime.insert(request.lifetime.clone());
                 auto response = rstd::move(*upgrade).accept(
                     [&](WebSocket& socket,
                         Shutdown,
                         RequestLifetime running) -> coro<Result<empty, BodyError>> {
                         struct Guard {
                             int* dropped;
                             ~Guard() { ++*dropped; }
                         } guard { &dropped };
                         EXPECT_TRUE(running.report().is_none());
                         entered.request_stop().unwrap();
                         if (force) {
                             co_await rstd::async::sleep(rstd::time::Duration::from_secs(u64(60)));
                             co_return Err(BodyError::Timeout);
                         }
                         auto messages = socket.messages().unwrap();
                         auto first    = co_await rstd::stream::next(messages);
                         EXPECT_TRUE(first.is_some());
                         if (first.is_some()) EXPECT_TRUE(first->is_ok());
                         if (first.is_none() || first->is_err() ||
                             (**first).data.as_slice() != "hello"_str.as_bytes())
                             co_return Err(BodyError::Protocol);
                         auto sent = co_await socket.send(rstd::move(*first).unwrap());
                         EXPECT_TRUE(sent.is_ok());
                         if (sent.is_ok()) EXPECT_TRUE(sent->is_ok());
                         if (sent.is_err() || sent->is_err()) co_return Err(BodyError::Io);
                         if ((co_await rstd::stream::next(messages)).is_some())
                             co_return Err(BodyError::Protocol);
                         if (delayed_cleanup) {
                             handler_waiting.request_stop().unwrap();
                             co_await handler_release.notified();
                         }
                         co_return Ok(empty {});
                     },
                     {},
                     idle_pause ? rstd::time::Duration::from_millis(u64(10))
                                : rstd::time::Duration::from_secs(u64(30)));
                 if (response.is_err())
                     co_return Err(liserveto::Error { ErrorKind::InvalidRequest });
                 response->head.headers.add("X-Final"_str, "yes"_str).unwrap();
                 response->on_finish([&](const ResponseReport& report) {
                     EXPECT_EQ(dropped, 1);
                     if (embedded) EXPECT_EQ(released, 1);
                     EXPECT_EQ(report.outcome,
                               force ? ResponseOutcome::Cancelled : ResponseOutcome::Complete);
                     ++completed;
                 });
                 co_return Ok(rstd::move(*response));
             }))
        .unwrap();
    ServerLimits limits;
    limits.shutdown_timeout = delayed_cleanup ? rstd::time::Duration::from_secs(u64(1))
                                              : rstd::time::Duration::from_millis(u64(2));
    auto endpoint           = rstd::sync::Arc<Endpoint>::make(rstd::move(routes).into_endpoint());
    Option<rstd::async::AbortOnDropHandle<Result<ServerReport, rstd::io::error::Error>>> server;
    Option<ConnectionTask>                                                               connection;
    if (! embedded)
        server.insert(rstd::async::AbortOnDropHandle(rstd::async::spawn(
            serve(rstd::move(listener), endpoint.clone(), stop.clone(), limits))));
    auto client = (co_await rstd::net::TcpStream::connect(address)).unwrap();
    if (embedded) {
        auto accepted = (co_await listener.accept()).unwrap();
        connection.insert(
            ConnectionTask::spawn(EndpointSocket(rstd::move(accepted.get<0>()), released),
                                  endpoint.clone(),
                                  stop.clone(),
                                  limits.connection,
                                  Some(accepted.get<1>())));
    }
    array<u8, 16> nonce {};
    Vec<String>   protocols;
    protocols.push(String::make("test"_str));
    auto handshake =
        ClientHandshake::make("/ws"_str, "local"_str, nonce, {}, rstd::move(protocols)).unwrap();
    Session session(Role::Client);
    auto text = session.send(Message { Kind::Text, Bytes::copy_from_slice("hello"_str.as_bytes()) })
                    .unwrap();
    BytesMut input;
    input.put_slice("GET /api HTTP/1.1\r\nHost: x\r\n\r\n"_str.as_bytes());
    input.put_slice(handshake.request().bytes().as_slice());
    if (! force && ! idle_pause) input.put_slice(text.bytes.as_slice());
    if ((co_await rstd::async::io::write_all(client, input.freeze())).is_err()) co_return false;
    session.written(text.ticket).unwrap();
    auto api    = "HTTP/1.1 200 \r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"_str;
    auto buffer = BytesMut::with_capacity(api.len());
    if ((co_await rstd::async::io::read_exact(client, buffer, api.len())).is_err() ||
        buffer.as_slice() != api.as_bytes())
        co_return false;
    buffer = BytesMut::with_capacity(usize(1024));
    lihttpto::Http1HeadParser parser;
    Bytes                     pending;
    for (;;) {
        buffer.clear();
        auto read = co_await rstd::async::io::read(client, buffer);
        if (read.is_err() || *read == usize()) co_return false;
        auto parsed = parser.push(buffer.as_slice());
        if (parsed.is_err()) co_return false;
        if (parsed->is_NeedMore()) continue;
        auto complete = rstd::move(parsed).unwrap().as_Complete();
        auto response = rstd::move(complete.head).into_response().unwrap();
        if (handshake.validate(response).is_err() || ! response.headers.contains("X-Final"_str))
            co_return false;
        buffer.advance(complete.input_consumed);
        pending = Bytes::copy_from_slice(buffer.as_slice());
        break;
    }
    if (! force) {
        if (idle_pause) {
            co_await rstd::async::sleep(rstd::time::Duration::from_millis(u64(50)));
            EXPECT_EQ(dropped, 0);
            EXPECT_TRUE(lifetime->report().is_none());
            if (dropped != 0 || lifetime->report().is_some()) co_return false;
            if ((co_await rstd::async::io::write_all(client, text.bytes)).is_err()) co_return false;
        }
        bool closed = false;
        bool echoed = false;
        while (! closed) {
            if (pending.is_empty()) {
                buffer.clear();
                auto read = co_await rstd::async::io::read(client, buffer);
                EXPECT_TRUE(read.is_ok());
                if (read.is_ok()) EXPECT_NE(*read, usize());
                if (read.is_err() || *read == usize()) co_return false;
                pending = Bytes::copy_from_slice(buffer.as_slice());
            }
            auto progress = session.receive(pending.as_slice()).unwrap();
            pending.advance(progress.consumed);
            if (progress.message.is_none()) continue;
            if (! echoed) {
                EXPECT_EQ(progress.message->kind, Kind::Text);
                EXPECT_EQ(progress.message->data.as_slice(), "hello"_str.as_bytes());
                if (progress.message->kind != Kind::Text ||
                    progress.message->data.as_slice() != "hello"_str.as_bytes())
                    co_return false;
                echoed = true;
                session.close().unwrap();
                auto close = session.next_control().unwrap().take().unwrap();
                if ((co_await rstd::async::io::write_all(client, close.bytes)).is_err())
                    co_return false;
                session.written(close.ticket).unwrap();
            } else
                closed = progress.message->kind == Kind::Close;
        }
    }
    if ((co_await entered.notified()).is_err()) co_return false;
    if (delayed_cleanup) {
        if ((co_await handler_waiting.notified()).is_err()) co_return false;
        stop.request_stop().unwrap();
        for (int index = 0; index < 8; ++index) co_await rstd::async::yield_now();
        EXPECT_EQ(dropped, 0);
        EXPECT_EQ(completed, 0);
        EXPECT_TRUE(lifetime->report().is_none());
        handler_release.request_stop().unwrap();
    }
    if (! force) {
        auto completed = co_await lifetime->finished();
        if (completed.is_err() || completed->outcome != ResponseOutcome::Complete) co_return false;
    }
    if (embedded) {
        if (force) {
            auto abandoned = co_await rstd::async::timeout(
                rstd::future::by_ref(*connection), rstd::time::Duration::from_millis(u64(1)));
            if (abandoned.is_ok() || completed != 0 || released != 0 ||
                lifetime->report().is_some())
                co_return false;
            connection->abort();
        }
        auto moved  = connection.take().unwrap();
        auto joined = co_await rstd::future::by_ref(moved);
        if (force ? joined.is_ok() : (joined.is_err() || ! *joined)) co_return false;
        if (released != 1) co_return false;
    } else {
        stop.request_stop().unwrap();
        auto report = co_await rstd::move(*server);
        if (report.is_err() || report->is_err() || (**report).accepted != usize(1) ||
            (**report).failed != usize() || (**report).cancelled != usize(force ? 1 : 0))
            co_return false;
    }
    if (dropped != 1 || completed != 1) co_return false;
    if (lifetime->report().is_none()) co_return false;
    for (;;) {
        buffer.clear();
        auto read = co_await rstd::async::io::read(client, buffer);
        if (read.is_err()) co_return false;
        if (*read == usize()) break;
    }
    co_return true;
}
TEST(WebSocketEndpoint, SharesApiRouterAndPreservesFirstFrameAfterKeepAlive) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(managed_endpoint_socket(false)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketEndpoint, IdleReadOutlivesWriteDeadlineThenExchangesFrames) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(rstd::async::timeout(
        rstd::async::into_future(managed_endpoint_socket(false, false, false, true)),
        rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketEndpoint, ForcedShutdownWaitsForDriverTasksBeforeCompletion) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(managed_endpoint_socket(true)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketEndpoint, EmbeddedTaskOwnsCleanupAfterAbandoningWaitAndAbortingConnection) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(managed_endpoint_socket(true, true)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketEndpoint, EmbeddedTaskCompletesUpgradeAndReleasesTransport) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(managed_endpoint_socket(false, true)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketEndpoint, ShutdownWaitsForHandlerAfterCloseCompletes) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(managed_endpoint_socket(false, false, true)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
#endif
