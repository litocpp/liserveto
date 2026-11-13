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

static auto serve_one(rstd::net::TcpListener& listener) -> coro<bool> {
    auto accepted = co_await listener.accept();
    if (accepted.is_err()) co_return false;
    auto                  pair = rstd::move(accepted).unwrap();
    liserveto::Connection http(rstd::move(pair.template get<0>()));
    auto                  request = co_await http.read_request();
    if (request.is_err() || request->is_none()) co_return false;
    auto head          = rstd::move(*request).unwrap();
    auto authorization = head.headers.get_unique_text("authorization"_str);
    if (authorization.is_err() || authorization->is_none() ||
        **authorization != "Bearer local-test"_str)
        co_return false;
    auto handshake = ServerHandshake::parse(head, Some("fern.test"_str));
    if (handshake.is_err()) co_return false;
    auto prepared = http.prepare_upgrade(lihttpto::UpgradeProtocol::make("websocket"_str).unwrap(),
                                         handshake->headers());
    if (prepared.is_err()) co_return false;
    auto upgraded = co_await http.upgrade(rstd::move(prepared).unwrap());
    if (upgraded.is_err()) co_return false;
    Session  session(Role::Server);
    auto     pending  = rstd::move(upgraded->pending);
    auto     buffer   = BytesMut::with_capacity(usize(1024));
    unsigned messages = 0;
    for (;;) {
        if (pending.is_empty()) {
            buffer.clear();
            auto read = co_await rstd::async::io::read(upgraded->stream, buffer);
            if (read.is_err() || *read == usize()) co_return false;
            pending = Bytes::copy_from_slice(buffer.as_slice());
        }
        auto progress = session.receive(pending.as_slice());
        if (progress.is_err()) co_return false;
        pending.advance(progress->consumed);
        if (progress->message.is_some() && progress->message->kind == Kind::Text) {
            auto output = session.send(*progress->message);
            if (output.is_err() ||
                (co_await rstd::async::io::write_all(upgraded->stream, output->bytes)).is_err() ||
                session.written(output->ticket).is_err())
                co_return false;
            ++messages;
        }
        auto control = session.next_control();
        if (control.is_err()) co_return false;
        if (control->is_some()) {
            auto output = control->take().unwrap();
            if ((co_await rstd::async::io::write_all(upgraded->stream, output.bytes)).is_err() ||
                session.written(output.ticket).is_err())
                co_return false;
            ++messages;
        }
        if (session.state() == SessionState::Closed)
            co_return messages == 3 && session.eof().is_ok();
    }
}

static auto roundtrip(rstd::net::TcpListener& listener, rstd::net::SocketAddr address)
    -> coro<bool> {
    auto server    = rstd::async::AbortOnDropHandle { rstd::async::spawn(serve_one(listener)) };
    auto connected = co_await rstd::net::TcpStream::connect(address);
    if (connected.is_err()) co_return false;
    auto          socket = rstd::move(connected).unwrap();
    array<u8, 16> nonce;
    if (rstd::random::fill_secure(nonce.as_mut_slice()).is_err()) co_return false;
    lihttpto::Headers headers;
    headers.add("Authorization"_str, "Bearer local-test"_str).unwrap();
    Vec<String> protocols;
    protocols.push(String::make("fern.test"_str));
    auto handshake =
        ClientHandshake::make(
            "/worker"_str, "localhost"_str, nonce, rstd::move(headers), rstd::move(protocols))
            .unwrap();
    if ((co_await rstd::async::io::write_all(socket, handshake.request().bytes())).is_err())
        co_return false;
    lihttpto::Http1HeadParser parser;
    Bytes                     pending;
    auto                      buffer = BytesMut::with_capacity(usize(1024));
    for (;;) {
        buffer.clear();
        auto read = co_await rstd::async::io::read(socket, buffer);
        if (read.is_err() || *read == usize()) co_return false;
        auto parsed = parser.push(buffer.as_slice());
        if (parsed.is_err()) co_return false;
        if (parsed->is_NeedMore()) continue;
        auto complete = rstd::move(parsed).unwrap().as_Complete();
        auto response = rstd::move(complete.head).into_response();
        if (response.is_err() || handshake.validate(*response).is_err()) co_return false;
        buffer.advance(complete.input_consumed);
        pending = Bytes::copy_from_slice(buffer.as_slice());
        break;
    }
    Session session(Role::Client);
    for (auto kind : array<Kind, 3> { Kind::Text, Kind::Ping, Kind::Close }) {
        auto    body = kind == Kind::Close ? ""_str : "hello"_str;
        Message message { kind, Bytes::copy_from_slice(body.as_bytes()) };
        auto    output = [&]() -> Result<Outbound, SessionError> {
            if (kind != Kind::Close) return session.send(message);
            auto closing = session.close();
            if (closing.is_err()) return Err(closing.unwrap_err());
            auto control = session.next_control();
            if (control.is_err()) return Err(control.unwrap_err());
            return Ok(control->take().unwrap());
        }();
        if (output.is_err() ||
            (co_await rstd::async::io::write_all(socket, output->bytes)).is_err() ||
            session.written(output->ticket).is_err())
            co_return false;
        for (;;) {
            if (pending.is_empty()) {
                buffer.clear();
                auto read = co_await rstd::async::io::read(socket, buffer);
                if (read.is_err() || *read == usize()) co_return false;
                pending = Bytes::copy_from_slice(buffer.as_slice());
            }
            auto progress = session.receive(pending.as_slice());
            if (progress.is_err()) co_return false;
            pending.advance(progress->consumed);
            if (progress->message.is_none()) continue;
            auto expected = kind == Kind::Ping ? Kind::Pong : kind;
            if (progress->message->kind != expected ||
                progress->message->data.as_slice() != body.as_bytes())
                co_return false;
            break;
        }
    }
    auto                                  joined = co_await rstd::move(server);
    co_return joined.is_ok() && *joined&& session.state() == SessionState::Closed;
}
TEST(Roundtrip, AuthenticatedUpgradeTextPingAndClose) {
    auto listener =
        rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
    auto address = listener.local_addr().unwrap();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = rstd::async::AbortOnDropHandle { runtime.spawn(roundtrip(listener, address)) };
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::move(task), rstd::time::Duration::from_secs(u64(5))));
    ASSERT_TRUE(result.is_ok() && result->is_ok());
    EXPECT_TRUE(**result);
}

static auto transport_idle_read(bool expire) -> coro<bool> {
    auto listener =
        rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
    auto client = (co_await rstd::net::TcpStream::connect(listener.local_addr().unwrap())).unwrap();
    auto accepted = (co_await listener.accept()).unwrap();
    auto idle =
        expire ? Some(rstd::time::Duration::from_millis(u64(10))) : Option<rstd::time::Duration> {};
    auto transport = TcpTransport::make(UpgradedConnection { rstd::move(accepted.get<0>()), {} },
                                        rstd::time::Duration::from_millis(u64(5)),
                                        idle)
                         .unwrap();
    auto reading   = rstd::async::into_future(transport.read());
    auto waited    = co_await rstd::async::timeout(rstd::future::by_ref(reading),
                                                   rstd::time::Duration::from_millis(u64(50)));
    if (expire)
        co_return waited.is_ok() && waited->is_err() &&
            waited->unwrap_err() == WebSocketError::Timeout;
    if (waited.is_ok()) co_return false;
    transport.cancel();
    auto result = co_await rstd::future::by_ref(reading);
    co_return result.is_err() && result.unwrap_err() == WebSocketError::Cancelled;
}
TEST(WebSocketTransport, ExplicitIdleTimeoutExpiresRead) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result =
        runtime.block_on(rstd::async::timeout(rstd::async::into_future(transport_idle_read(true)),
                                              rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(WebSocketTransport, DefaultIdleReadRemainsCancellableAfterWriteDeadline) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result =
        runtime.block_on(rstd::async::timeout(rstd::async::into_future(transport_idle_read(false)),
                                              rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
#endif
