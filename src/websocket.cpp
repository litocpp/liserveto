module liserveto;
#ifdef LITO_FEAT_WEBSOCKET
import :websocket;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liwebsocketo;
using rstd::async::coro;
using rstd::bytes::Bytes;

namespace
{
template<class F>
auto websocket_read_with_timeout(F reading, Option<rstd::time::Duration> idle_timeout)
    -> coro<WebSocketResult<Bytes>> {
    if (idle_timeout.is_none()) co_return co_await rstd::move(reading);
    auto result =
        co_await rstd::async::timeout(rstd::async::into_future(rstd::move(reading)), *idle_timeout);
    if (result.is_err()) co_return Err(WebSocketError::Timeout);
    co_return rstd::move(*result);
}
} // namespace

namespace liserveto
{
TcpTransport::TcpTransport(UpgradedConnection           connection,
                           rstd::async::Notify          read,
                           rstd::async::Notify          write,
                           rstd::time::Duration         timeout,
                           Option<rstd::time::Duration> idle_timeout)
    : stream_(rstd::move(connection.stream)),
      pending_(rstd::move(connection.pending)),
      cancel_read_(rstd::move(read)),
      cancel_write_(rstd::move(write)),
      cancelled_(rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>>::make(false)),
      timeout_(timeout),
      idle_timeout_(idle_timeout) {
}

auto TcpTransport::make(UpgradedConnection           connection,
                        rstd::time::Duration         timeout,
                        Option<rstd::time::Duration> idle_timeout)
    -> WebSocketResult<TcpTransport> {
    if (timeout.is_zero() || (idle_timeout.is_some() && idle_timeout->is_zero()))
        return Err(WebSocketError::Timeout);
    auto read  = rstd::async::Notify::make();
    auto write = rstd::async::Notify::make();
    if (read.is_err() || write.is_err()) return Err(WebSocketError::Transport);
    return Ok(TcpTransport(rstd::move(connection),
                           rstd::move(read).unwrap(),
                           rstd::move(write).unwrap(),
                           timeout,
                           idle_timeout));
}

auto TcpTransport::read() -> coro<WebSocketResult<Bytes>> {
    if (cancelled_->load()) co_return Err(WebSocketError::Cancelled);
    if (! pending_.is_empty()) co_return Ok(rstd::exchange(pending_, Bytes {}));
    auto reading = [&]() -> coro<WebSocketResult<Bytes>> {
        auto buffer   = rstd::bytes::BytesMut::with_capacity(usize(16384));
        auto selected = co_await rstd::async::select(rstd::async::io::read(stream_, buffer),
                                                     cancel_read_.notified());
        if (selected.is_right()) co_return Err(WebSocketError::Cancelled);
        if (rstd::move(selected).unwrap_left().is_err()) co_return Err(WebSocketError::Transport);
        co_return Ok(Bytes::copy_from_slice(buffer.as_slice()));
    };
    co_return co_await websocket_read_with_timeout(reading(), idle_timeout_);
}

auto TcpTransport::write(Bytes bytes) -> coro<WebSocketResult<usize>> {
    if (cancelled_->load()) co_return Err(WebSocketError::Cancelled);
    auto result = co_await rstd::async::timeout(
        rstd::async::select(rstd::async::io::write_all(stream_, bytes), cancel_write_.notified()),
        timeout_);
    if (result.is_err()) co_return Err(WebSocketError::Timeout);
    auto selected = rstd::move(result).unwrap();
    if (selected.is_right()) co_return Err(WebSocketError::Cancelled);
    if (rstd::move(selected).unwrap_left().is_err()) co_return Err(WebSocketError::Transport);
    co_return Ok(bytes.len());
}

void TcpTransport::cancel() {
    cancelled_->store(true);
    (void)cancel_read_.notifier().notify();
    (void)cancel_write_.notifier().notify();
}

ServerConnection::ServerConnection(Driver driver): Driver(rstd::move(driver)) {
}

auto ServerConnection::accept(Connection&                  http,
                              const ServerHandshake&       handshake,
                              WebSocketOptions             options,
                              rstd::time::Duration         io_timeout,
                              Option<rstd::time::Duration> idle_timeout)
    -> coro<Result<ServerConnection, ServerConnectFailure>> {
    if (io_timeout.is_zero() || (idle_timeout.is_some() && idle_timeout->is_zero()) ||
        options.receive_messages == usize() || options.receive_bytes == usize())
        co_return Err(ServerConnectFailure { .connection = Some(WebSocketError::Protocol) });
    auto prepared = http.prepare_upgrade(lihttpto::UpgradeProtocol::make("websocket"_str).unwrap(),
                                         handshake.headers());
    if (prepared.is_err())
        co_return Err(ServerConnectFailure { .handshake = Some(HandshakeError::Upgrade()) });
    auto upgraded = co_await http.upgrade(rstd::move(prepared).unwrap());
    if (upgraded.is_err())
        co_return Err(ServerConnectFailure { .upgrade = Some(rstd::move(upgraded).unwrap_err()) });
    auto wire = TcpTransport::make(rstd::move(upgraded).unwrap(), io_timeout, idle_timeout);
    if (wire.is_err())
        co_return Err(ServerConnectFailure { .connection = Some(wire.unwrap_err()) });
    auto driver = Driver::start(
        rstd::move(wire).unwrap(), Role::Server, options, handshake.protocol().clone());
    if (driver.is_err())
        co_return Err(ServerConnectFailure { .connection = Some(driver.unwrap_err()) });
    co_return Ok(ServerConnection(rstd::move(driver).unwrap()));
}
} // namespace liserveto

auto UpgradedTransport::read() -> coro<WebSocketResult<Bytes>> {
    if (cancelled.requested()) co_return Err(WebSocketError::Cancelled);
    auto reading = [&]() -> coro<WebSocketResult<Bytes>> {
        auto bytes    = BytesMut::with_capacity(usize(16384));
        auto selected = co_await rstd::async::select(cancelled.notified(),
                                                     rstd::async::io::read(stream, bytes));
        if (selected.is_left()) co_return Err(WebSocketError::Cancelled);
        if (rstd::move(selected).unwrap_right().is_err()) co_return Err(WebSocketError::Transport);
        co_return Ok(bytes.freeze());
    };
    co_return co_await websocket_read_with_timeout(reading(), idle_timeout);
}

auto UpgradedTransport::write(Bytes bytes) -> coro<WebSocketResult<usize>> {
    if (cancelled.requested()) co_return Err(WebSocketError::Cancelled);
    auto result = co_await rstd::async::timeout(
        rstd::async::select(cancelled.notified(), rstd::async::io::write_all(stream, bytes)),
        timeout);
    if (result.is_err()) co_return Err(WebSocketError::Timeout);
    auto selected = rstd::move(result).unwrap();
    if (selected.is_left()) co_return Err(WebSocketError::Cancelled);
    if (rstd::move(selected).unwrap_right().is_err()) co_return Err(WebSocketError::Transport);
    co_return Ok(bytes.len());
}

void UpgradedTransport::cancel() {
    (void)cancelled.request_stop();
}

namespace liserveto
{
WebSocket::WebSocket(Driver driver, rstd::sync::Arc<ConnectionCleanup> cleanup)
    : Driver(rstd::move(driver)), cleanup_(rstd::move(cleanup)) {
}

WebSocket::WebSocket(WebSocket&&) noexcept = default;
WebSocket::~WebSocket() {
    if (cleanup_) cleanup_->defer(rstd::move(static_cast<Driver&>(*this)).into_shutdown());
}

WebSocketUpgrade::WebSocketUpgrade(UpgradePermit   permit,
                                   ServerHandshake handshake,
                                   Shutdown        shutdown,
                                   RequestLifetime lifetime)
    : permit_(rstd::move(permit)),
      handshake_(rstd::move(handshake)),
      shutdown_(rstd::move(shutdown)),
      lifetime_(rstd::move(lifetime)) {
}

auto WebSocketUpgrade::from_request(const Request& request, Option<ref<str>> protocol)
    -> Result<WebSocketUpgrade, HandshakeError> {
    if (request.upgrade.is_none()) return Err(HandshakeError::Upgrade());
    auto handshake = ServerHandshake::parse(request.head(), protocol);
    if (handshake.is_err()) return Err(handshake.unwrap_err());
    return Ok(WebSocketUpgrade(request.upgrade->clone(),
                               rstd::move(handshake).unwrap(),
                               request.shutdown.clone(),
                               request.lifetime.clone()));
}

auto WebSocketUpgrade::open_socket(UpgradedStream                     stream,
                                   rstd::sync::Arc<ConnectionCleanup> cleanup,
                                   const Option<String>&              protocol,
                                   WebSocketOptions                   options,
                                   rstd::time::Duration               io_timeout,
                                   Option<rstd::time::Duration>       idle_timeout)
    -> Result<WebSocket, BodyError> {
    auto driver = WebSocketConnection<UpgradedTransport>::start(
        UpgradedTransport {
            rstd::move(stream), Shutdown::make().unwrap(), io_timeout, idle_timeout },
        Role::Server,
        options,
        protocol.clone());
    if (driver.is_err()) return Err(BodyError::Io);
    return Ok(WebSocket(rstd::move(driver).unwrap(), rstd::move(cleanup)));
}
} // namespace liserveto
#endif
