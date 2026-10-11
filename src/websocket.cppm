module;
#ifdef LITO_FEAT_WEBSOCKET
export module liserveto:websocket;
export import :connection;
export import liwebsocketo;
using namespace rstd::prelude;
using namespace liwebsocketo;
using rstd::async::coro;
using rstd::bytes::Bytes;

export namespace liserveto
{
class TcpTransport {
    rstd::net::TcpStream                              stream_;
    Bytes                                             pending_;
    rstd::async::Notify                               cancel_read_, cancel_write_;
    rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>> cancelled_;
    rstd::time::Duration                              timeout_;
    TcpTransport(UpgradedConnection   connection,
                 rstd::async::Notify  read,
                 rstd::async::Notify  write,
                 rstd::time::Duration timeout)
        : stream_(rstd::move(connection.stream)),
          pending_(rstd::move(connection.pending)),
          cancel_read_(rstd::move(read)),
          cancel_write_(rstd::move(write)),
          cancelled_(rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>>::make(false)),
          timeout_(timeout) {}

public:
    TcpTransport(TcpTransport&&) noexcept = default;
    static auto make(UpgradedConnection   connection,
                     rstd::time::Duration timeout = rstd::time::Duration::from_secs(u64(30)))
        -> WebSocketResult<TcpTransport> {
        if (timeout.is_zero()) return Err(WebSocketError::Timeout);
        auto read  = rstd::async::Notify::make();
        auto write = rstd::async::Notify::make();
        if (read.is_err() || write.is_err()) return Err(WebSocketError::Transport);
        return Ok(TcpTransport(rstd::move(connection),
                               rstd::move(read).unwrap(),
                               rstd::move(write).unwrap(),
                               timeout));
    }
    auto read() -> coro<WebSocketResult<Bytes>> {
        if (cancelled_->load()) co_return Err(WebSocketError::Cancelled);
        if (! pending_.is_empty()) co_return Ok(rstd::exchange(pending_, Bytes {}));
        auto buffer = rstd::bytes::BytesMut::with_capacity(usize(16384));
        auto result = co_await rstd::async::timeout(
            rstd::async::select(rstd::async::io::read(stream_, buffer), cancel_read_.notified()),
            timeout_);
        if (result.is_err()) co_return Err(WebSocketError::Timeout);
        auto selected = rstd::move(result).unwrap();
        if (selected.is_right()) co_return Err(WebSocketError::Cancelled);
        if (rstd::move(selected).unwrap_left().is_err()) co_return Err(WebSocketError::Transport);
        co_return Ok(Bytes::copy_from_slice(buffer.as_slice()));
    }
    auto write(Bytes bytes) -> coro<WebSocketResult<usize>> {
        if (cancelled_->load()) co_return Err(WebSocketError::Cancelled);
        auto result = co_await rstd::async::timeout(
            rstd::async::select(rstd::async::io::write_all(stream_, bytes),
                                cancel_write_.notified()),
            timeout_);
        if (result.is_err()) co_return Err(WebSocketError::Timeout);
        auto selected = rstd::move(result).unwrap();
        if (selected.is_right()) co_return Err(WebSocketError::Cancelled);
        if (rstd::move(selected).unwrap_left().is_err()) co_return Err(WebSocketError::Transport);
        co_return Ok(bytes.len());
    }
    void cancel() {
        cancelled_->store(true);
        (void)cancel_read_.notifier().notify();
        (void)cancel_write_.notifier().notify();
    }
};
struct ServerConnectFailure {
    Option<HandshakeError>  handshake;
    Option<ConnectionError> upgrade;
    Option<WebSocketError>  connection;
};
class ServerConnection : public WebSocketConnection<TcpTransport> {
    using Driver = WebSocketConnection<TcpTransport>;
    explicit ServerConnection(Driver driver): Driver(rstd::move(driver)) {}

public:
    // Authenticate before calling accept; keep the connection inside the HTTP handler lifetime.
    static auto accept(Connection&            http,
                       const ServerHandshake& handshake,
                       WebSocketOptions       options    = {},
                       rstd::time::Duration   io_timeout = rstd::time::Duration::from_secs(u64(30)))
        -> coro<Result<ServerConnection, ServerConnectFailure>> {
        if (io_timeout.is_zero() || options.receive_messages == usize() ||
            options.receive_bytes == usize())
            co_return Err(ServerConnectFailure { .connection = Some(WebSocketError::Protocol) });
        auto prepared = http.prepare_upgrade(
            lihttpto::UpgradeProtocol::make("websocket"_str).unwrap(), handshake.headers());
        if (prepared.is_err())
            co_return Err(ServerConnectFailure { .handshake = Some(HandshakeError::Upgrade) });
        auto upgraded = co_await http.upgrade(rstd::move(prepared).unwrap());
        if (upgraded.is_err())
            co_return Err(
                ServerConnectFailure { .upgrade = Some(rstd::move(upgraded).unwrap_err()) });
        auto wire = TcpTransport::make(rstd::move(upgraded).unwrap(), io_timeout);
        if (wire.is_err())
            co_return Err(ServerConnectFailure { .connection = Some(wire.unwrap_err()) });
        auto driver = Driver::start(
            rstd::move(wire).unwrap(), Role::Server, options, handshake.protocol().clone());
        if (driver.is_err())
            co_return Err(ServerConnectFailure { .connection = Some(driver.unwrap_err()) });
        co_return Ok(ServerConnection(rstd::move(driver).unwrap()));
    }
};
} // namespace liserveto
#endif
