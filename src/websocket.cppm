module;
export module liserveto:websocket;
#ifdef LITO_FEAT_WEBSOCKET
export import :connection;
export import :request;
export import :shutdown;
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
    Option<rstd::time::Duration>                      idle_timeout_;
    TcpTransport(UpgradedConnection           connection,
                 rstd::async::Notify          read,
                 rstd::async::Notify          write,
                 rstd::time::Duration         timeout,
                 Option<rstd::time::Duration> idle_timeout);

public:
    TcpTransport(TcpTransport&&) noexcept = default;
    static auto make(UpgradedConnection   connection,
                     rstd::time::Duration timeout = rstd::time::Duration::from_secs(u64(30)),
                     Option<rstd::time::Duration> idle_timeout = {})
        -> WebSocketResult<TcpTransport>;
    auto read() -> coro<WebSocketResult<Bytes>>;
    auto write(Bytes bytes) -> coro<WebSocketResult<usize>>;
    void cancel();
};
struct ServerConnectFailure {
    Option<HandshakeError>  handshake;
    Option<ConnectionError> upgrade;
    Option<WebSocketError>  connection;
};
class ServerConnection : public WebSocketConnection<TcpTransport> {
    using Driver = WebSocketConnection<TcpTransport>;
    explicit ServerConnection(Driver driver);

public:
    // Authenticate before calling accept; keep the connection inside the HTTP handler lifetime.
    static auto accept(Connection&            http,
                       const ServerHandshake& handshake,
                       WebSocketOptions       options    = {},
                       rstd::time::Duration   io_timeout = rstd::time::Duration::from_secs(u64(30)),
                       Option<rstd::time::Duration> idle_timeout = {})
        -> coro<Result<ServerConnection, ServerConnectFailure>>;
};
} // namespace liserveto

struct UpgradedTransport {
    liserveto::UpgradedStream    stream;
    liserveto::Shutdown          cancelled;
    rstd::time::Duration         timeout;
    Option<rstd::time::Duration> idle_timeout;
    auto                         read() -> coro<WebSocketResult<Bytes>>;
    auto                         write(Bytes bytes) -> coro<WebSocketResult<usize>>;
    void                         cancel();
};

export namespace liserveto
{
class WebSocket : public WebSocketConnection<UpgradedTransport> {
    using Driver = WebSocketConnection<UpgradedTransport>;
    rstd::sync::Arc<ConnectionCleanup> cleanup_;
    friend class WebSocketUpgrade;
    WebSocket(Driver driver, rstd::sync::Arc<ConnectionCleanup> cleanup);

public:
    WebSocket(WebSocket&&) noexcept;
    ~WebSocket();
};
class WebSocketUpgrade {
    UpgradePermit   permit_;
    ServerHandshake handshake_;
    Shutdown        shutdown_;
    RequestLifetime lifetime_;
    static auto     open_socket(UpgradedStream                     stream,
                                rstd::sync::Arc<ConnectionCleanup> cleanup,
                                const Option<String>&              protocol,
                                WebSocketOptions                   options,
                                rstd::time::Duration               io_timeout,
                                Option<rstd::time::Duration>       idle_timeout)
        -> Result<WebSocket, BodyError>;
    WebSocketUpgrade(UpgradePermit   permit,
                     ServerHandshake handshake,
                     Shutdown        shutdown,
                     RequestLifetime lifetime);

public:
    static auto from_request(const Request& request, Option<ref<str>> protocol = {})
        -> Result<WebSocketUpgrade, HandshakeError>;
    template<class H>
    // Idle reads are unbounded unless explicitly configured; writes retain their deadline.
    auto
    accept(H                            handler,
           WebSocketOptions             options      = {},
           rstd::time::Duration         io_timeout   = rstd::time::Duration::from_secs(u64(30)),
           Option<rstd::time::Duration> idle_timeout = {}) && -> Result<Response, HandshakeError> {
        if (io_timeout.is_zero() || (idle_timeout.is_some() && idle_timeout->is_zero()) ||
            options.receive_messages == usize() || options.receive_bytes == usize())
            return Err(HandshakeError::Upgrade());
        struct Action {
            H                            handler;
            WebSocketOptions             options;
            rstd::time::Duration         io_timeout;
            Option<rstd::time::Duration> idle_timeout;
            ServerHandshake              handshake;
            Shutdown                     shutdown;
            RequestLifetime              lifetime;
            auto                         validate_response(const lihttpto::ResponseHead& head) const
                -> Result<empty, BodyError> {
                if (handshake.validate_response_headers(head.headers).is_err())
                    return Err(BodyError::Protocol);
                return Ok(empty {});
            }
            auto operator()(UpgradedStream stream, rstd::sync::Arc<ConnectionCleanup> cleanup)
                -> coro<Result<empty, BodyError>> {
                auto opened = open_socket(rstd::move(stream),
                                          rstd::move(cleanup),
                                          handshake.protocol(),
                                          options,
                                          io_timeout,
                                          idle_timeout);
                if (opened.is_err()) co_return Err(opened.unwrap_err());
                auto& socket = *opened;
                auto  invoke = [&]() -> coro<Result<empty, BodyError>> {
                    co_return co_await handler(socket, shutdown.clone(), lifetime.clone());
                };
                auto pending  = Some(rstd::async::into_future(invoke()));
                auto selected = co_await rstd::async::select(shutdown.notified(),
                                                             rstd::future::by_ref(*pending));
                Result<empty, BodyError> result = Err(BodyError::Cancelled);
                if (selected.is_right())
                    result = rstd::move(selected).unwrap_right();
                else {
                    auto closing = socket.close();
                    auto waited  = co_await rstd::async::timeout(
                        rstd::async::join(rstd::future::by_ref(closing),
                                          rstd::future::by_ref(*pending)),
                        options.close_timeout);
                    if (waited.is_ok()) result = rstd::move(*waited).template get<1>();
                }
                pending = None();
                co_await socket.shutdown();
                co_return result;
            }
        };
        auto headers = handshake_.headers().clone();
        auto action =
            Action { rstd::move(handler),    options,           io_timeout,       idle_timeout,
                     rstd::move(handshake_), shutdown_.clone(), lifetime_.clone() };
        auto response =
            Response::on_upgrade(rstd::move(permit_),
                                 lihttpto::UpgradeProtocol::make("websocket"_str).unwrap(),
                                 rstd::move(action),
                                 headers);
        if (response.is_err()) return Err(HandshakeError::Upgrade());
        return Ok(rstd::move(response).unwrap());
    }
};
} // namespace liserveto
#endif
