module;
#include <rstd/macro.hpp>
export module liserveto:server;
export import :connection;
export import :endpoint;

using namespace rstd::prelude;
using namespace lihttpto;
using rstd::sync::Arc;
using AtomicBool  = rstd::sync::atomic::Atomic<bool>;
using Ordering    = rstd::sync::atomic::Ordering;
using IoError     = rstd::io::error::Error;
using IoErrorKind = rstd::io::error::ErrorKind;
using rstd::async::AbortOnDropHandle;

export namespace liserveto
{
struct ServerLimits {
    ConnectionLimits     connection;
    usize                connections { 128 };
    rstd::time::Duration shutdown_timeout { rstd::time::Duration::from_secs(u64(10)) };
};
struct ServerReport {
    usize accepted {};
    usize failed {};
    usize cancelled {};
};
} // namespace liserveto

namespace liserveto
{
struct ServerAccess {
    static auto notified(const Shutdown& shutdown) -> rstd::async::NotifyFuture {
        return shutdown.state_->notification.notified();
    }
    static auto claim(const Shutdown& shutdown) -> bool {
        return ! shutdown.state_->used.exchange(true, Ordering::AcqRel);
    }
};
struct StopOnExit {
    Shutdown shutdown;
    ~StopOnExit() { (void)shutdown.request_stop(); }
};
using ConnectionTasks = Vec<Option<AbortOnDropHandle<bool>>>;
struct ConnectionCompletion {
    bool success;
    bool cancelled;
};
struct NextConnection {
    using Output = ConnectionCompletion;
    ConnectionTasks* tasks;
    auto poll(mut_ref<NextConnection> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        for (auto& slot : *self->tasks) {
            if (slot.is_none()) continue;
            auto result = rstd::future::poll(*slot, cx);
            if (result.is_pending()) continue;
            auto joined = rstd::move(result).take();
            slot        = None();
            return rstd::task::Poll<Output>::Ready(
                Output { joined.is_ok() && *joined, joined.is_err() });
        }
        return rstd::task::Poll<Output>::Pending();
    }
};
void record_completion(ServerReport& report, ConnectionCompletion result) {
    if (result.cancelled)
        ++report.cancelled;
    else if (! result.success)
        ++report.failed;
}
export template<typename IO, typename Handler>
auto serve_connection(IO                            stream,
                      Arc<Handler>                  handler,
                      Shutdown                      shutdown,
                      ConnectionLimits              limits,
                      Option<rstd::net::SocketAddr> peer = {}) -> rstd::async::coro<bool> {
    auto connection = Arc<BasicConnection<IO>>::make(rstd::move(stream), limits);
    struct CloseOnExit {
        Arc<BasicConnection<IO>> connection;
        ~CloseOnExit() { connection->close(); }
    } close_on_exit { connection.clone() };
    while (! shutdown.requested()) {
        auto request = co_await connection->read_request();
        if (request.is_err()) co_return false;
        if (request->is_none()) co_return true;
        if (shutdown.requested()) co_return true;
        auto head = rstd::move(*request).unwrap();
        if constexpr (requires { handler->call(rstd::declval<Request>()); }) {
            auto incoming = IncomingBody::from_connection(connection.clone());
            auto scope    = incoming.scope();
            auto response = co_await handler->call(
                Request(rstd::move(head), rstd::move(incoming), shutdown.clone(), peer));
            scope.expire();
            if (response.is_err()) {
                liserveto::Response rejection;
                const auto&         error = response.unwrap_err();
                auto                status =
                    error.kind == ErrorKind::InvalidRequest ? u16(400)
                    : error.kind == ErrorKind::Body && error.body == Some(BodyError::LimitExceeded)
                        ? u16(413)
                        : u16(500);
                rejection.head.status = lihttpto::StatusCode::make(status).unwrap();
                rejection.close_after = true;
                if ((co_await connection->send(rstd::move(rejection))).is_err()) co_return false;
            } else if ((co_await connection->send(rstd::move(response).unwrap())).is_err())
                co_return false;
        } else {
            auto handled = co_await (*handler)(*connection, head, shutdown);
            if (handled.is_err()) co_return false;
        }
        if (connection->is_closed() || connection->is_upgraded()) co_return true;
        if (! connection->ready_for_request()) co_return false;
    }
    co_return true;
}
} // namespace liserveto

export namespace liserveto
{
// Handler and its shared state must support concurrent calls and cooperative
// cancellation.
// Upgraded IO must remain owned by the handler until it returns.
template<typename Handler>
auto serve(rstd::net::TcpListener listener,
           Arc<Handler>           handler,
           Shutdown               shutdown,
           ServerLimits           limits = {}) -> rstd::async::coro<Result<ServerReport, IoError>> {
    if (limits.connections == usize() || ! ServerAccess::claim(shutdown))
        co_return Err(IoError::from_kind(IoErrorKind { IoErrorKind::InvalidInput }));
    auto            listening = Option<rstd::net::TcpListener> { Some(rstd::move(listener)) };
    ConnectionTasks tasks;
    StopOnExit      stop_on_exit { shutdown.clone() };
    for (usize i {}; i < limits.connections; ++i) tasks.push(None());
    usize           active {};
    ServerReport    report;
    Option<IoError> failure;
    while (! shutdown.requested()) {
        if (active == limits.connections) {
            auto event = co_await rstd::async::select(ServerAccess::notified(shutdown),
                                                      NextConnection { &tasks });
            if (event.is_left()) {
                auto notified = rstd::move(event).unwrap_left();
                if (notified.is_err()) failure = Some(rstd::move(notified).unwrap_err());
                break;
            }
            record_completion(report, rstd::move(event).unwrap_right());
            --active;
            continue;
        }
        auto event = co_await rstd::async::select(
            ServerAccess::notified(shutdown),
            rstd::async::select(NextConnection { &tasks }, listening->readable()));
        if (event.is_left()) {
            auto notified = rstd::move(event).unwrap_left();
            if (notified.is_err()) failure = Some(rstd::move(notified).unwrap_err());
            break;
        }
        auto available = rstd::move(event).unwrap_right();
        if (available.is_left()) {
            record_completion(report, rstd::move(available).unwrap_left());
            --active;
            continue;
        }
        auto ready = rstd::move(available).unwrap_right();
        if (ready.is_err()) {
            failure = Some(rstd::move(ready).unwrap_err());
            break;
        }
        if (shutdown.requested()) break;
        auto accepted = listening->try_accept();
        if (accepted.is_err()) {
            auto error = rstd::move(accepted).unwrap_err();
            if (error.kind() == IoErrorKind { IoErrorKind::WouldBlock }) continue;
            failure = Some(rstd::move(error));
            break;
        }
        auto pair = rstd::move(accepted).unwrap();
        for (auto& slot : tasks) {
            if (slot.is_some()) continue;
            slot = Some(AbortOnDropHandle<bool> {
                rstd::async::spawn(serve_connection(rstd::move(pair.template get<0>()),
                                                    handler.clone(),
                                                    shutdown.clone(),
                                                    limits.connection,
                                                    Some(pair.template get<1>()))) });
            ++active;
            ++report.accepted;
            break;
        }
    }
    listening = None();
    (void)shutdown.request_stop();
    auto started = rstd::time::Instant::now();
    while (active != usize() && failure.is_none()) {
        auto elapsed = started.elapsed();
        if (elapsed >= limits.shutdown_timeout) break;
        auto completed = co_await rstd::async::timeout(NextConnection { &tasks },
                                                       limits.shutdown_timeout - elapsed);
        if (completed.is_err()) break;
        record_completion(report, *completed);
        --active;
    }
    for (auto& slot : tasks)
        if (slot.is_some()) slot->abort();
    while (active != usize()) {
        record_completion(report, co_await NextConnection { &tasks });
        --active;
    }
    if (failure.is_some()) co_return Err(rstd::move(*failure));
    co_return Ok(report);
}
} // namespace liserveto
