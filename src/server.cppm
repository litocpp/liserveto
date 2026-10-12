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
    static auto notified(const Shutdown& shutdown) -> LifecycleWait<empty> {
        return shutdown.notified();
    }
    static auto claim(const Shutdown& shutdown) -> bool {
        return ! shutdown.state_->used.exchange(true, Ordering::AcqRel);
    }
};
struct StopOnExit {
    Shutdown shutdown;
    ~StopOnExit() { (void)shutdown.request_stop(); }
};
struct ConnectionCompletion {
    bool success;
    bool cancelled;
};
void record_completion(ServerReport& report, ConnectionCompletion result) {
    if (result.cancelled)
        ++report.cancelled;
    else if (! result.success)
        ++report.failed;
}
template<typename IO, typename Handler>
auto dispatch_connection(IO                            stream,
                         Arc<Handler>                  handler,
                         Shutdown                      shutdown,
                         ConnectionLimits              limits,
                         Option<rstd::net::SocketAddr> peer,
                         Arc<ConnectionCleanup>        cleanup) -> rstd::async::coro<bool> {
    auto connection = Arc<BasicConnection<IO>>::make(rstd::move(stream), limits);
    struct CloseOnExit {
        Arc<BasicConnection<IO>> connection;
        ~CloseOnExit() { connection->close(); }
    } close_on_exit { connection.clone() };
    while (! shutdown.requested()) {
        auto received = co_await rstd::async::select(
            shutdown.notified(), rstd::async::into_future(connection->read_request()));
        if (received.is_left()) co_return true;
        auto request = rstd::move(received).unwrap_right();
        if (request.is_err()) co_return false;
        if (request->is_none()) co_return true;
        if (shutdown.requested()) co_return true;
        auto head = rstd::move(*request).unwrap();
        if constexpr (requires { handler->call(rstd::declval<Request>()); }) {
            RequestCompletion completion(cleanup.clone());
            auto              incoming         = IncomingBody::from_connection(connection.clone());
            auto              scope            = incoming.scope();
            auto              incoming_request = Request(rstd::move(head),
                                                         rstd::move(incoming),
                                                         shutdown.clone(),
                                                         peer,
                                                         completion.lifetime.clone(),
                                                         connection->upgrade_permit());
            completion.attach(incoming_request);
            auto response = co_await handler->call(rstd::move(incoming_request));
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
                completion.attach(rejection);
                if ((co_await connection->send(rstd::move(rejection), cleanup.clone())).is_err())
                    co_return false;
            } else {
                completion.attach(*response);
                if ((co_await connection->send(rstd::move(response).unwrap(), cleanup.clone()))
                        .is_err())
                    co_return false;
            }
        } else {
            auto handled = co_await (*handler)(*connection, head, shutdown);
            if (handled.is_err()) co_return false;
        }
        if (connection->is_closed() || connection->is_upgraded()) co_return true;
        if (! connection->ready_for_request()) co_return false;
    }
    co_return true;
}
template<typename IO, typename Handler>
auto run_connection(IO                            stream,
                    Arc<Handler>                  handler,
                    Shutdown                      shutdown,
                    ConnectionLimits              limits,
                    Option<rstd::net::SocketAddr> peer,
                    Arc<ConnectionCleanup>        cleanup) -> rstd::async::coro<bool> {
    auto result = co_await dispatch_connection(rstd::move(stream),
                                               rstd::move(handler),
                                               rstd::move(shutdown),
                                               limits,
                                               peer,
                                               cleanup.clone());
    co_await ConnectionCleanupWait { rstd::move(cleanup) };
    co_return result;
}

// Forced cancellation needs an external ConnectionTask owner to await cleanup.
export template<typename IO, typename Handler>
auto serve_connection(IO                            stream,
                      Arc<Handler>                  handler,
                      Shutdown                      shutdown,
                      ConnectionLimits              limits,
                      Option<rstd::net::SocketAddr> peer = {}) -> rstd::async::coro<bool> {
    return run_connection(rstd::move(stream),
                          rstd::move(handler),
                          rstd::move(shutdown),
                          limits,
                          peer,
                          Arc<ConnectionCleanup>::make());
}

export class ConnectionTask {
    Option<AbortOnDropHandle<bool>>              task_;
    Arc<ConnectionCleanup>                       cleanup_;
    Option<Result<bool, rstd::async::JoinError>> result_;

    ConnectionTask(rstd::async::JoinHandle<bool> task, Arc<ConnectionCleanup> cleanup)
        : task_(Some(AbortOnDropHandle<bool>(rstd::move(task)))), cleanup_(rstd::move(cleanup)) {}

public:
    using Output                                             = Result<bool, rstd::async::JoinError>;
    ConnectionTask(const ConnectionTask&)                    = delete;
    auto operator=(const ConnectionTask&) -> ConnectionTask& = delete;
    ConnectionTask(ConnectionTask&&) noexcept                = default;
    auto operator=(ConnectionTask&&) noexcept -> ConnectionTask& = default;

    template<typename IO, typename Handler>
    static auto spawn(IO                            stream,
                      Arc<Handler>                  handler,
                      Shutdown                      shutdown,
                      ConnectionLimits              limits = {},
                      Option<rstd::net::SocketAddr> peer   = {}) -> ConnectionTask {
        auto cleanup = Arc<ConnectionCleanup>::make();
        auto task    = rstd::async::spawn(run_connection(rstd::move(stream),
                                                         rstd::move(handler),
                                                         rstd::move(shutdown),
                                                         limits,
                                                         peer,
                                                         cleanup.clone()));
        return ConnectionTask(rstd::move(task), rstd::move(cleanup));
    }
    // Request cancellation; keep this owner until its Future completes.
    void abort() const {
        if (task_.is_some()) task_->abort();
    }
    auto poll(mut_ref<ConnectionTask> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        if (self->task_.is_some()) {
            auto joined = rstd::future::poll(*self->task_, cx);
            if (joined.is_pending()) return rstd::task::Poll<Output>::Pending();
            self->result_ = Some(rstd::move(joined).take());
            self->task_   = None();
        }
        if (self->result_.is_none()) rstd::panic { "ConnectionTask polled after completion" };
        if (self->cleanup_->poll(cx).is_pending()) return rstd::task::Poll<Output>::Pending();
        return rstd::task::Poll<Output>::Ready(self->result_.take().unwrap());
    }
};

using ConnectionTasks = Vec<Option<ConnectionTask>>;
struct NextConnection {
    using Output = ConnectionCompletion;
    ConnectionTasks* tasks;
    auto poll(mut_ref<NextConnection> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        for (auto& slot : *self->tasks) {
            if (slot.is_none()) continue;
            auto result = rstd::future::poll(*slot, cx);
            if (result.is_pending()) continue;
            auto joined    = rstd::move(result).take();
            auto completed = Output { joined.is_ok() && *joined, joined.is_err() };
            slot           = None();
            return rstd::task::Poll<Output>::Ready(completed);
        }
        return rstd::task::Poll<Output>::Pending();
    }
};
} // namespace liserveto

export namespace liserveto
{
// Shared handlers must support concurrent calls. Request shutdown and await serve
// before releasing borrowed application state; cancellation alone does not join.
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
                if (notified.is_err())
                    failure = Some(IoError::from_kind(IoErrorKind { IoErrorKind::Interrupted }));
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
            if (notified.is_err())
                failure = Some(IoError::from_kind(IoErrorKind { IoErrorKind::Interrupted }));
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
            slot = Some(ConnectionTask::spawn(rstd::move(pair.template get<0>()),
                                              handler.clone(),
                                              shutdown.clone(),
                                              limits.connection,
                                              Some(pair.template get<1>())));
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
