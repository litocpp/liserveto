export module liserveto:lifecycle;
export import rstd;
import :cleanup;

using namespace rstd::prelude;
using rstd::sync::Arc;
using rstd::sync::Weak;

template<class T>
struct LifecycleEventState {
    using Sender = rstd::async::oneshot::Sender<T>;
    struct Fields {
        Option<T>         value;
        Vec<Weak<Sender>> waiters;
    };
    rstd::sync::Mutex<Fields> fields { Fields {} };
};
template<class T>
class LifecycleWait {
    Arc<LifecycleEventState<T>>          state_;
    Arc<rstd::async::oneshot::Sender<T>> sender_;
    rstd::async::oneshot::Receiver<T>    receiver_;

public:
    using Output = Result<T, rstd::async::oneshot::Canceled>;
    LifecycleWait(Arc<LifecycleEventState<T>>          state,
                  Arc<rstd::async::oneshot::Sender<T>> sender,
                  rstd::async::oneshot::Receiver<T>    receiver)
        : state_(rstd::move(state)), sender_(rstd::move(sender)), receiver_(rstd::move(receiver)) {}
    auto poll(mut_ref<LifecycleWait> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        return rstd::future::poll(self->receiver_, cx);
    }
};
template<class T>
class LifecycleEvent {
    Arc<LifecycleEventState<T>> state_ { Arc<LifecycleEventState<T>>::make() };
    explicit LifecycleEvent(Arc<LifecycleEventState<T>> state): state_(rstd::move(state)) {}

public:
    LifecycleEvent() = default;
    auto clone() const -> LifecycleEvent { return LifecycleEvent(state_.clone()); }
    auto value() const -> Option<T> { return state_->fields.lock().unwrap()->value; }
    auto wait() const -> LifecycleWait<T> {
        auto pair = rstd::async::oneshot::channel<T>();
        auto sender =
            Arc<rstd::async::oneshot::Sender<T>>::make(rstd::move(pair.template get<0>()));
        Option<T> ready;
        {
            auto fields = state_->fields.lock().unwrap();
            ready       = fields->value;
            if (ready.is_none()) {
                fields->waiters.retain([](const auto& waiter) {
                    return bool(waiter.upgrade());
                });
                fields->waiters.push(sender.downgrade());
            }
        }
        if (ready.is_some()) (void)sender->send(*ready);
        return LifecycleWait<T>(
            state_.clone(), rstd::move(sender), rstd::move(pair.template get<1>()));
    }
    void finish(T value) const {
        Vec<Weak<rstd::async::oneshot::Sender<T>>> waiters;
        {
            auto fields = state_->fields.lock().unwrap();
            if (fields->value.is_some()) return;
            fields->value = Some(value);
            waiters       = rstd::move(fields->waiters);
        }
        for (auto& waiter : waiters) {
            auto sender = waiter.upgrade();
            if (sender) (void)sender->send(value);
        }
    }
};

struct RequestCompletion;
export namespace liserveto
{
class Response;
enum class ResponseOutcome
{
    Complete,
    SourceError,
    WriteError,
    Cancelled,
    Timeout
};
struct ResponseReport {
    ResponseOutcome      outcome;
    u64                  bytes {};
    rstd::time::Duration elapsed;
};
class RequestLifetime {
    friend struct ::RequestCompletion;
    friend class Response;
    LifecycleEvent<ResponseReport> event_;
    explicit RequestLifetime(LifecycleEvent<ResponseReport> event): event_(rstd::move(event)) {}

public:
    RequestLifetime() = default;
    auto clone() const -> RequestLifetime { return RequestLifetime(event_.clone()); }
    auto report() const -> Option<ResponseReport> { return event_.value(); }
    auto finished() const -> LifecycleWait<ResponseReport> { return event_.wait(); }
};
} // namespace liserveto

struct RequestCompletion {
    liserveto::RequestLifetime lifetime;
    rstd::time::Instant        started { rstd::time::Instant::now() };
    Arc<ConnectionCleanup>     cleanup;
    explicit RequestCompletion(Arc<ConnectionCleanup> owner): cleanup(rstd::move(owner)) {}
    void attach(liserveto::Response& response);
    ~RequestCompletion() {
        cleanup->complete([lifetime = lifetime.clone(), elapsed = started.elapsed()] {
            lifetime.event_.finish({ liserveto::ResponseOutcome::Cancelled, {}, elapsed });
        });
    }
};
