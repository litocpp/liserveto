module;
#include <rstd/macro.hpp>
export module liserveto:shutdown;
export import rstd;
using namespace rstd::prelude;
using rstd::sync::Arc;
using AtomicBool = rstd::sync::atomic::Atomic<bool>;
using Ordering   = rstd::sync::atomic::Ordering;
using IoError    = rstd::io::error::Error;

namespace liserveto
{
struct ServerAccess;
struct ShutdownState {
    AtomicBool          requested { false };
    AtomicBool          used { false };
    rstd::async::Notify notification;
    explicit ShutdownState(rstd::async::Notify value): notification(rstd::move(value)) {}
};
} // namespace liserveto
export namespace liserveto
{
class Shutdown {
    friend struct ServerAccess;
    Arc<ShutdownState> state_;
    explicit Shutdown(Arc<ShutdownState> state): state_(rstd::move(state)) {}

public:
    static auto make() -> Result<Shutdown, IoError> {
        auto notification = rstd_try(rstd::async::Notify::make());
        return Ok(Shutdown { Arc<ShutdownState>::make(rstd::move(notification)) });
    }
    auto clone() const -> Shutdown { return Shutdown { state_.clone() }; }
    auto request_stop() const -> Result<empty, IoError> {
        state_->requested.store(true, Ordering::Release);
        return state_->notification.notifier().notify();
    }
    auto requested() const -> bool { return state_->requested.load(Ordering::Acquire); }
};
} // namespace liserveto
