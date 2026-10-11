module;
#include <rstd/macro.hpp>
export module liserveto:shutdown;
export import :lifecycle;
using namespace rstd::prelude;
using rstd::sync::Arc;
using AtomicBool = rstd::sync::atomic::Atomic<bool>;
using Ordering   = rstd::sync::atomic::Ordering;
using IoError    = rstd::io::error::Error;

namespace liserveto
{
struct ServerAccess;
struct ShutdownState {
    AtomicBool            used { false };
    LifecycleEvent<empty> notification;
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
        return Ok(Shutdown { Arc<ShutdownState>::make() });
    }
    auto clone() const -> Shutdown { return Shutdown { state_.clone() }; }
    auto request_stop() const -> Result<empty, IoError> {
        state_->notification.finish(empty {});
        return Ok(empty {});
    }
    auto requested() const -> bool { return state_->notification.value().is_some(); }
    auto notified() const -> LifecycleWait<empty> { return state_->notification.wait(); }
};
} // namespace liserveto
