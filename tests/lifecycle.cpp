#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace liserveto;

auto broadcast_shutdown() -> rstd::async::coro<bool> {
    auto shutdown  = Shutdown::make().unwrap();
    auto abandoned = shutdown.notified();
    EXPECT_TRUE(
        (co_await rstd::async::timeout(rstd::move(abandoned), rstd::time::Duration {})).is_err());
    auto first  = rstd::async::spawn(shutdown.notified());
    auto second = rstd::async::spawn(shutdown.notified());
    co_await rstd::async::yield_now();
    shutdown.request_stop().unwrap();
    auto a    = co_await rstd::move(first);
    auto b    = co_await rstd::move(second);
    auto late = co_await shutdown.notified();
    co_return a.is_ok() && a->is_ok() && b.is_ok() && b->is_ok() && late.is_ok();
}
TEST(Lifecycle, ShutdownBroadcastsAndLateWaitersObservePersistentStop) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(rstd::async::timeout(
        rstd::async::into_future(broadcast_shutdown()), rstd::time::Duration::from_secs(u64(1))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
