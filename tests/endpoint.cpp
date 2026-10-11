#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;

TEST(Endpoint, ResponseMovesObserverAndReportsDiscardExactlyOnce) {
    int calls = 0;
    {
        auto response = liserveto::Response::bytes(rstd::bytes::Bytes {});
        response.on_finish([&](const liserveto::ResponseReport& report) {
            EXPECT_EQ(report.outcome, liserveto::ResponseOutcome::Cancelled);
            ++calls;
        });
        auto moved = rstd::move(response);
    }
    EXPECT_EQ(calls, 1);
}
