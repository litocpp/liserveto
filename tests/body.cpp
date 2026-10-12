#include <rstd/test/gtest.hpp>
import liserveto;

using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using lihttpto::BodyFrame;

auto chunk(ref<str> text) -> BodyItem {
    return Ok(BodyFrame::Data(rstd::bytes::Bytes::copy_from_slice(text.as_bytes())));
}
TEST(Body, CollectPreservesTrailersAndUsesActualByteLimit) {
    lihttpto::Headers fields;
    fields.add("X-End"_str, "yes"_str).unwrap();
    auto parts = Vec<BodyItem>::make();
    parts.push(chunk("ab"_str));
    parts.push(chunk("cd"_str));
    parts.push(Ok(BodyFrame::Trailers(rstd::move(fields))));
    auto body    = Body::from_stream(rstd::stream::iter(rstd::move(parts)));
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(body.collect(u64(4))).unwrap();
    EXPECT_EQ(result.data.as_slice(), "abcd"_str.as_bytes());
    EXPECT_EQ(*result.trailers.get_unique_text("x-end"_str).unwrap(), "yes"_str);
    auto limited = Body::from_bytes(rstd::bytes::Bytes::copy_from_slice("abc"_str.as_bytes()));
    EXPECT_EQ(runtime.block_on(limited.collect(u64(2))).unwrap_err(), BodyError::LimitExceeded);
}
TEST(Body, SourceErrorsAndFramesAfterTrailersCannotBecomeSuccessfulEof) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto failed  = Body::from_stream(rstd::stream::once(BodyItem(Err(BodyError::Source))));
    EXPECT_EQ(runtime.block_on(failed.collect(u64(4))).unwrap_err(), BodyError::Source);
    auto parts = Vec<BodyItem>::make();
    parts.push(Ok(BodyFrame::Trailers(lihttpto::Headers {})));
    parts.push(chunk("late"_str));
    auto invalid = Body::from_stream(rstd::stream::iter(rstd::move(parts)));
    EXPECT_EQ(runtime.block_on(invalid.collect(u64(4))).unwrap_err(), BodyError::Protocol);
    auto empty = Body {};
    EXPECT_TRUE(runtime.block_on(empty.collect(u64())).unwrap().data.is_empty());
}
struct PendingBody {
    using Item = BodyItem;
    int* polls;
    auto poll_next(mut_ref<PendingBody> self, rstd::task::Context& cx)
        -> rstd::task::Poll<Option<Item>> {
        if (++*self->polls == 1) {
            cx.waker().wake_by_ref();
            return rstd::task::Poll<Option<Item>>::Pending();
        }
        return rstd::task::Poll<Option<Item>>::Ready(Some(chunk("x"_str)));
    }
};
TEST(Body, BoxedSourceKeepsPendingStateAcrossWaitCancellationAndBodyMove) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  polls   = 0;
    auto body    = Body::from_stream(PendingBody { &polls });
    auto waited =
        runtime.block_on(rstd::async::timeout(rstd::stream::next(body), rstd::time::Duration {}));
    EXPECT_TRUE(waited.is_err());
    EXPECT_EQ(polls, 1);
    auto moved = rstd::move(body);
    auto next  = runtime.block_on(rstd::stream::next(moved));
    ASSERT_TRUE(next.is_some());
    EXPECT_EQ(next->unwrap().as_Data().bytes.as_slice(), "x"_str.as_bytes());
    EXPECT_EQ(polls, 2);
}

struct ClosingState {
    int  polls {}, releases {};
    bool ready {}, failed {};
};
struct ClosingBody {
    using Item = BodyItem;
    rstd::sync::Arc<ClosingState> state;
    ClosingBody(rstd::sync::Arc<ClosingState> value): state(rstd::move(value)) {}
    ClosingBody(ClosingBody&&) noexcept = default;
    ~ClosingBody() {
        if (state) ++state->releases;
    }
    auto poll_next(mut_ref<ClosingBody>, rstd::task::Context&) -> rstd::task::Poll<Option<Item>> {
        return rstd::task::Poll<Option<Item>>::Ready(None());
    }
    auto poll_close(mut_ref<ClosingBody> self, rstd::task::Context&)
        -> rstd::task::Poll<Result<empty, BodyError>> {
        ++self->state->polls;
        if (! self->state->ready) return rstd::task::Poll<Result<empty, BodyError>>::Pending();
        return rstd::task::Poll<Result<empty, BodyError>>::Ready(
            self->state->failed ? Result<empty, BodyError>(Err(BodyError::Io))
                                : Result<empty, BodyError>(Ok(empty {})));
    }
};
TEST(Body, EofWaitsForCloseAndCloseFailureIsReportedOnce) {
    auto state = rstd::sync::Arc<ClosingState>::make();
    auto body  = Body::from_stream(ClosingBody(state.clone()));
    auto cx    = rstd::task::Context(rstd::task::Waker::noop());
    EXPECT_TRUE(rstd::future::poll_next(body, cx).is_pending());
    EXPECT_EQ(state->releases, 0);
    state->ready  = true;
    state->failed = true;
    auto last     = rstd::move(rstd::future::poll_next(body, cx)).take();
    EXPECT_TRUE(last.is_some());
    if (last.is_some()) EXPECT_EQ(last->unwrap_err(), BodyError::Io);
    EXPECT_EQ(state->releases, 1);
    EXPECT_TRUE(rstd::move(rstd::future::poll_next(body, cx)).take().is_none());
}
TEST(Body, ResponseCloseRetainsWaitAcrossCancellationAndMove) {
    auto state    = rstd::sync::Arc<ClosingState>::make();
    auto response = Response::stream(Body::from_stream(ClosingBody(state.clone())));
    int  notified {};
    response.on_finish([&](const ResponseReport& report) {
        EXPECT_EQ(report.outcome, ResponseOutcome::SourceError);
        EXPECT_EQ(state->releases, 1);
        ++notified;
    });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto waiting = rstd::async::into_future(response.close({ ResponseOutcome::Complete }));
    EXPECT_TRUE(runtime.block_on(rstd::async::timeout(rstd::move(waiting), rstd::time::Duration {}))
                    .is_err());
    EXPECT_EQ(notified, 0);
    auto moved    = rstd::move(response);
    state->ready  = true;
    state->failed = true;
    auto result   = runtime.block_on(moved.close({ ResponseOutcome::Complete }));
    EXPECT_TRUE(result.is_err());
    EXPECT_EQ(notified, 1);
    EXPECT_TRUE(runtime.block_on(moved.close()).is_ok());
    EXPECT_EQ(notified, 1);
}
