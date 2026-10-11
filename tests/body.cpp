#include <rstd/test/gtest.hpp>
import libserveto;

using namespace rstd::prelude;
using namespace rstd::literals;
using namespace libserveto;
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
