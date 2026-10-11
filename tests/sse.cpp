#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;

TEST(Sse, EncodesOwnedEventsWithLimitsAndExplicitMetadata) {
    Vec<EventItem> events;
    events.push(Ok(lihttpto::ServerEvent {
        String::make("update"_str), String::make("one\r\ntwo\n"_str), String::make("7"_str) }));
    auto shutdown = Shutdown::make().unwrap();
    auto response = sse_response(rstd::stream::iter(rstd::move(events)), shutdown);
    EXPECT_TRUE(response.long_lived);
    EXPECT_TRUE(response.content_length.is_none());
    EXPECT_EQ(*response.head.headers.get_unique_text("Content-Type"_str).unwrap(),
              "text/event-stream"_str);
    auto runtime   = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto collected = runtime.block_on(response.body.collect(u64(256))).unwrap();
    EXPECT_EQ(collected.data.as_slice(),
              "event: update\nid: 7\ndata: one\ndata: two\ndata: \n\n"_str.as_bytes());
    lihttpto::ServerEvent invalid { String::make("bad\nevent"_str), {}, {} };
    EXPECT_EQ(lihttpto::encode_event(invalid).unwrap_err(),
              lihttpto::EventStreamError::InvalidField);
    EXPECT_EQ(lihttpto::encode_event({ {}, String::make("ok"_str), {} }, usize(1)).unwrap_err(),
              lihttpto::EventStreamError::LimitExceeded);
}
struct IdleEvents {
    using Item = EventItem;
    auto poll_next(mut_ref<IdleEvents>, rstd::task::Context&) -> rstd::task::Poll<Option<Item>> {
        return rstd::task::Poll<Option<Item>>::Pending();
    }
};
auto stopped_sse() -> rstd::async::coro<bool> {
    auto shutdown = Shutdown::make().unwrap();
    auto response = sse_response(IdleEvents {}, shutdown);
    auto next     = rstd::async::spawn(rstd::stream::next(response.body));
    co_await rstd::async::yield_now();
    shutdown.request_stop().unwrap();
    auto result = co_await rstd::move(next);
    co_return result.is_ok() && result->is_none();
}
TEST(Sse, PendingSourceEndsOnShutdownWithoutRecreatingReads) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(rstd::async::timeout(rstd::async::into_future(stopped_sse()),
                                                         rstd::time::Duration::from_secs(u64(1))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
