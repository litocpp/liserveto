#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;

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

enum class SseNetworkMode
{
    Shutdown,
    SourceError,
    WriteError,
    PeerClosed
};
struct SseNetworkState {
    Shutdown                pending;
    Option<RequestLifetime> lifetime;
    int                     released {}, notified {}, polls {};
    usize                   reads {}, pending_reads {};
    explicit SseNetworkState(Shutdown signal): pending(rstd::move(signal)) {}
};
struct SseNetworkEvents {
    using Item = EventItem;
    rstd::sync::Arc<SseNetworkState> state;
    bool                             error, sent {};
    SseNetworkEvents(rstd::sync::Arc<SseNetworkState> owner, bool fail)
        : state(rstd::move(owner)), error(fail) {}
    SseNetworkEvents(SseNetworkEvents&&) noexcept = default;
    ~SseNetworkEvents() {
        if (state) ++state->released;
    }
    auto poll_next(mut_ref<SseNetworkEvents> self, rstd::task::Context&)
        -> rstd::task::Poll<Option<Item>> {
        using Poll = rstd::task::Poll<Option<Item>>;
        ++self->state->polls;
        if (! rstd::exchange(self->sent, true))
            return Poll::Ready(
                Some(Item(Ok(lihttpto::ServerEvent { {}, String::make("one"_str), {} }))));
        if (self->error) return Poll::Ready(Some(Item(Err(BodyError::Source))));
        if (! self->state->pending.requested()) self->state->pending_reads = self->state->reads;
        self->state->pending.request_stop().unwrap();
        return Poll::Pending();
    }
};
struct SseNetworkIO {
    rstd::net::TcpStream             stream;
    rstd::sync::Arc<SseNetworkState> state;
    bool                             fail_write {}, wrote {};
    auto poll_read(mut_ref<SseNetworkIO> self, rstd::task::Context& cx, BytesMut& bytes)
        -> rstd::task::Poll<rstd::io::Result<usize>> {
        ++self->state->reads;
        return rstd::async::io::poll_read(self->stream, cx, bytes);
    }
    auto poll_write(mut_ref<SseNetworkIO> self, rstd::task::Context& cx, const Bytes& bytes)
        -> rstd::task::Poll<rstd::io::Result<usize>> {
        if (self->fail_write && self->wrote)
            return rstd::task::Poll<rstd::io::Result<usize>>::Ready(
                Err(rstd::io::error::Error::from_kind(
                    rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::BrokenPipe })));
        auto result = rstd::async::io::poll_write(self->stream, cx, bytes);
        if (result.is_ready() && result.value().is_ok() && *result.value() != usize())
            self->wrote = true;
        return result;
    }
    auto poll_flush(mut_ref<SseNetworkIO> self, rstd::task::Context& cx)
        -> rstd::task::Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_flush(self->stream, cx);
    }
    auto poll_shutdown(mut_ref<SseNetworkIO> self, rstd::task::Context& cx)
        -> rstd::task::Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_shutdown(self->stream, cx);
    }
};
auto sse_network(SseNetworkMode mode) -> rstd::async::coro<bool> {
    auto listener =
        rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
    auto address  = listener.local_addr().unwrap();
    auto stop     = Shutdown::make().unwrap();
    auto state    = rstd::sync::Arc<SseNetworkState>::make(Shutdown::make().unwrap());
    auto endpoint = Endpoint::make(
        [state = state.clone(), mode](Request request) -> rstd::async::coro<ResponseResult> {
            state->lifetime.insert(request.lifetime.clone());
            SseOptions options;
            if (mode == SseNetworkMode::PeerClosed)
                options.idle_timeout = Some(rstd::time::Duration::from_millis(u64(50)));
            auto response =
                sse_response(SseNetworkEvents(state.clone(), mode == SseNetworkMode::SourceError),
                             request.shutdown,
                             options);
            response.on_finish([state = state.clone(), mode](const ResponseReport& report) {
                EXPECT_EQ(state->released, 1);
                EXPECT_TRUE(state->lifetime->report().is_some());
                auto expected = mode == SseNetworkMode::Shutdown      ? ResponseOutcome::Complete
                                : mode == SseNetworkMode::SourceError ? ResponseOutcome::SourceError
                                : mode == SseNetworkMode::WriteError  ? ResponseOutcome::WriteError
                                                                      : ResponseOutcome::Timeout;
                EXPECT_EQ(report.outcome, expected);
                EXPECT_EQ(report.bytes, mode == SseNetworkMode::WriteError ? u64() : u64(11));
                ++state->notified;
            });
            co_return Ok(rstd::move(response));
        });
    auto accept = [&]() -> rstd::async::coro<bool> {
        auto pair = (co_await listener.accept()).unwrap();
        co_return co_await serve_connection(SseNetworkIO { rstd::move(pair.get<0>()),
                                                           state.clone(),
                                                           mode == SseNetworkMode::WriteError },
                                            rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                            stop.clone(),
                                            {});
    };
    auto task = rstd::async::AbortOnDropHandle(rstd::async::spawn(accept()));
    Option<rstd::net::TcpStream> client { Some(
        (co_await rstd::net::TcpStream::connect(address)).unwrap()) };
    if ((co_await rstd::async::io::write_all(
             *client,
             Bytes::copy_from_slice("GET /events HTTP/1.1\r\nHost: local\r\n\r\n"_str.as_bytes())))
            .is_err())
        co_return false;
    BytesMut output;
    bool     stopped = false;
    for (;;) {
        auto chunk = BytesMut::with_capacity(usize(4096));
        auto read  = co_await rstd::async::io::read(*client, chunk);
        if (read.is_err()) co_return false;
        if (*read == usize()) break;
        output.put_slice(chunk.as_slice());
        auto text = rstd::str_::from_utf8(output.as_slice()).unwrap();
        if (! stopped && text.contains("data: one\n\n"_str) &&
            (mode == SseNetworkMode::Shutdown || mode == SseNetworkMode::PeerClosed)) {
            if ((co_await state->pending.notified()).is_err()) co_return false;
            if (state->released != 0 || state->lifetime->report().is_some()) co_return false;
            stopped = true;
            if (mode == SseNetworkMode::PeerClosed) {
                client = None();
                break;
            }
            stop.request_stop().unwrap();
        }
    }
    if (mode == SseNetworkMode::PeerClosed) {
        auto report = co_await state->lifetime->finished();
        if (report.is_err() || report->outcome != ResponseOutcome::Timeout ||
            state->reads != state->pending_reads)
            co_return false;
    }
    auto joined = co_await rstd::move(task);
    if (joined.is_err() || *joined != (mode == SseNetworkMode::Shutdown) || state->released != 1 ||
        state->notified != 1 || state->lifetime->report().is_none())
        co_return false;
    auto text = rstd::str_::from_utf8(output.as_slice()).unwrap();
    if (! text.starts_with("HTTP/1.1 200 "_str) ||
        ! text.contains("Content-Type: text/event-stream\r\n"_str))
        co_return false;
    co_return text.ends_with("0\r\n\r\n"_str) == (mode == SseNetworkMode::Shutdown);
}
TEST(SseNetwork, ShutdownDrainsPendingSourceBeforeCompletionAndClosesTransport) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(sse_network(SseNetworkMode::Shutdown)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
TEST(SseNetwork, SourceAndWriteErrorsReleaseSourceAndLeaveTruncatedResponse) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (auto mode :
         array<SseNetworkMode, 2> { SseNetworkMode::SourceError, SseNetworkMode::WriteError }) {
        auto result = runtime.block_on(rstd::async::timeout(
            rstd::async::into_future(sse_network(mode)), rstd::time::Duration::from_secs(u64(2))));
        ASSERT_TRUE(result.is_ok());
        EXPECT_TRUE(*result);
    }
}
TEST(SseNetwork, IdlePeerCloseIsBoundedByConfiguredTimeoutWithoutReadingAhead) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(sse_network(SseNetworkMode::PeerClosed)),
                             rstd::time::Duration::from_secs(u64(2))));
    ASSERT_TRUE(result.is_ok());
    EXPECT_TRUE(*result);
}
