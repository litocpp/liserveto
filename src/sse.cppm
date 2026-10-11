export module liserveto:sse;
export import :response;
export import :shutdown;

using namespace rstd::prelude;
using namespace rstd::literals;

export namespace liserveto
{
using EventItem = Result<lihttpto::ServerEvent, BodyError>;
struct SseOptions {
    usize                        event_bytes { 4 * 1024 * 1024 };
    Option<rstd::time::Duration> idle_timeout, write_timeout;
};
} // namespace liserveto

template<rstd::stream::Source S>
struct SseStream {
    using Item = liserveto::BodyItem;
    S                    source;
    LifecycleWait<empty> stop;
    usize                limit;
    bool                 done {};
    auto                 poll_next(mut_ref<SseStream> self, rstd::task::Context& cx)
        -> rstd::task::Poll<Option<Item>> {
        using Poll = rstd::task::Poll<Option<Item>>;
        if (self->done) return Poll::Ready(None());
        if (rstd::future::poll(self->stop, cx).is_ready()) {
            self->done = true;
            return Poll::Ready(None());
        }
        auto next = rstd::future::poll_next(self->source, cx);
        if (next.is_pending()) return Poll::Pending();
        auto item = rstd::move(next).take();
        if (item.is_none()) {
            self->done = true;
            return Poll::Ready(None());
        }
        if (item->is_err()) {
            self->done = true;
            return Poll::Ready(Some(Item(Err(item->unwrap_err()))));
        }
        auto encoded = lihttpto::encode_event(**item, self->limit);
        if (encoded.is_err()) {
            self->done = true;
            return Poll::Ready(
                Some(Item(Err(encoded.unwrap_err() == lihttpto::EventStreamError::LimitExceeded
                                  ? liserveto::BodyError::LimitExceeded
                                  : liserveto::BodyError::Protocol))));
        }
        return Poll::Ready(Some(Item(Ok(lihttpto::BodyFrame::Data(rstd::move(encoded).unwrap())))));
    }
};

export namespace liserveto
{
template<rstd::stream::Source S>
    requires rstd::mtp::same_as<rstd::future::stream_item_t<S>, EventItem>
auto sse_response(S source, const Shutdown& shutdown, SseOptions options = {}) -> Response {
    auto response = Response::stream(Body::from_stream(
        SseStream<S> { rstd::move(source), shutdown.notified(), options.event_bytes }));
    response.head.headers.add("Content-Type"_str, "text/event-stream"_str).unwrap();
    response.head.headers.add("Cache-Control"_str, "no-cache"_str).unwrap();
    response.long_lived    = true;
    response.write_timeout = options.write_timeout;
    response.idle_timeout  = options.idle_timeout;
    return response;
}
} // namespace liserveto
