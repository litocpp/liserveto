export module liserveto:request;
export import :incoming;
export import :shutdown;
import :cleanup;

using namespace rstd::prelude;

template<class T>
struct DiscardBlockingResult {
    using Output = empty;
    rstd::async::JoinHandle<T> handle;
    auto                       poll(mut_ref<DiscardBlockingResult> self, rstd::task::Context& cx)
        -> rstd::task::Poll<empty> {
        auto result = rstd::future::poll(self->handle, cx);
        if (result.is_pending()) return rstd::task::Poll<empty>::Pending();
        // Destroy unclaimed resources before the cleanup owner publishes completion.
        {
            auto discarded = rstd::move(result).take();
        }
        return rstd::task::Poll<empty>::Ready(empty {});
    }
};
template<class T>
class RequestBlockingJob {
    Option<rstd::async::JoinHandle<T>> handle_;
    rstd::sync::Arc<ConnectionCleanup> cleanup_;

public:
    using Output = typename rstd::async::JoinHandle<T>::Output;
    RequestBlockingJob(rstd::async::JoinHandle<T>         handle,
                       rstd::sync::Arc<ConnectionCleanup> cleanup)
        : handle_(Some(rstd::move(handle))), cleanup_(rstd::move(cleanup)) {}
    RequestBlockingJob(const RequestBlockingJob&) = delete;
    RequestBlockingJob(RequestBlockingJob&& other) noexcept
        : handle_(other.handle_.take()), cleanup_(rstd::move(other.cleanup_)) {}
    ~RequestBlockingJob() {
        if (handle_.is_none()) return;
        handle_->abort();
        cleanup_->defer(DiscardBlockingResult<T> { handle_.take().unwrap() });
    }
    auto poll(mut_ref<RequestBlockingJob> self, rstd::task::Context& cx)
        -> rstd::task::Poll<Output> {
        auto result = rstd::future::poll(*self->handle_, cx);
        if (result.is_ready()) self->handle_ = None();
        return result;
    }
};
template<class F>
auto run_request_blocking(Option<rstd::sync::Arc<ConnectionCleanup>> cleanup, F function)
    -> rstd::async::coro<rstd::io::Result<rstd::mtp::void_empty_t<rstd::mtp::invoke_result_t<F>>>> {
    using T = rstd::mtp::invoke_result_t<F>;
    if (cleanup.is_none())
        co_return Err(rstd::io::error::Error::from_kind(
            rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::NotConnected }));
    auto submitted = rstd::async::spawn_blocking(rstd::move(function));
    if (submitted.is_err()) co_return Err(rstd::move(submitted).unwrap_err());
    auto job    = RequestBlockingJob<T>(rstd::move(submitted).unwrap(), cleanup.take().unwrap());
    auto result = co_await rstd::future::by_ref(job);
    if (result.is_err())
        co_return Err(rstd::io::error::Error::from_kind(
            rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::Interrupted }));
    co_return Ok(rstd::move(result).unwrap());
}

export namespace liserveto
{
class Router;
struct RouteParameter {
    String  name;
    Vec<u8> value;
};
class Request {
    friend class Router;
    friend struct ::RequestCompletion;
    Option<rstd::sync::Weak<ConnectionCleanup>> cleanup_;
    usize                                       path_offset_ {};
    Vec<RouteParameter>                         parameters_;
    Option<IncomingBody>                        body_;
    lihttpto::RequestHead                       head_;

public:
    Shutdown                      shutdown;
    RequestLifetime               lifetime;
    Option<UpgradePermit>         upgrade;
    Option<rstd::net::SocketAddr> peer;
    Request(lihttpto::RequestHead         head,
            IncomingBody                  body,
            Shutdown                      stop,
            Option<rstd::net::SocketAddr> peer     = {},
            RequestLifetime               lifetime = {},
            Option<UpgradePermit>         upgrade  = {})
        : body_(Some(rstd::move(body))),
          head_(rstd::move(head)),
          shutdown(rstd::move(stop)),
          lifetime(rstd::move(lifetime)),
          upgrade(rstd::move(upgrade)),
          peer(peer) {}
    Request(const Request&)                        = delete;
    Request(Request&&) noexcept                    = default;
    auto operator=(Request&&) noexcept -> Request& = default;
    auto take_body() -> Result<IncomingBody, BodyError> {
        if (body_.is_none()) return Err(BodyError::Protocol);
        return Ok(body_.take().unwrap());
    }
    auto collect(u64 limit) -> rstd::async::coro<Result<CollectedBody, BodyError>> {
        auto body = take_body();
        if (body.is_err()) co_return Err(body.unwrap_err());
        co_return co_await body->collect(limit);
    }
    auto head() const -> const lihttpto::RequestHead& { return head_; }
    // The closure must own its inputs; connection cleanup waits for cancelled blocking work.
    template<class F>
    auto run_blocking(F function) const -> rstd::async::coro<
        rstd::io::Result<rstd::mtp::void_empty_t<rstd::mtp::invoke_result_t<F>>>> {
        Option<rstd::sync::Arc<ConnectionCleanup>> owner;
        if (cleanup_.is_some()) {
            auto upgraded = cleanup_->upgrade();
            if (upgraded) owner.insert(rstd::move(upgraded));
        }
        return run_request_blocking(rstd::move(owner), rstd::move(function));
    }
    auto target() const -> ref<str> { return head_.line.target.as_str(); }
    auto path_size() const -> usize {
        auto count = head_.line.resource.segments.len() - path_offset_;
        return count == usize(1) && head_.line.resource.segments[path_offset_].is_empty() ? usize()
                                                                                          : count;
    }
    auto path_segment(usize index) const -> slice<u8> {
        return head_.line.resource.segments[path_offset_ + index].as_slice();
    }
    auto parameter(ref<str> name) const -> Option<slice<u8>> {
        for (const auto& item : parameters_)
            if (item.name == name) return Some(item.value.as_slice());
        return None();
    }
};
} // namespace liserveto

void RequestCompletion::attach(liserveto::Request& request) {
    request.cleanup_ = Some(cleanup.downgrade());
}
