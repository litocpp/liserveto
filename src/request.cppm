export module liserveto:request;
export import :incoming;
export import :shutdown;

using namespace rstd::prelude;

export namespace liserveto
{
class Router;
struct RouteParameter {
    String  name;
    Vec<u8> value;
};
class Request {
    friend class Router;
    usize                 path_offset_ {};
    Vec<RouteParameter>   parameters_;
    Option<IncomingBody>  body_;
    lihttpto::RequestHead head_;

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
