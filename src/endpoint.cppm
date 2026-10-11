export module liserveto:endpoint;
export import :request;
export import :response;

using namespace rstd::prelude;
using liserveto::Request;
using liserveto::ResponseResult;

struct EndpointCall {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = EndpointCall;
        auto call(Request request) const -> rstd::async::coro<ResponseResult> {
            return rstd::trait_call<0>(this, rstd::move(request));
        }
    };
    template<class S>
    using Funcs = TraitFuncs<&S::call>;
};
template<class H>
struct StoredEndpoint {
    H    handler;
    auto call(Request request) const -> rstd::async::coro<ResponseResult> {
        co_return co_await handler(rstd::move(request));
    }
};

auto invoke_endpoint(rstd::sync::Arc<dyn<EndpointCall>> owner, Request request)
    -> rstd::async::coro<ResponseResult> {
    co_return co_await owner->call(rstd::move(request));
}
auto missing_next() -> rstd::async::coro<ResponseResult> {
    co_return Err(liserveto::Error { liserveto::ErrorKind::Internal });
}

export namespace liserveto
{
class Endpoint {
    rstd::sync::Arc<dyn<EndpointCall>> handler_;
    explicit Endpoint(rstd::sync::Arc<dyn<EndpointCall>> handler): handler_(rstd::move(handler)) {}

public:
    template<class H>
        requires rstd::async::AwaitableInput<rstd::mtp::invoke_result_t<const H&, Request>> &&
                 rstd::mtp::same_as<
                     rstd::async::await_output_t<rstd::mtp::invoke_result_t<const H&, Request>>,
                     ResponseResult>
    static auto make(H handler) -> Endpoint {
        return Endpoint(
            rstd::sync::Arc<dyn<EndpointCall>>::make(StoredEndpoint<H> { rstd::move(handler) }));
    }
    auto clone() const -> Endpoint { return Endpoint(handler_.clone()); }
    auto call(Request request) const -> rstd::async::coro<ResponseResult> {
        return invoke_endpoint(handler_.clone(), rstd::move(request));
    }
};
class Next {
    Option<Endpoint> endpoint_;

public:
    explicit Next(Endpoint endpoint): endpoint_(Some(rstd::move(endpoint))) {}
    Next(const Next&) = delete;
    Next(Next&& other) noexcept: endpoint_(other.endpoint_.take()) {}
    auto call(Request request) -> rstd::async::coro<ResponseResult> {
        if (endpoint_.is_none()) return missing_next();
        auto endpoint = endpoint_.take().unwrap();
        return endpoint.call(rstd::move(request));
    }
};
template<class H>
auto middleware(Endpoint next, H function) -> Endpoint {
    return Endpoint::make([next = rstd::move(next), function = rstd::move(function)](
                              Request request) -> rstd::async::coro<ResponseResult> {
        co_return co_await function(rstd::move(request), Next(next.clone()));
    });
}
} // namespace liserveto
