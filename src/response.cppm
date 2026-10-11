export module liserveto:response;
export import :body;
export import :lifecycle;
export import :upgrade;

using namespace rstd::prelude;
using rstd::bytes::Bytes;

struct ResponseObserver {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = ResponseObserver;
        void finish(const liserveto::ResponseReport& report) { rstd::trait_call<0>(this, report); }
    };
    template<class S>
    using Funcs = TraitFuncs<&S::finish>;
};
template<class F>
struct StoredResponseObserver {
    F    function;
    void finish(const liserveto::ResponseReport& report) { function(report); }
};

export namespace liserveto
{
class Response {
    template<class>
    friend class BasicConnection;
    friend struct ::RequestCompletion;
    Option<Box<dyn<ResponseObserver>>>         observer_;
    Option<RequestLifetime>                    lifetime_;
    Option<UpgradePlan>                        upgrade_;
    Option<rstd::sync::Arc<ConnectionCleanup>> cleanup_;

public:
    lihttpto::ResponseHead       head { lihttpto::StatusCode::make(u16(200)).unwrap(), {}, {}, {} };
    Body                         body;
    Option<u64>                  content_length { Some(u64()) };
    bool                         close_after {};
    bool                         long_lived {};
    Option<rstd::time::Duration> write_timeout, idle_timeout;

    Response()                                   = default;
    Response(const Response&)                    = delete;
    auto operator=(const Response&) -> Response& = delete;
    Response(Response&& other) noexcept
        : observer_(other.observer_.take()),
          lifetime_(other.lifetime_.take()),
          upgrade_(other.upgrade_.take()),
          cleanup_(other.cleanup_.take()),
          head(rstd::move(other.head)),
          body(rstd::move(other.body)),
          content_length(other.content_length),
          close_after(other.close_after),
          long_lived(other.long_lived),
          write_timeout(other.write_timeout),
          idle_timeout(other.idle_timeout) {}
    auto operator=(Response&& other) noexcept -> Response& {
        if (this == &other) return *this;
        finish({ ResponseOutcome::Cancelled });
        observer_      = other.observer_.take();
        lifetime_      = other.lifetime_.take();
        upgrade_       = other.upgrade_.take();
        cleanup_       = other.cleanup_.take();
        head           = rstd::move(other.head);
        body           = rstd::move(other.body);
        content_length = other.content_length;
        close_after    = other.close_after;
        long_lived     = other.long_lived;
        write_timeout  = other.write_timeout;
        idle_timeout   = other.idle_timeout;
        return *this;
    }
    ~Response() { finish({ ResponseOutcome::Cancelled }); }
    static auto bytes(Bytes bytes) -> Response {
        Response result;
        result.content_length = Some(u64(bytes.len().to_primitive()));
        result.body           = Body::from_bytes(rstd::move(bytes));
        return result;
    }
    static auto stream(Body body, Option<u64> length = {}) -> Response {
        Response result;
        result.body           = rstd::move(body);
        result.content_length = length;
        return result;
    }
    template<class F>
    void on_finish(F function) {
        if (observer_.is_some()) {
            auto observer = observer_.take().unwrap();
            observer->finish({ ResponseOutcome::Cancelled });
        }
        observer_ = Some(
            Box<dyn<ResponseObserver>>::make(StoredResponseObserver<F> { rstd::move(function) }));
    }
    template<class H>
    static auto on_upgrade(UpgradePermit             permit,
                           lihttpto::UpgradeProtocol protocol,
                           H                         handler,
                           const lihttpto::Headers&  headers = {})
        -> Result<Response, lihttpto::UpgradeError> {
        auto head = permit.offer_->response_head(protocol, headers);
        if (head.is_err()) return Err(head.unwrap_err());
        Response result;
        result.head           = rstd::move(head).unwrap();
        result.content_length = None();
        result.upgrade_       = Some(UpgradePlan {
            rstd::move(permit),
            rstd::move(protocol),
            Box<dyn<UpgradeAction>>::make(StoredUpgradeAction<H> { rstd::move(handler) }) });
        return Ok(rstd::move(result));
    }
    void finish(ResponseReport report) {
        body     = Body {};
        upgrade_ = None();
        if (lifetime_.is_none() && observer_.is_none()) {
            cleanup_ = None();
            return;
        }
        auto complete =
            [lifetime = lifetime_.take(), observer = observer_.take(), report]() mutable {
                if (lifetime.is_some()) lifetime->event_.finish(report);
                if (observer.is_some()) (*observer)->finish(report);
            };
        if (cleanup_.is_some())
            cleanup_.take().unwrap()->complete(rstd::move(complete));
        else
            complete();
    }
};
enum class ErrorKind
{
    InvalidRequest,
    Body,
    Cancelled,
    Internal
};
struct Error {
    ErrorKind         kind;
    Option<BodyError> body;
};
using ResponseResult = Result<Response, Error>;
} // namespace liserveto

inline void RequestCompletion::attach(liserveto::Response& response) {
    response.lifetime_ = Some(lifetime.clone());
    response.cleanup_  = Some(cleanup.clone());
}
