export module liserveto:response;
export import :body;

using namespace rstd::prelude;
using rstd::bytes::Bytes;

export namespace liserveto
{
enum class ResponseOutcome
{
    Complete,
    SourceError,
    WriteError,
    Cancelled,
    Timeout
};
struct ResponseReport {
    ResponseOutcome      outcome;
    u64                  bytes {};
    rstd::time::Duration elapsed;
};
} // namespace liserveto

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
    Option<Box<dyn<ResponseObserver>>> observer_;

public:
    lihttpto::ResponseHead head { lihttpto::StatusCode::make(u16(200)).unwrap(), {}, {}, {} };
    Body                   body;
    Option<u64>            content_length { Some(u64()) };
    bool                   close_after {};

    Response()                                   = default;
    Response(const Response&)                    = delete;
    auto operator=(const Response&) -> Response& = delete;
    Response(Response&& other) noexcept
        : observer_(other.observer_.take()),
          head(rstd::move(other.head)),
          body(rstd::move(other.body)),
          content_length(other.content_length),
          close_after(other.close_after) {}
    auto operator=(Response&& other) noexcept -> Response& {
        if (this == &other) return *this;
        finish({ ResponseOutcome::Cancelled });
        observer_      = other.observer_.take();
        head           = rstd::move(other.head);
        body           = rstd::move(other.body);
        content_length = other.content_length;
        close_after    = other.close_after;
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
        finish({ ResponseOutcome::Cancelled });
        observer_ = Some(
            Box<dyn<ResponseObserver>>::make(StoredResponseObserver<F> { rstd::move(function) }));
    }
    void finish(ResponseReport report) {
        if (observer_.is_some()) {
            auto observer = observer_.take().unwrap();
            observer->finish(report);
        }
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
