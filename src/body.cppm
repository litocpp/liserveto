export module liserveto:body;
export import lihttpto;
export import rstd;

using namespace rstd::prelude;
using rstd::bytes::Bytes;
using rstd::task::Context;
using rstd::task::Poll;

export namespace liserveto
{
enum class BodyError
{
    Source,
    Protocol,
    LimitExceeded,
    Cancelled,
    Io,
    Timeout
};
using BodyItem = Result<lihttpto::BodyFrame, BodyError>;
struct CollectedBody {
    Bytes             data;
    lihttpto::Headers trailers;
};
} // namespace liserveto

namespace liserveto
{
// Erase only the stored source; all progression uses the public Stream protocol.
struct BodyPoll {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = BodyPoll;
        auto poll(Context& cx) -> Poll<Option<BodyItem>> { return rstd::trait_call<0>(this, cx); }
        auto requires_input() const -> bool { return rstd::trait_call<1>(this); }
    };
    template<typename S>
    using Funcs = TraitFuncs<&S::poll, &S::requires_input>;
};
template<rstd::stream::Source S>
struct StoredBody {
    S    source;
    auto poll(Context& cx) -> Poll<Option<BodyItem>> { return rstd::future::poll_next(source, cx); }
    auto requires_input() const -> bool {
        if constexpr (requires { source.requires_input(); }) return source.requires_input();
        return false;
    }
};
} // namespace liserveto

export namespace liserveto
{
class Body {
    Option<Box<dyn<BodyPoll>>> source_;
    bool                       trailers_ {};
    explicit Body(Box<dyn<BodyPoll>> source): source_(Some(rstd::move(source))) {}

public:
    using Item                               = BodyItem;
    Body()                                   = default;
    Body(const Body&)                        = delete;
    auto operator=(const Body&) -> Body&     = delete;
    Body(Body&&) noexcept                    = default;
    auto operator=(Body&&) noexcept -> Body& = default;

    template<rstd::stream::Source S>
        requires rstd::mtp::same_as<rstd::future::stream_item_t<S>, Item>
    static auto from_stream(S source) -> Body {
        return Body(Box<dyn<BodyPoll>>::make(StoredBody<S> { rstd::move(source) }));
    }
    static auto from_bytes(Bytes bytes) -> Body {
        return from_stream(
            rstd::stream::once(Item(Ok(lihttpto::BodyFrame::Data(rstd::move(bytes))))));
    }
    auto requires_input() const -> bool {
        return source_.is_some() && (*source_)->requires_input();
    }
    auto is_empty() const -> bool { return source_.is_none(); }
    auto poll_next(mut_ref<Body> self, Context& cx) -> Poll<Option<Item>> {
        if (self->source_.is_none()) return Poll<Option<Item>>::Ready(None());
        auto out = (*self->source_)->poll(cx);
        if (out.is_pending()) return Poll<Option<Item>>::Pending();
        auto frame = rstd::move(out).take();
        if (frame.is_none() || frame->is_err()) {
            self->source_ = None();
            return Poll<Option<Item>>::Ready(rstd::move(frame));
        }
        auto& value = **frame;
        if (self->trailers_ ||
            (value.is_Trailers() &&
             lihttpto::validate_trailers(value.as_Trailers().headers).is_err())) {
            self->source_ = None();
            return Poll<Option<Item>>::Ready(Some(Item(Err(BodyError::Protocol))));
        }
        self->trailers_ = value.is_Trailers();
        return Poll<Option<Item>>::Ready(rstd::move(frame));
    }
    auto collect(u64 byte_limit) -> rstd::async::coro<Result<CollectedBody, BodyError>> {
        rstd::bytes::BytesMut bytes;
        lihttpto::Headers     trailers;
        unsigned              budget = 64;
        for (;;) {
            if (budget-- == 0) {
                budget = 64;
                co_await rstd::async::yield_now();
            }
            auto next = co_await rstd::stream::next(*this);
            if (next.is_none())
                co_return Ok(CollectedBody { bytes.freeze(), rstd::move(trailers) });
            auto item = rstd::move(next).unwrap();
            if (item.is_err()) co_return Err(rstd::move(item).unwrap_err());
            auto frame = rstd::move(item).unwrap();
            if (frame.is_Trailers()) {
                trailers = rstd::move(frame.as_Trailers().headers);
                continue;
            }
            auto& data = frame.as_Data().bytes;
            if (u64(data.len().to_primitive()) > byte_limit - u64(bytes.len().to_primitive())) {
                source_ = None();
                co_return Err(BodyError::LimitExceeded);
            }
            bytes.put_slice(data.as_slice());
        }
    }
};
} // namespace liserveto
