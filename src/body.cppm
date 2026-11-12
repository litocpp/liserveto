export module liserveto:body;
export import lihttpto;
export import rstd;

using namespace rstd::prelude;
using namespace rstd::literals;
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

export namespace rstd
{
template<>
struct Impl<fmt::Display, liserveto::BodyError> : ImplBase<liserveto::BodyError> {
    auto fmt(fmt::Formatter& f) const -> bool {
        switch (this->self()) {
        case liserveto::BodyError::Source: return f.write_str("HTTP body source failed"_str);
        case liserveto::BodyError::Protocol: return f.write_str("invalid HTTP body framing"_str);
        case liserveto::BodyError::LimitExceeded:
            return f.write_str("HTTP body limit exceeded"_str);
        case liserveto::BodyError::Cancelled: return f.write_str("HTTP body cancelled"_str);
        case liserveto::BodyError::Io: return f.write_str("HTTP body I/O failed"_str);
        case liserveto::BodyError::Timeout: return f.write_str("HTTP body timed out"_str);
        }
        __builtin_unreachable();
    }
};
template<>
struct Impl<fmt::Debug, liserveto::BodyError> : Impl<fmt::Display, liserveto::BodyError> {};
template<>
struct Impl<error::Error, liserveto::BodyError>
    : DefaultInImpl<error::Error, liserveto::BodyError> {};
} // namespace rstd

namespace liserveto
{
// Erase only the stored source; all progression uses the public Stream protocol.
struct BodyPoll {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = BodyPoll;
        auto poll(Context& cx) -> Poll<Option<BodyItem>> { return rstd::trait_call<0>(this, cx); }
        auto requires_input() const -> bool { return rstd::trait_call<1>(this); }
        auto close(Context& cx) -> Poll<Result<empty, BodyError>> {
            return rstd::trait_call<2>(this, cx);
        }
        auto has_close() const -> bool { return rstd::trait_call<3>(this); }
    };
    template<typename S>
    using Funcs = TraitFuncs<&S::poll, &S::requires_input, &S::close, &S::has_close>;
};
template<rstd::stream::Source S>
struct StoredBody {
    S    source;
    auto poll(Context& cx) -> Poll<Option<BodyItem>> { return rstd::future::poll_next(source, cx); }
    auto requires_input() const -> bool {
        if constexpr (requires { source.requires_input(); }) return source.requires_input();
        return false;
    }
    auto close(Context& cx) -> Poll<Result<empty, BodyError>> {
        if constexpr (requires { source.poll_close(rstd::future::as_mut_ref(source), cx); })
            return source.poll_close(rstd::future::as_mut_ref(source), cx);
        else
            return Poll<Result<empty, BodyError>>::Ready(Ok(empty {}));
    }
    auto has_close() const -> bool {
        return requires(S& value, Context& cx) {
            value.poll_close(rstd::future::as_mut_ref(value), cx);
        };
    }
};
} // namespace liserveto

export namespace liserveto
{
class BodyClose {
    friend class Body;
    Option<Box<dyn<BodyPoll>>> source_;
    bool                       completed_ {};
    Option<BodyError>          error_;
    explicit BodyClose(Option<Box<dyn<BodyPoll>>> source): source_(rstd::move(source)) {}

public:
    using Output                = Result<empty, BodyError>;
    BodyClose(const BodyClose&) = delete;
    BodyClose(BodyClose&& other) noexcept
        : source_(other.source_.take()),
          completed_(rstd::exchange(other.completed_, true)),
          error_(other.error_) {}
    auto operator=(BodyClose&& other) noexcept -> BodyClose& {
        if (this == &other) return *this;
        source_    = other.source_.take();
        completed_ = rstd::exchange(other.completed_, true);
        error_     = other.error_;
        return *this;
    }
    auto poll(mut_ref<BodyClose> self, Context& cx) -> Poll<Output> {
        if (self->completed_)
            return Poll<Output>::Ready(self->error_.is_some() ? Output(Err(*self->error_))
                                                              : Output(Ok(empty {})));
        if (self->source_.is_none()) {
            self->completed_ = true;
            return Poll<Output>::Ready(Ok(empty {}));
        }
        auto result = (*self->source_)->close(cx);
        if (result.is_ready()) {
            auto closed = rstd::move(result).take();
            if (closed.is_err()) self->error_ = Some(closed.unwrap_err());
            self->source_    = None();
            self->completed_ = true;
            return Poll<Output>::Ready(rstd::move(closed));
        }
        return result;
    }
    auto is_immediate() const -> bool { return source_.is_none() || ! (*source_)->has_close(); }
};
class Body {
    Option<Box<dyn<BodyPoll>>> source_;
    Option<BodyClose>          closing_;
    Option<Option<BodyItem>>   terminal_;
    bool                       trailers_ {};
    bool                       done_ {};
    explicit Body(Box<dyn<BodyPoll>> source): source_(Some(rstd::move(source))) {}

public:
    using Item                           = BodyItem;
    Body()                               = default;
    Body(const Body&)                    = delete;
    auto operator=(const Body&) -> Body& = delete;
    Body(Body&& other) noexcept
        : source_(other.source_.take()),
          closing_(other.closing_.take()),
          terminal_(other.terminal_.take()),
          trailers_(other.trailers_),
          done_(rstd::exchange(other.done_, true)) {}
    auto operator=(Body&& other) noexcept -> Body& {
        if (this == &other) return *this;
        source_   = other.source_.take();
        closing_  = other.closing_.take();
        terminal_ = other.terminal_.take();
        trailers_ = other.trailers_;
        done_     = rstd::exchange(other.done_, true);
        return *this;
    }

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
    auto is_empty() const -> bool { return source_.is_none() && closing_.is_none(); }
    // Keep the returned future alive and await it when a release barrier is required.
    auto close() -> BodyClose {
        done_     = true;
        terminal_ = None();
        if (closing_.is_some()) return closing_.take().unwrap();
        return BodyClose(source_.take());
    }
    auto poll_next(mut_ref<Body> self, Context& cx) -> Poll<Option<Item>> {
        if (self->closing_.is_some()) {
            auto result = rstd::future::poll(*self->closing_, cx);
            if (result.is_pending()) return Poll<Option<Item>>::Pending();
            auto closed    = rstd::move(result).take();
            self->closing_ = None();
            self->done_    = true;
            auto frame     = self->terminal_.take().unwrap();
            if (closed.is_err() && frame.is_none()) frame = Some(Item(Err(closed.unwrap_err())));
            return Poll<Option<Item>>::Ready(rstd::move(frame));
        }
        if (self->done_) return Poll<Option<Item>>::Ready(None());
        if (self->source_.is_none()) return Poll<Option<Item>>::Ready(None());
        auto out = (*self->source_)->poll(cx);
        if (out.is_pending()) return Poll<Option<Item>>::Pending();
        auto frame = rstd::move(out).take();
        if (frame.is_none() || frame->is_err()) {
            self->terminal_ = Some(rstd::move(frame));
            self->closing_  = Some(BodyClose(self->source_.take()));
            return self->poll_next(self, cx);
        }
        auto& value = **frame;
        if (self->trailers_ ||
            (value.is_Trailers() &&
             lihttpto::validate_trailers(value.as_Trailers().headers).is_err())) {
            self->terminal_ = Some(Some(Item(Err(BodyError::Protocol))));
            self->closing_  = Some(BodyClose(self->source_.take()));
            return self->poll_next(self, cx);
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
                (void)co_await close();
                co_return Err(BodyError::LimitExceeded);
            }
            bytes.put_slice(data.as_slice());
        }
    }
};
} // namespace liserveto
