export module liserveto:incoming;
export import :connection;

using namespace rstd::prelude;
using rstd::sync::Arc;
using rstd::task::Poll;
using rstd::task::Context;

struct IncomingState {
    rstd::sync::atomic::Atomic<bool> active { true };
    rstd::sync::atomic::Atomic<bool> complete { false };
    liserveto::Body                  source;
};
template<class IO>
struct IncomingFrames {
    using Item       = liserveto::BodyItem;
    using ReadResult = Result<Option<rstd::bytes::Bytes>, liserveto::ConnectionError>;
    Arc<liserveto::BasicConnection<IO>>         connection;
    IncomingState*                              state;
    Option<rstd::async::CoroFuture<ReadResult>> pending;
    bool                                        done {};
    auto requires_input() const -> bool { return ! state->complete.load(); }
    auto poll_next(mut_ref<IncomingFrames> self, Context& cx) -> Poll<Option<Item>> {
        if (self->done) return Poll<Option<Item>>::Ready(None());
        if (! self->state->active.load()) {
            self->pending = None();
            self->done    = true;
            return Poll<Option<Item>>::Ready(Some(Item(Err(liserveto::BodyError::Cancelled))));
        }
        if (self->pending.is_none())
            self->pending.insert(rstd::async::into_future(self->connection->read_body()));
        auto out = rstd::future::poll(*self->pending, cx);
        if (out.is_pending()) return Poll<Option<Item>>::Pending();
        auto result   = rstd::move(out).take();
        self->pending = None();
        if (result.is_err()) {
            self->done = true;
            auto kind  = result.unwrap_err().kind;
            auto error =
                kind == liserveto::ConnectionErrorKind::Timeout  ? liserveto::BodyError::Timeout
                : kind == liserveto::ConnectionErrorKind::Decode ? liserveto::BodyError::Protocol
                                                                 : liserveto::BodyError::Io;
            return Poll<Option<Item>>::Ready(Some(Item(Err(error))));
        }
        auto bytes = rstd::move(result).unwrap();
        if (bytes.is_some())
            return Poll<Option<Item>>::Ready(
                Some(Item(Ok(lihttpto::BodyFrame::Data(bytes.take().unwrap())))));
        self->done = true;
        self->state->complete.store(true);
        auto trailers = self->connection->take_trailers();
        if (trailers.is_some() && ! trailers->is_empty())
            return Poll<Option<Item>>::Ready(
                Some(Item(Ok(lihttpto::BodyFrame::Trailers(trailers.take().unwrap())))));
        return Poll<Option<Item>>::Ready(None());
    }
};

export namespace liserveto
{
class IncomingScope {
    Arc<IncomingState> state_;
    friend class IncomingBody;
    explicit IncomingScope(Arc<IncomingState> state): state_(rstd::move(state)) {}

public:
    IncomingScope(const IncomingScope&)     = delete;
    IncomingScope(IncomingScope&&) noexcept = default;
    ~IncomingScope() { expire(); }
    auto complete() const -> bool { return state_->complete.load(); }
    void expire() {
        if (state_ && state_->active.exchange(false)) state_->source = Body {};
    }
};
class IncomingBody {
    Arc<IncomingState> state_;
    bool               done_ {};
    explicit IncomingBody(Arc<IncomingState> state): state_(rstd::move(state)) {}

public:
    using Item                                               = BodyItem;
    IncomingBody(const IncomingBody&)                        = delete;
    IncomingBody(IncomingBody&&) noexcept                    = default;
    auto operator=(IncomingBody&&) noexcept -> IncomingBody& = default;
    template<class IO>
    static auto from_connection(Arc<BasicConnection<IO>> connection) -> IncomingBody {
        auto state = Arc<IncomingState>::make();
        state->complete.store(connection->body_complete());
        auto source   = IncomingFrames<IO> { rstd::move(connection), state.as_ptr().as_raw_ptr() };
        state->source = Body::from_stream(rstd::move(source));
        return IncomingBody(rstd::move(state));
    }
    static auto from_body(Body body) -> IncomingBody {
        auto state    = Arc<IncomingState>::make();
        state->source = rstd::move(body);
        return IncomingBody(rstd::move(state));
    }
    auto scope() const -> IncomingScope { return IncomingScope(state_.clone()); }
    auto requires_input() const -> bool { return ! state_->complete.load(); }
    auto poll_next(mut_ref<IncomingBody> self, Context& cx) -> Poll<Option<Item>> {
        if (self->done_) return Poll<Option<Item>>::Ready(None());
        if (! self->state_->active.load()) {
            self->done_ = true;
            return Poll<Option<Item>>::Ready(Some(Item(Err(BodyError::Cancelled))));
        }
        auto out = rstd::future::poll_next(self->state_->source, cx);
        if (out.is_ready()) {
            auto value = rstd::move(out).take();
            if (value.is_none()) {
                self->done_ = true;
                self->state_->complete.store(true);
            } else if (value->is_err())
                self->done_ = true;
            return Poll<Option<Item>>::Ready(rstd::move(value));
        }
        return out;
    }
    auto collect(u64 limit) -> rstd::async::coro<Result<CollectedBody, BodyError>> {
        auto stream = Body::from_stream(rstd::stream::by_ref(*this));
        co_return co_await stream.collect(limit);
    }
};
} // namespace liserveto
