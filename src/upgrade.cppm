export module liserveto:upgrade;
export import lihttpto;
export import :cleanup;
export import :body;

using namespace rstd::prelude;
using rstd::sync::Arc;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;
using rstd::task::Context;
using rstd::task::Poll;

struct UpgradeIoPoll {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = UpgradeIoPoll;
        auto read(Context& cx, BytesMut& bytes) -> Poll<rstd::io::Result<usize>> {
            return rstd::trait_call<0>(this, cx, bytes);
        }
        auto write(Context& cx, const Bytes& bytes) -> Poll<rstd::io::Result<usize>> {
            return rstd::trait_call<1>(this, cx, bytes);
        }
        auto flush(Context& cx) -> Poll<rstd::io::Result<empty>> {
            return rstd::trait_call<2>(this, cx);
        }
        auto shutdown(Context& cx) -> Poll<rstd::io::Result<empty>> {
            return rstd::trait_call<3>(this, cx);
        }
    };
    template<class S>
    using Funcs = TraitFuncs<&S::read, &S::write, &S::flush, &S::shutdown>;
};
template<class IO>
struct StoredUpgradeIo {
    IO   stream;
    auto read(Context& cx, BytesMut& bytes) -> Poll<rstd::io::Result<usize>> {
        return rstd::async::io::poll_read(stream, cx, bytes);
    }
    auto write(Context& cx, const Bytes& bytes) -> Poll<rstd::io::Result<usize>> {
        return rstd::async::io::poll_write(stream, cx, bytes);
    }
    auto flush(Context& cx) -> Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_flush(stream, cx);
    }
    auto shutdown(Context& cx) -> Poll<rstd::io::Result<empty>> {
        return rstd::async::io::poll_shutdown(stream, cx);
    }
};

export namespace liserveto
{
template<class>
class BasicConnection;
class Response;
class UpgradePermit {
    template<class>
    friend class BasicConnection;
    friend class Response;
    Arc<lihttpto::UpgradeOffer> offer_;
    explicit UpgradePermit(Arc<lihttpto::UpgradeOffer> offer): offer_(rstd::move(offer)) {}

public:
    auto clone() const -> UpgradePermit { return UpgradePermit(offer_.clone()); }
};
class UpgradedStream {
    Box<dyn<UpgradeIoPoll>> stream_;
    Bytes                   pending_;
    UpgradedStream(Box<dyn<UpgradeIoPoll>> stream, Bytes pending)
        : stream_(rstd::move(stream)), pending_(rstd::move(pending)) {}

public:
    template<class IO>
    static auto make(IO stream, Bytes pending) -> UpgradedStream {
        return UpgradedStream(
            Box<dyn<UpgradeIoPoll>>::make(StoredUpgradeIo<IO> { rstd::move(stream) }),
            rstd::move(pending));
    }
    auto poll_read(mut_ref<UpgradedStream> self, Context& cx, BytesMut& output)
        -> Poll<rstd::io::Result<usize>> {
        if (! self->pending_.is_empty()) {
            auto data = rstd::exchange(self->pending_, Bytes {});
            output.put_slice(data.as_slice());
            return Poll<rstd::io::Result<usize>>::Ready(Ok(data.len()));
        }
        return self->stream_->read(cx, output);
    }
    auto poll_write(mut_ref<UpgradedStream> self, Context& cx, const Bytes& bytes)
        -> Poll<rstd::io::Result<usize>> {
        return self->stream_->write(cx, bytes);
    }
    auto poll_flush(mut_ref<UpgradedStream> self, Context& cx) -> Poll<rstd::io::Result<empty>> {
        return self->stream_->flush(cx);
    }
    auto poll_shutdown(mut_ref<UpgradedStream> self, Context& cx) -> Poll<rstd::io::Result<empty>> {
        return self->stream_->shutdown(cx);
    }
};
} // namespace liserveto

struct UpgradeAction {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = UpgradeAction;
        auto run(liserveto::UpgradedStream stream, Arc<ConnectionCleanup> cleanup)
            -> rstd::async::coro<Result<empty, liserveto::BodyError>> {
            return rstd::trait_call<0>(this, rstd::move(stream), rstd::move(cleanup));
        }
    };
    template<class S>
    using Funcs = TraitFuncs<&S::run>;
};
template<class H>
struct StoredUpgradeAction {
    H    handler;
    auto run(liserveto::UpgradedStream stream, Arc<ConnectionCleanup> cleanup)
        -> rstd::async::coro<Result<empty, liserveto::BodyError>> {
        co_return co_await handler(rstd::move(stream), rstd::move(cleanup));
    }
};
struct UpgradePlan {
    liserveto::UpgradePermit  permit;
    lihttpto::UpgradeProtocol protocol;
    Box<dyn<UpgradeAction>>   action;
};
