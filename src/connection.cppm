module;
#include <rstd/macro.hpp>
export module liserveto:connection;
export import lihttpto;
export import :response_io;
export import :response;

import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lihttpto;
using IoError = rstd::io::error::Error;

export namespace liserveto
{
enum class ConnectionErrorKind
{
    State,
    Decode,
    Io,
    Timeout,
    Expectation,
    Response,
    Body
};
struct ConnectionError {
    ConnectionErrorKind   kind;
    Option<DecodeError>   decode;
    Option<ResponseError> response;
    Option<IoError>       cause;
    Option<BodyError>     body;
};
struct ConnectionLimits {
    HeadLimits           head;
    BodyLimits           body;
    rstd::time::Duration head_timeout { rstd::time::Duration::from_secs(u64(15)) };
    rstd::time::Duration body_timeout { rstd::time::Duration::from_secs(u64(60)) };
    rstd::time::Duration write_timeout { rstd::time::Duration::from_secs(u64(30)) };
    rstd::time::Duration close_timeout { rstd::time::Duration::from_millis(u64(100)) };
    usize                close_bytes { 65536 };
    usize                requests { 100 };
    // When set, each streamed body write has its own budget instead of the response-wide deadline.
    Option<rstd::time::Duration> stream_write_timeout;
    Option<rstd::time::Duration> stream_idle_timeout;
};
} // namespace liserveto

namespace liserveto
{
auto connection_error(ConnectionErrorKind kind) -> ConnectionError {
    return ConnectionError { kind, None(), None(), None() };
}
auto decode_error(DecodeError error) -> ConnectionError {
    return ConnectionError { ConnectionErrorKind::Decode, Some(error), None(), None() };
}
auto response_error(ResponseWriteError error) -> ConnectionError {
    return ConnectionError { error.kind == ResponseError::Timeout ? ConnectionErrorKind::Timeout
                                                                  : ConnectionErrorKind::Response,
                             None(),
                             Some(error.kind),
                             rstd::move(error.cause) };
}
struct ResponseTransfer {
    liserveto::Response& response;
    rstd::time::Instant  started { rstd::time::Instant::now() };
    u64                  bytes {};
    bool                 done {};
    void                 finish(ResponseOutcome outcome) {
        if (done) return;
        done          = true;
        response.body = Body {};
        response.finish({ outcome, bytes, started.elapsed() });
    }
    ~ResponseTransfer() { finish(ResponseOutcome::Cancelled); }
};
} // namespace liserveto

export namespace liserveto
{
template<typename IO>
class BasicConnection;
class PreparedUpgrade {
    template<typename>
    friend class BasicConnection;
    rstd::sync::Arc<empty> identity_;
    rstd::bytes::Bytes     response_;
    PreparedUpgrade(rstd::sync::Arc<empty> identity, rstd::bytes::Bytes response)
        : identity_(rstd::move(identity)), response_(rstd::move(response)) {}

public:
    PreparedUpgrade(const PreparedUpgrade&)                    = delete;
    auto operator=(const PreparedUpgrade&) -> PreparedUpgrade& = delete;
    PreparedUpgrade(PreparedUpgrade&&) noexcept                = default;
};
template<typename IO>
struct UpgradedIO {
    IO stream;
    // Consume these bytes before reading from the stream.
    rstd::bytes::Bytes pending;
};
template<typename IO>
class BasicConnection {
    enum class State
    {
        Idle,
        Reading,
        Body,
        ReadingBody,
        Ready,
        Writing,
        Streaming,
        Upgraded,
        Closed
    };
    State                 state_ { State::Idle };
    Option<IO>            stream_;
    ConnectionLimits      limits_;
    rstd::bytes::BytesMut buffer_ { rstd::bytes::BytesMut::with_capacity(usize(8192)) };
    Option<alloc::boxed::Box<RequestBodyDecoder>> body_;
    Option<UpgradeOffer>                          upgrade_;
    Option<rstd::sync::Arc<empty>>                upgrade_identity_;
    UpgradeError                                  upgrade_error_ { UpgradeError::InvalidRequest };
    String                                        method_;
    Version                                       version_ { Version::Http11 };
    bool                                          keep_alive_ {};
    bool                                          continue_pending_ {};
    bool                                          stream_body_allowed_ {};
    Option<u64>                                   stream_remaining_;
    rstd::time::Instant                           stream_started_ { rstd::time::Instant::now() };
    usize                                         requests_ {};
    rstd::time::Instant                           body_started_ { rstd::time::Instant::now() };

    struct Operation {
        BasicConnection& owner;
        bool             complete { false };
        ~Operation() {
            if (! complete) owner.close();
        }
    };

    auto refill(rstd::time::Instant started, rstd::time::Duration budget)
        -> rstd::async::coro<Result<bool, ConnectionError>> {
        auto elapsed = started.elapsed();
        if (elapsed >= budget) co_return Err(connection_error(ConnectionErrorKind::Timeout));
        buffer_.clear();
        auto timed = co_await rstd::async::timeout(rstd::async::io::read(*stream_, buffer_),
                                                   budget - elapsed);
        if (timed.is_err()) co_return Err(connection_error(ConnectionErrorKind::Timeout));
        auto read = rstd::move(timed).unwrap();
        if (read.is_err())
            co_return Err(ConnectionError {
                ConnectionErrorKind::Io, None(), None(), Some(rstd::move(read).unwrap_err()) });
        co_return Ok(*read != usize());
    }

    auto finish_rejection() -> rstd::async::coro<empty> {
        if ((co_await rstd::async::io::shutdown(*stream_)).is_ok()) {
            auto started   = rstd::time::Instant::now();
            auto discarded = buffer_.len();
            while (discarded < limits_.close_bytes) {
                auto read = co_await refill(started, limits_.close_timeout);
                if (read.is_err() || ! *read) break;
                if (buffer_.len() >= limits_.close_bytes - discarded) break;
                discarded += buffer_.len();
            }
        }
        close();
        co_return empty {};
    }

public:
    explicit BasicConnection(IO stream, ConnectionLimits limits = {})
        : stream_(Some(rstd::move(stream))), limits_(limits) {}
    BasicConnection(const BasicConnection&)                    = delete;
    BasicConnection(BasicConnection&&)                         = delete;
    auto operator=(const BasicConnection&) -> BasicConnection& = delete;
    auto operator=(BasicConnection&&) -> BasicConnection&      = delete;

    void close() {
        state_  = State::Closed;
        stream_ = None();
    }
    auto is_closed() const -> bool { return state_ == State::Closed; }
    auto is_upgraded() const -> bool { return state_ == State::Upgraded; }
    auto ready_for_request() const -> bool { return state_ == State::Idle; }
    auto body_complete() const -> bool { return state_ == State::Ready; }

    // Operations are sequential; cancelling an active operation closes this
    // connection.
    auto read_request() -> rstd::async::coro<Result<Option<RequestHead>, ConnectionError>> {
        if (state_ != State::Idle) co_return Err(connection_error(ConnectionErrorKind::State));
        state_ = State::Reading;
        Operation operation { *this };
        if (requests_ >= limits_.requests) {
            close();
            co_return Ok<Option<RequestHead>>(None());
        }
        RequestHeadDecoder decoder(limits_.head);
        auto               started  = rstd::time::Instant::now();
        bool               received = false;
        for (;;) {
            if (started.elapsed() >= limits_.head_timeout)
                co_return Err(connection_error(ConnectionErrorKind::Timeout));
            if (buffer_.is_empty()) {
                auto read = co_await refill(started, limits_.head_timeout);
                if (read.is_err()) co_return Err(rstd::move(read).unwrap_err());
                if (! *read) {
                    if (received) co_return Err(decode_error(DecodeError::Truncated));
                    close();
                    co_return Ok<Option<RequestHead>>(None());
                }
            }
            received      = true;
            auto progress = decoder.feed(buffer_.as_slice());
            if (progress.is_err()) co_return Err(decode_error(progress.unwrap_err()));
            buffer_.advance(progress->consumed);
            if (progress->status != DecodeStatus::Complete) continue;
            auto head         = decoder.take().unwrap();
            auto offer        = UpgradeOffer::parse(head);
            upgrade_          = None();
            upgrade_identity_ = None();
            if (offer.is_ok())
                upgrade_ = Some(rstd::move(offer).unwrap());
            else
                upgrade_error_ = offer.unwrap_err();
            auto expectation = request_expectation(head);
            if (expectation.is_err()) {
                lihttpto::Response rejection;
                rejection.status = u16(417);
                auto written     = co_await write_response(*stream_,
                                                           rejection,
                                                           head.line.method.as_str(),
                                                           head.line.version,
                                                           false,
                                                           limits_.write_timeout);
                if (written.is_err())
                    co_return Err(response_error(rstd::move(written).unwrap_err()));
                co_await finish_rejection();
                co_return Err(connection_error(ConnectionErrorKind::Expectation));
            }
            bool expect = *expectation == RequestExpectation::Continue;
            body_ = Some(alloc::boxed::Box<RequestBodyDecoder>::make(head.body, limits_.body));
            auto initial = (*body_)->feed({});
            if (initial.is_err()) co_return Err(decode_error(initial.unwrap_err()));
            method_  = head.line.method.clone();
            version_ = head.line.version;
            ++requests_;
            keep_alive_        = head.keep_alive && requests_ < limits_.requests;
            body_started_      = rstd::time::Instant::now();
            bool complete      = initial->status == DecodeStatus::Complete;
            continue_pending_  = expect && ! complete;
            state_             = complete ? State::Ready : State::Body;
            operation.complete = true;
            co_return Ok(Some(rstd::move(head)));
        }
    }

    auto accept_body() -> rstd::async::coro<Result<empty, ConnectionError>> {
        if (state_ != State::Body && state_ != State::Ready)
            co_return Err(connection_error(ConnectionErrorKind::State));
        if (! continue_pending_) co_return Ok(empty {});
        state_ = State::ReadingBody;
        Operation operation { *this };
        if (body_started_.elapsed() >= limits_.body_timeout)
            co_return Err(connection_error(ConnectionErrorKind::Timeout));
        auto interim =
            rstd::bytes::Bytes::copy_from_slice("HTTP/1.1 100 Continue\r\n\r\n"_str.as_bytes());
        rstd_co_try(co_await write_with_deadline(
                        *stream_, interim, rstd::time::Instant::now(), limits_.write_timeout),
                    response_error);
        continue_pending_  = false;
        state_             = State::Body;
        operation.complete = true;
        co_return Ok(empty {});
    }

    auto read_body() -> rstd::async::coro<Result<Option<rstd::bytes::Bytes>, ConnectionError>> {
        if (state_ == State::Ready) co_return Ok<Option<rstd::bytes::Bytes>>(None());
        if (state_ != State::Body) co_return Err(connection_error(ConnectionErrorKind::State));
        rstd_co_try(co_await accept_body());
        state_ = State::ReadingBody;
        Operation operation { *this };
        for (;;) {
            if (body_started_.elapsed() >= limits_.body_timeout)
                co_return Err(connection_error(ConnectionErrorKind::Timeout));
            if (buffer_.is_empty()) {
                auto read = co_await refill(body_started_, limits_.body_timeout);
                if (read.is_err()) co_return Err(rstd::move(read).unwrap_err());
                if (! *read) co_return Err(decode_error(DecodeError::Truncated));
            }
            auto progress = (*body_)->feed(buffer_.as_slice());
            if (progress.is_err()) co_return Err(decode_error(progress.unwrap_err()));
            auto output = rstd::bytes::Bytes::copy_from_slice(progress->data);
            buffer_.advance(progress->consumed);
            bool complete = progress->status == DecodeStatus::Complete;
            if (complete || ! output.is_empty()) {
                state_             = complete ? State::Ready : State::Body;
                operation.complete = true;
                if (output.is_empty()) co_return Ok<Option<rstd::bytes::Bytes>>(None());
                co_return Ok(Some(rstd::move(output)));
            }
        }
    }

    auto take_trailers() -> Option<Headers> {
        if (state_ != State::Ready) return None();
        return (*body_)->take_trailers();
    }

    auto prepare_upgrade(const UpgradeProtocol& protocol, const Headers& headers = {})
        -> Result<PreparedUpgrade, UpgradeError> {
        if (state_ != State::Ready) return Err(UpgradeError::InvalidRequest);
        if (upgrade_.is_none()) return Err(upgrade_error_);
        auto response = upgrade_->response(protocol, headers, limits_.head.total_bytes);
        if (response.is_err()) return Err(response.unwrap_err());
        if (upgrade_identity_.is_none()) upgrade_identity_ = Some(rstd::sync::Arc<empty>::make());
        return Ok(PreparedUpgrade { upgrade_identity_->clone(), rstd::move(response).unwrap() });
    }

    auto upgrade(PreparedUpgrade prepared)
        -> rstd::async::coro<Result<UpgradedIO<IO>, ConnectionError>> {
        if (state_ != State::Ready || upgrade_identity_.is_none() ||
            ! rstd::sync::Arc<empty>::ptr_eq(prepared.identity_, *upgrade_identity_))
            co_return Err(connection_error(ConnectionErrorKind::State));
        state_ = State::Writing;
        Operation operation { *this };
        rstd_co_try(
            co_await write_with_deadline(
                *stream_, prepared.response_, rstd::time::Instant::now(), limits_.write_timeout),
            response_error);
        auto pending = rstd::bytes::Bytes::copy_from_slice(buffer_.as_slice());
        buffer_.clear();
        body_              = None();
        upgrade_           = None();
        upgrade_identity_  = None();
        state_             = State::Upgraded;
        operation.complete = true;
        co_return Ok(UpgradedIO<IO> { stream_.take().unwrap(), rstd::move(pending) });
    }

    auto respond(const lihttpto::Response& response, bool close_after = false)
        -> rstd::async::coro<Result<empty, ConnectionError>> {
        if (state_ != State::Ready && ! (state_ == State::Body && close_after))
            co_return Err(connection_error(ConnectionErrorKind::State));
        bool unread_body  = state_ == State::Body;
        state_            = State::Writing;
        continue_pending_ = false;
        Operation operation { *this };
        bool      reuse   = keep_alive_ && ! close_after;
        auto      written = co_await write_response(
            *stream_, response, method_.as_str(), version_, reuse, limits_.write_timeout);
        if (written.is_err()) co_return Err(response_error(rstd::move(written).unwrap_err()));
        body_ = None();
        if (reuse)
            state_ = State::Idle;
        else if (unread_body)
            co_await finish_rejection();
        else
            close();
        operation.complete = true;
        co_return Ok(empty {});
    }

    auto send(liserveto::Response response) -> rstd::async::coro<Result<empty, ConnectionError>> {
        ResponseTransfer transfer { response };
        if ((state_ != State::Ready && state_ != State::Body) || response.body.requires_input()) {
            close();
            transfer.finish(ResponseOutcome::SourceError);
            co_return Err(connection_error(ConnectionErrorKind::State));
        }
        bool unread = state_ == State::Body;
        bool reuse  = keep_alive_ && ! response.close_after && ! unread;
        auto method = lihttpto::Method::parse(method_.as_str()).unwrap();
        auto made   = lihttpto::ResponseEncoder::make(response.head,
                                                      method,
                                                      version_,
                                                      response.content_length,
                                                      reuse,
                                                      limits_.head.total_bytes);
        if (made.is_err()) {
            close();
            transfer.finish(ResponseOutcome::SourceError);
            co_return Err(response_error({ made.unwrap_err(), None() }));
        }
        auto encoder      = rstd::move(made).unwrap();
        state_            = State::Writing;
        continue_pending_ = false;
        Operation operation { *this };
        auto      head = encoder.take_head().unwrap();
        auto      written =
            co_await write_with_deadline(*stream_, head, transfer.started, limits_.write_timeout);
        if (written.is_err()) {
            transfer.finish(written.unwrap_err().kind == ResponseError::Timeout
                                ? ResponseOutcome::Timeout
                                : ResponseOutcome::WriteError);
            co_return Err(response_error(rstd::move(written).unwrap_err()));
        }
        unsigned budget = 64;
        if (encoder.framing().kind != lihttpto::BodyKind::None)
            for (;;) {
                if (budget-- == 0) {
                    budget = 64;
                    co_await rstd::async::yield_now();
                }
                auto next = [&]() -> rstd::async::coro<Result<Option<BodyItem>, ConnectionError>> {
                    auto idle = limits_.stream_idle_timeout;
                    if (idle.is_none() && limits_.stream_write_timeout.is_none()) {
                        auto elapsed = transfer.started.elapsed();
                        if (elapsed >= limits_.write_timeout)
                            co_return Err(connection_error(ConnectionErrorKind::Timeout));
                        idle = Some(limits_.write_timeout - elapsed);
                    }
                    if (idle.is_none()) co_return Ok(co_await rstd::stream::next(response.body));
                    auto waited =
                        co_await rstd::async::timeout(rstd::stream::next(response.body), *idle);
                    if (waited.is_err())
                        co_return Err(connection_error(ConnectionErrorKind::Timeout));
                    co_return Ok(rstd::move(waited).unwrap());
                };
                auto part = co_await next();
                if (part.is_err()) {
                    transfer.finish(ResponseOutcome::Timeout);
                    co_return Err(rstd::move(part).unwrap_err());
                }
                auto item = rstd::move(part).unwrap();
                if (item.is_none()) break;
                auto frame = item.take().unwrap();
                if (frame.is_err()) {
                    auto error = frame.unwrap_err();
                    transfer.finish(error == BodyError::Timeout ? ResponseOutcome::Timeout
                                                                : ResponseOutcome::SourceError);
                    co_return Err(ConnectionError {
                        ConnectionErrorKind::Body, None(), None(), None(), Some(error) });
                }
                u64 payload {};
                if (frame->is_Data()) payload = u64(frame->as_Data().bytes.len().to_primitive());
                auto encoded = encoder.encode(rstd::move(frame).unwrap());
                if (encoded.is_err()) {
                    transfer.finish(ResponseOutcome::SourceError);
                    co_return Err(response_error({ encoded.unwrap_err(), None() }));
                }
                auto started = limits_.stream_write_timeout.is_some() ? rstd::time::Instant::now()
                                                                      : transfer.started;
                auto timeout = limits_.stream_write_timeout.is_some()
                                   ? *limits_.stream_write_timeout
                                   : limits_.write_timeout;
                auto output  = co_await write_with_deadline(*stream_, *encoded, started, timeout);
                if (output.is_err()) {
                    transfer.finish(output.unwrap_err().kind == ResponseError::Timeout
                                        ? ResponseOutcome::Timeout
                                        : ResponseOutcome::WriteError);
                    co_return Err(response_error(rstd::move(output).unwrap_err()));
                }
                transfer.bytes += payload;
            }
        auto tail = encoder.finish();
        if (tail.is_err()) {
            transfer.finish(ResponseOutcome::SourceError);
            co_return Err(response_error({ tail.unwrap_err(), None() }));
        }
        if (! tail->is_empty()) {
            auto started = limits_.stream_write_timeout.is_some() ? rstd::time::Instant::now()
                                                                  : transfer.started;
            auto timeout = limits_.stream_write_timeout.is_some() ? *limits_.stream_write_timeout
                                                                  : limits_.write_timeout;
            auto output  = co_await write_with_deadline(*stream_, *tail, started, timeout);
            if (output.is_err()) {
                transfer.finish(output.unwrap_err().kind == ResponseError::Timeout
                                    ? ResponseOutcome::Timeout
                                    : ResponseOutcome::WriteError);
                co_return Err(response_error(rstd::move(output).unwrap_err()));
            }
        }
        body_ = None();
        if (encoder.keep_alive())
            state_ = State::Idle;
        else if (unread)
            co_await finish_rejection();
        else
            close();
        operation.complete = true;
        transfer.finish(ResponseOutcome::Complete);
        co_return Ok(empty {});
    }

    // Streamed responses may declare a length and always close the connection.
    auto begin_response(const StreamResponseHead& response)
        -> rstd::async::coro<Result<empty, ConnectionError>> {
        if (state_ != State::Ready) co_return Err(connection_error(ConnectionErrorKind::State));
        auto head = encode_response_head(response, method_.as_str(), version_);
        if (head.is_err())
            co_return Err(response_error(ResponseWriteError { head.unwrap_err(), None() }));
        state_ = State::Writing;
        Operation operation { *this };
        stream_started_ = rstd::time::Instant::now();
        rstd_co_try(
            co_await write_with_deadline(*stream_, *head, stream_started_, limits_.write_timeout),
            response_error);
        stream_body_allowed_ = ! omit_response_body(response.status, method_.as_str());
        stream_remaining_    = stream_body_allowed_ ? response.content_length : Some(u64());
        body_                = None();
        state_               = State::Streaming;
        operation.complete   = true;
        co_return Ok(empty {});
    }

    auto write_body(const rstd::bytes::Bytes& bytes)
        -> rstd::async::coro<Result<empty, ConnectionError>> {
        if (state_ != State::Streaming) co_return Err(connection_error(ConnectionErrorKind::State));
        state_ = State::Writing;
        Operation operation { *this };
        if (! stream_body_allowed_ && ! bytes.is_empty())
            co_return Err(
                response_error(ResponseWriteError { ResponseError::InvalidBody, None() }));
        auto length = u64(bytes.len().to_primitive());
        if (stream_remaining_.is_some() && length > *stream_remaining_)
            co_return Err(
                response_error(ResponseWriteError { ResponseError::InvalidBody, None() }));
        if (! bytes.is_empty()) {
            auto started = limits_.stream_write_timeout.is_some() ? rstd::time::Instant::now()
                                                                  : stream_started_;
            auto budget  = limits_.stream_write_timeout.is_some() ? *limits_.stream_write_timeout
                                                                  : limits_.write_timeout;
            rstd_co_try(co_await write_with_deadline(*stream_, bytes, started, budget),
                        response_error);
        }
        if (stream_remaining_.is_some()) *stream_remaining_ -= length;
        state_             = State::Streaming;
        operation.complete = true;
        co_return Ok(empty {});
    }

    auto finish_response() -> Result<empty, ConnectionError> {
        if (state_ != State::Streaming) return Err(connection_error(ConnectionErrorKind::State));
        bool incomplete = stream_remaining_.is_some() && *stream_remaining_ != u64();
        close();
        if (incomplete)
            return Err(response_error(ResponseWriteError { ResponseError::InvalidBody, None() }));
        return Ok(empty {});
    }

    auto reject(const lihttpto::Response& response)
        -> rstd::async::coro<Result<empty, ConnectionError>> {
        co_return co_await respond(response, true);
    }
};
using Connection         = BasicConnection<rstd::net::TcpStream>;
using UpgradedConnection = UpgradedIO<rstd::net::TcpStream>;
} // namespace liserveto
