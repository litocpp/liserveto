module;
#include <rstd/macro.hpp>
export module liserveto:response_io;
export import lihttpto;
using namespace rstd::prelude;
using namespace lihttpto;
using IoError = rstd::io::error::Error;
export namespace liserveto
{
template<typename IO>
auto write_with_deadline(IO&                       stream,
                         const rstd::bytes::Bytes& bytes,
                         rstd::time::Instant       started,
                         rstd::time::Duration      budget)
    -> rstd::async::coro<Result<empty, ResponseWriteError>> {
    auto elapsed = started.elapsed();
    if (elapsed >= budget) co_return Err(ResponseWriteError { ResponseError::Timeout, None() });
    auto timed =
        co_await rstd::async::timeout(rstd::async::io::write_all(stream, bytes), budget - elapsed);
    if (timed.is_err()) co_return Err(ResponseWriteError { ResponseError::Timeout, None() });
    auto written = rstd::move(timed).unwrap();
    if (written.is_err())
        co_return Err(
            ResponseWriteError { ResponseError::Io, Some(rstd::move(written).unwrap_err()) });
    co_return Ok(empty {});
}
template<typename IO>
auto write_response(IO&                       stream,
                    const lihttpto::Response& response,
                    ref<str>                  request_method,
                    Version                   version,
                    bool                      keep_alive = false,
                    rstd::time::Duration      budget     = rstd::time::Duration::from_secs(u64(30)))
    -> rstd::async::coro<Result<empty, ResponseWriteError>> {
    auto head = encode_response_head(response, request_method, version, keep_alive);
    if (head.is_err()) co_return Err(ResponseWriteError { head.unwrap_err(), None() });
    auto bytes   = rstd::move(head).unwrap();
    auto started = rstd::time::Instant::now();
    rstd_co_try(co_await write_with_deadline(stream, bytes, started, budget));
    if (! omit_response_body(response.status, request_method) && ! response.body.is_empty()) {
        rstd_co_try(co_await write_with_deadline(stream, response.body, started, budget));
    }
    co_return Ok(empty {});
}
} // namespace liserveto
