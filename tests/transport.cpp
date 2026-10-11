#include <rstd/test/gtest.hpp>
import libserveto;

using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::task::Context;
using rstd::task::Poll;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;

struct MemoryWire {
    Bytes    input;
    BytesMut output;
    bool     fail_write {};
};
struct MemoryIO {
    rstd::sync::Arc<MemoryWire> state;
    auto                        poll_read(mut_ref<MemoryIO> self, Context&, BytesMut& output)
        -> Poll<rstd::io::Result<usize>> {
        auto data = rstd::exchange(self->state->input, Bytes {});
        output.put_slice(data.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(data.len()));
    }
    auto poll_write(mut_ref<MemoryIO> self, Context&, const Bytes& bytes)
        -> Poll<rstd::io::Result<usize>> {
        if (self->state->fail_write)
            return Poll<rstd::io::Result<usize>>::Ready(Err(rstd::io::error::Error::from_kind(
                rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::BrokenPipe })));
        self->state->output.put_slice(bytes.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(bytes.len()));
    }
    auto poll_flush(mut_ref<MemoryIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
    auto poll_shutdown(mut_ref<MemoryIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
};
auto memory_exchange(rstd::sync::Arc<MemoryWire> wire) -> rstd::async::coro<bool> {
    libserveto::BasicConnection<MemoryIO> connection(MemoryIO { wire.clone() });
    auto                                  first = co_await connection.read_request();
    if (first.is_err() || first->is_none()) co_return false;
    auto data = co_await connection.read_body();
    if (data.is_err() || data->is_none() || (**data).as_slice() != "abc"_str.as_bytes())
        co_return false;
    if ((co_await connection.read_body()).unwrap().is_some()) co_return false;
    lihttpto::Response response;
    response.body = Bytes::copy_from_slice("ok"_str.as_bytes());
    if ((co_await connection.respond(response)).is_err()) co_return false;
    auto second = co_await connection.read_request();
    if (second.is_err() || second->is_none() || (*second)->line.target.as_str() != "/next"_str)
        co_return false;
    co_return (co_await connection.respond(response)).is_ok() && connection.is_closed();
}
TEST(Transport, GenericConnectionReusesBufferedPipelineWithoutAnotherParser) {
    auto wire   = rstd::sync::Arc<MemoryWire>::make();
    wire->input = Bytes::copy_from_slice(
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET /next HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_str
            .as_bytes());
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(memory_exchange(wire.clone())));
    auto output = rstd::str_::from_utf8(wire->output.as_slice()).unwrap();
    EXPECT_TRUE(output.contains("Connection: keep-alive\r\n"_str));
    EXPECT_TRUE(output.contains("Connection: close\r\n"_str));
}
auto failed_write(rstd::sync::Arc<MemoryWire> wire) -> rstd::async::coro<bool> {
    libserveto::BasicConnection<MemoryIO> connection(MemoryIO { wire.clone() });
    auto                                  request = co_await connection.read_request();
    if (request.is_err() || request->is_none()) co_return false;
    lihttpto::Response response;
    auto               result = co_await connection.respond(response);
    co_return result.is_err() && result.unwrap_err().cause.is_some() && connection.is_closed();
}
TEST(Transport, WriteFailurePreservesCauseAndClosesConnection) {
    auto wire        = rstd::sync::Arc<MemoryWire>::make();
    wire->input      = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_str.as_bytes());
    wire->fail_write = true;
    auto runtime     = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(runtime.block_on(failed_write(wire.clone())));
}
