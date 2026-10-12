#include <rstd/test/gtest.hpp>
#include <rstd/macro.hpp>
#if RSTD_OS_UNIX
#include <fcntl.h>
#endif
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;
using rstd::task::Context;
using rstd::task::Poll;

#if RSTD_OS_LINUX || (RSTD_OS_MACOS && defined(O_RESOLVE_BENEATH) && defined(O_NOFOLLOW_ANY))
auto file_path(const rstd::fs::TempDir& directory) -> rstd::path::PathBuf {
    return rstd::path::PathBuf::from(directory.path().as_os_str().to_os_string()).join("file"_str);
}
auto prepared_file(ref<rstd::path::Path> path, int& descriptor) -> FileResponse {
    auto file  = rstd::fs::File::open(path).unwrap();
    descriptor = file.as_raw_fd();
    return FileResponse::prepare(rstd::move(file)).unwrap();
}
auto drain_file(Response& response, usize maximum) -> rstd::async::coro<Bytes> {
    BytesMut result;
    for (;;) {
        auto next = co_await rstd::stream::next(response.body);
        if (next.is_none()) break;
        EXPECT_TRUE(next->is_ok());
        if (next->is_err()) break;
        auto bytes = rstd::move(next).unwrap().unwrap().as_Data().bytes;
        EXPECT_TRUE(bytes.len() <= maximum);
        result.put_slice(bytes.as_slice());
    }
    co_return result.freeze();
}
TEST(FileResponse, BoundedRangeUsesPreparedHandleAfterRenameAndDeletion) {
    auto directory = rstd::fs::TempDir::make("liserveto-file"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    auto file      = rstd::fs::File::open(path.as_path()).unwrap();
    auto raw       = file.as_raw_fd();
    auto duplicate = file.try_clone().unwrap();
    file.seek(rstd::io::SeekFrom::from_start(u64(5))).unwrap();
    auto prepared = FileResponse::prepare(rstd::move(file)).unwrap();
    EXPECT_TRUE(prepared.metadata().etag->is_weak());
    auto moved =
        rstd::path::PathBuf::from(directory.path().as_os_str().to_os_string()).join("moved"_str);
    rstd::fs::rename(path.as_path(), moved.as_path()).unwrap();
    rstd::fs::remove_file(moved.as_path()).unwrap();
    lihttpto::Headers headers;
    headers.add("range"_str, "bytes=2-8"_str).unwrap();
    auto conditions = lihttpto::RequestConditions::parse(headers).unwrap();
    auto response   = rstd::move(prepared).respond("GET"_str, conditions, {}, usize(2)).unwrap();
    EXPECT_EQ(response.head.status.value(), u16(206));
    EXPECT_EQ(*response.content_length, u64(7));
    int notified {};
    response.on_finish([&](const ResponseReport& report) {
        ++notified;
        EXPECT_EQ(report.outcome, ResponseOutcome::Complete);
        EXPECT_LT(fcntl(raw, F_GETFD), 0);
    });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto bytes   = runtime.block_on(drain_file(response, usize(2)));
    EXPECT_TRUE(bytes.as_slice() == "2345678"_bytes);
    EXPECT_LT(fcntl(raw, F_GETFD), 0);
    EXPECT_EQ(duplicate.seek(rstd::io::SeekFrom::from_current(i64())).unwrap(), u64(5));
    EXPECT_TRUE(
        runtime.block_on(response.close({ ResponseOutcome::Complete, u64(7), {} })).is_ok());
    EXPECT_EQ(notified, 1);
}
TEST(FileResponse, HeadAndConditionsCloseWithoutStartingReader) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-head"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    for (unsigned mode = 0; mode < 4; ++mode) {
        int               raw {};
        auto              prepared = prepared_file(path.as_path(), raw);
        lihttpto::Headers headers;
        if (mode == 1)
            headers.push(
                lihttpto::Header::make("if-none-match"_str, prepared.metadata().etag->as_bytes())
                    .unwrap());
        if (mode == 2) headers.add("if-match"_str, "\"other\""_str).unwrap();
        if (mode == 3) headers.add("range"_str, "bytes=10-"_str).unwrap();
        auto conditions = lihttpto::RequestConditions::parse(headers).unwrap();
        auto response =
            rstd::move(prepared).respond(mode == 0 ? "HEAD"_str : "GET"_str, conditions).unwrap();
        EXPECT_TRUE(response.body.is_empty());
        EXPECT_LT(fcntl(raw, F_GETFD), 0);
        auto status = array<u16, 4> { u16(200), u16(304), u16(412), u16(416) }[usize(mode)];
        EXPECT_EQ(response.head.status.value(), status);
    }
}
TEST(FileResponse, ConfinedEntryReusesOpenedMetadata) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-entry"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    auto root  = ConfinedDir::open(directory.path()).unwrap();
    auto entry = root.open_entry("file"_str).unwrap();
    rstd::fs::write(path.as_path(), "abc"_bytes).unwrap();
    auto prepared = FileResponse::prepare(rstd::move(entry)).unwrap();
    EXPECT_EQ(prepared.metadata().length, u64(10));
    auto conditions = lihttpto::RequestConditions::parse(lihttpto::Headers {}).unwrap();
    auto response   = rstd::move(prepared).respond("GET"_str, conditions).unwrap();
    auto runtime    = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_EQ(runtime.block_on(response.body.collect(u64(10))).unwrap_err(), BodyError::Source);
}
TEST(FileResponse, TruncationReadErrorAndCollectionLimitReleaseFile) {
    auto directory  = rstd::fs::TempDir::make("liserveto-file-errors"_str).unwrap();
    auto path       = file_path(directory);
    auto runtime    = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto conditions = lihttpto::RequestConditions::parse(lihttpto::Headers {}).unwrap();
    for (unsigned mode = 0; mode < 3; ++mode) {
        rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
        auto file = mode == 1 ? rstd::fs::File::options().write(true).open(path.as_path()).unwrap()
                              : rstd::fs::File::open(path.as_path()).unwrap();
        auto raw  = file.as_raw_fd();
        auto prepared = FileResponse::prepare(rstd::move(file)).unwrap();
        if (mode == 0) rstd::fs::write(path.as_path(), "abc"_bytes).unwrap();
        auto response = rstd::move(prepared).respond("GET"_str, conditions, {}, usize(2)).unwrap();
        auto result   = runtime.block_on(response.body.collect(mode == 2 ? u64(3) : u64(10)));
        EXPECT_TRUE(result.is_err());
        if (result.is_err())
            EXPECT_EQ(result.unwrap_err(),
                      mode == 0   ? BodyError::Source
                      : mode == 1 ? BodyError::Io
                                  : BodyError::LimitExceeded);
        EXPECT_LT(fcntl(raw, F_GETFD), 0);
    }
}
TEST(FileResponse, CancelledWaitThenMovedResponseClosesBeforeObserver) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-cancel"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    int  raw {}, notified {};
    auto prepared   = prepared_file(path.as_path(), raw);
    auto conditions = lihttpto::RequestConditions::parse(lihttpto::Headers {}).unwrap();
    auto response   = rstd::move(prepared).respond("GET"_str, conditions).unwrap();
    response.on_finish([&](const ResponseReport& report) {
        ++notified;
        EXPECT_EQ(report.outcome, ResponseOutcome::Cancelled);
        EXPECT_LT(fcntl(raw, F_GETFD), 0);
    });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto waiting = runtime.block_on(
        rstd::async::timeout(rstd::stream::next(response.body), rstd::time::Duration {}));
    EXPECT_TRUE(waiting.is_err());
    EXPECT_GE(fcntl(raw, F_GETFD), 0);
    auto moved = rstd::move(response);
    EXPECT_TRUE(runtime.block_on(moved.close()).is_ok());
    EXPECT_EQ(notified, 1);
}

struct HeldFile {
    using Item = BodyItem;
    Body              body;
    Shutdown          polled;
    Option<BodyClose> closing;
    bool              observed {};
    int*              close_polls {};
    int               descriptor { -1 };
    auto              poll_next(mut_ref<HeldFile> self, Context& cx) -> Poll<Option<Item>> {
        if (! self->observed) {
            auto result = rstd::future::poll_next(self->body, cx);
            EXPECT_TRUE(result.is_pending());
            self->observed = true;
            self->polled.request_stop().unwrap();
        }
        return Poll<Option<Item>>::Pending();
    }
    auto poll_close(mut_ref<HeldFile> self, Context& cx) -> Poll<Result<empty, BodyError>> {
        ++*self->close_polls;
        if (self->closing.is_none()) self->closing.insert(self->body.close());
        auto result = rstd::future::poll(*self->closing, cx);
        if (result.is_pending()) return result;
        auto closed = rstd::move(result).take();
        EXPECT_TRUE(closed.is_ok());
        EXPECT_LT(fcntl(self->descriptor, F_GETFD), 0);
        return Poll<Result<empty, BodyError>>::Ready(rstd::move(closed));
    }
};
struct FileWire {
    Bytes    input;
    BytesMut output;
    bool     fail_write {};
};
struct FileIO {
    rstd::sync::Arc<FileWire> wire;
    auto                      poll_read(mut_ref<FileIO> self, Context&, BytesMut& output)
        -> Poll<rstd::io::Result<usize>> {
        auto input = rstd::exchange(self->wire->input, Bytes {});
        output.put_slice(input.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(input.len()));
    }
    auto poll_write(mut_ref<FileIO> self, Context&, const Bytes& input)
        -> Poll<rstd::io::Result<usize>> {
        if (self->wire->fail_write)
            return Poll<rstd::io::Result<usize>>::Ready(Err(rstd::io::error::Error::from_kind(
                rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::BrokenPipe })));
        self->wire->output.put_slice(input.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(input.len()));
    }
    auto poll_flush(mut_ref<FileIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
    auto poll_shutdown(mut_ref<FileIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
};
auto abort_file_connection(FileResponse prepared, int raw, int& notified)
    -> rstd::async::coro<bool> {
    auto polled = Shutdown::make().unwrap();
    int  close_polls {};
    auto stored =
        rstd::sync::Arc<rstd::sync::Mutex<Option<FileResponse>>>::make(Some(rstd::move(prepared)));
    auto endpoint = Endpoint::make(
        [stored = rstd::move(stored), polled = polled.clone(), raw, &notified, &close_polls](
            Request request) -> rstd::async::coro<ResponseResult> {
            auto conditions = lihttpto::RequestConditions::parse(request.head().headers).unwrap();
            auto file       = stored->lock().unwrap()->take();
            auto response   = rstd::move(file).unwrap().respond("GET"_str, conditions).unwrap();
            response.body   = Body::from_stream(HeldFile {
                rstd::move(response.body), polled.clone(), {}, false, &close_polls, raw });
            response.on_finish([raw, &notified, &close_polls](const ResponseReport& report) {
                ++notified;
                EXPECT_EQ(report.outcome, ResponseOutcome::Cancelled);
                EXPECT_LT(fcntl(raw, F_GETFD), 0);
                EXPECT_TRUE(close_polls > 0);
            });
            co_return Ok(rstd::move(response));
        });
    auto wire = rstd::sync::Arc<FileWire>::make();
    wire->input =
        Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"_bytes);
    auto task = ConnectionTask::spawn(FileIO { wire.clone() },
                                      rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                      Shutdown::make().unwrap());
    co_await polled.notified();
    EXPECT_EQ(notified, 0);
    task.abort();
    auto result = co_await rstd::future::by_ref(task);
    co_return result.is_err() && result.unwrap_err().is_aborted() && notified == 1 &&
        fcntl(raw, F_GETFD) < 0;
}
TEST(FileResponse, ConnectionTaskAbortDrainsPendingFileBeforeNotification) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-abort"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    int  raw {}, notified {};
    auto prepared = prepared_file(path.as_path(), raw);
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result   = runtime.block_on(rstd::async::timeout(
        rstd::async::into_future(abort_file_connection(rstd::move(prepared), raw, notified)),
        rstd::time::Duration::from_secs(u64(2))));
    EXPECT_TRUE(result.is_ok());
    if (result.is_ok()) EXPECT_TRUE(*result);
}

auto file_endpoint(FileResponse prepared, int raw, int& notified, ResponseOutcome outcome)
    -> Endpoint {
    auto stored =
        rstd::sync::Arc<rstd::sync::Mutex<Option<FileResponse>>>::make(Some(rstd::move(prepared)));
    return Endpoint::make([stored = rstd::move(stored), raw, &notified, outcome](
                              Request request) -> rstd::async::coro<ResponseResult> {
        auto conditions = lihttpto::RequestConditions::parse(request.head().headers).unwrap();
        auto file       = stored->lock().unwrap()->take();
        auto response = rstd::move(file)
                            .unwrap()
                            .respond(request.head().line.method.as_str(), conditions, {}, usize(2))
                            .unwrap();
        response.on_finish([raw, &notified, outcome](const ResponseReport& report) {
            ++notified;
            EXPECT_EQ(report.outcome, outcome);
            EXPECT_LT(fcntl(raw, F_GETFD), 0);
        });
        co_return Ok(rstd::move(response));
    });
}
TEST(FileResponse, HeaderWriteFailureClosesUnstartedFileBeforeNotification) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-write"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    int  raw {}, notified {};
    auto prepared = prepared_file(path.as_path(), raw);
    auto endpoint = file_endpoint(rstd::move(prepared), raw, notified, ResponseOutcome::WriteError);
    auto wire     = rstd::sync::Arc<FileWire>::make();
    wire->input   = Bytes::copy_from_slice("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_bytes);
    wire->fail_write = true;
    auto runtime     = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_FALSE(
        runtime.block_on(serve_connection(FileIO { wire.clone() },
                                          rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)),
                                          Shutdown::make().unwrap(),
                                          {})));
    EXPECT_EQ(notified, 1);
}
auto tcp_file_response(rstd::net::TcpListener& listener, rstd::sync::Arc<Endpoint> endpoint)
    -> rstd::async::coro<bool> {
    auto connected = co_await rstd::net::TcpStream::connect(listener.local_addr().unwrap());
    if (connected.is_err()) co_return false;
    auto client   = rstd::move(connected).unwrap();
    auto accepted = co_await listener.accept();
    if (accepted.is_err()) co_return false;
    auto task = rstd::async::AbortOnDropHandle(
        rstd::async::spawn(serve_connection(rstd::move(accepted).unwrap().get<0>(),
                                            rstd::move(endpoint),
                                            Shutdown::make().unwrap(),
                                            {})));
    auto request = Bytes::copy_from_slice(
        "GET /file HTTP/1.1\r\nHost: x\r\nRange: bytes=2-4\r\nConnection: close\r\n\r\n"_bytes);
    if ((co_await rstd::async::io::write_all(client, request)).is_err()) co_return false;
    BytesMut output;
    for (;;) {
        auto part = BytesMut::with_capacity(usize(64));
        auto read = co_await rstd::async::io::read(client, part);
        if (read.is_err()) co_return false;
        if (*read == usize()) break;
        output.put_slice(part.as_slice());
        if (output.len() > usize(4096)) co_return false;
    }
    auto joined = co_await rstd::move(task);
    if (joined.is_err() || ! *joined) co_return false;
    auto text = rstd::str_::from_utf8(output.as_slice()).unwrap();
    co_return text.starts_with("HTTP/1.1 206 "_str) &&
        text.contains("content-range: bytes 2-4/10\r\n"_str) && text.ends_with("\r\n\r\n234"_str);
}
TEST(FileResponse, TcpLoopbackSendsOnlySelectedFileRange) {
    auto directory = rstd::fs::TempDir::make("liserveto-file-tcp"_str).unwrap();
    auto path      = file_path(directory);
    rstd::fs::write(path.as_path(), "0123456789"_bytes).unwrap();
    int  raw {}, notified {};
    auto prepared = prepared_file(path.as_path(), raw);
    auto endpoint = file_endpoint(rstd::move(prepared), raw, notified, ResponseOutcome::Complete);
    auto listener =
        rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto result  = runtime.block_on(
        rstd::async::timeout(rstd::async::into_future(tcp_file_response(
                                 listener, rstd::sync::Arc<Endpoint>::make(rstd::move(endpoint)))),
                             rstd::time::Duration::from_secs(u64(2))));
    EXPECT_TRUE(result.is_ok());
    if (result.is_ok()) EXPECT_TRUE(*result);
    EXPECT_EQ(notified, 1);
}
#endif
