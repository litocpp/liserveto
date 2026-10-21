#include <rstd/test/gtest.hpp>
#include <rstd/macro.hpp>
#if RSTD_OS_UNIX
#include <fcntl.h>
#include <sys/stat.h>
#endif
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using rstd::bytes::Bytes;
using rstd::bytes::BytesMut;
using rstd::task::Context;
using rstd::task::Poll;
using rstd::sync::Arc;

#if RSTD_OS_LINUX || (RSTD_OS_MACOS && defined(O_RESOLVE_BENEATH) && defined(O_NOFOLLOW_ANY))
struct StaticWire {
    Bytes    input;
    BytesMut output;
};
struct StaticIO {
    Arc<StaticWire> wire;
    auto            poll_read(mut_ref<StaticIO> self, Context&, BytesMut& output)
        -> Poll<rstd::io::Result<usize>> {
        auto input = rstd::exchange(self->wire->input, Bytes {});
        output.put_slice(input.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(input.len()));
    }
    auto poll_write(mut_ref<StaticIO> self, Context&, const Bytes& input)
        -> Poll<rstd::io::Result<usize>> {
        self->wire->output.put_slice(input.as_slice());
        return Poll<rstd::io::Result<usize>>::Ready(Ok(input.len()));
    }
    auto poll_flush(mut_ref<StaticIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
    auto poll_shutdown(mut_ref<StaticIO>, Context&) -> Poll<rstd::io::Result<empty>> {
        return Poll<rstd::io::Result<empty>>::Ready(Ok(empty {}));
    }
};
auto static_wire(ref<str> target, ref<str> method, ref<str> headers) -> Arc<StaticWire> {
    auto wire = Arc<StaticWire>::make();
    auto text = rstd::format(
        "{} {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n{}\r\n", method, target, headers);
    wire->input = Bytes::copy_from_slice(text.as_str().as_bytes());
    return wire;
}
auto static_exchange(rstd::async::Runtime& runtime,
                     const Endpoint&       handler,
                     ref<str>              target,
                     ref<str>              method  = "GET"_str,
                     ref<str>              headers = ""_str) -> String {
    auto wire = static_wire(target, method, headers);
    EXPECT_TRUE(runtime.block_on(serve_connection(StaticIO { wire.clone() },
                                                  Arc<Endpoint>::make(handler.clone()),
                                                  Shutdown::make().unwrap(),
                                                  {})));
    return String::make(rstd::str_::from_utf8(wire->output.as_slice()).unwrap());
}
auto static_tag(ref<str> response) -> String {
    for (;;) {
        auto split = response.split_once("\r\n"_str);
        if (split.is_none()) break;
        auto line = split->template get<0>();
        auto tag  = line.strip_prefix("etag: "_str);
        if (tag.is_some()) return String::make(*tag);
        response = split->template get<1>();
    }
    return {};
}
#ifdef LITO_FEAT_RESOURCES
static constexpr unsigned char embedded_data[] =
    "index.htmlhome"
    "1111111111111111111111111111111111111111111111111111111111111111"
    "main.jsscript"
    "sub/file.txtchild";
static constexpr liresto::ResourceIndex embedded_index[] = {
    { usize(0), usize(10), usize(10), usize(4), usize(14), usize(64) },
    { usize(78), usize(7), usize(85), usize(6), usize(14), usize(64) },
    { usize(91), usize(12), usize(103), usize(5), usize(14), usize(64) },
};
TEST(StaticFiles, EmbeddedResourcesReuseHttpPolicy) {
    auto bundle =
        liresto::ResourceBundle::from_static<embedded_data, embedded_index>(usize(3)).unwrap();
    StaticFilesOptions options;
    options.spa = Some(SpaFallback {});
    options.spa->routes.push("view"_Str);
    auto files    = StaticFiles::make(rstd::move(bundle), rstd::move(options)).unwrap();
    auto endpoint = files.endpoint();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto home     = static_exchange(runtime, endpoint, "/"_str);
    EXPECT_TRUE(home.as_str().ends_with("\r\n\r\nhome"_str));
    EXPECT_TRUE(home.as_str().contains("content-type: text/html; charset=utf-8"_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/main.js?v=1"_str)
                    .as_str()
                    .ends_with("\r\n\r\nscript"_str));
    auto tag         = static_tag(home.as_str());
    auto conditional = rstd::format("If-None-Match: {}\r\n", tag);
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/"_str, "GET"_str, conditional.as_str())
                    .as_str()
                    .contains("HTTP/1.1 304 "_str));
    auto partial =
        static_exchange(runtime, endpoint, "/main.js"_str, "GET"_str, "Range: bytes=1-3\r\n"_str);
    EXPECT_TRUE(partial.as_str().contains("HTTP/1.1 206 "_str));
    EXPECT_TRUE(partial.as_str().ends_with("\r\n\r\ncri"_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/main.js"_str, "HEAD"_str)
                    .as_str()
                    .ends_with("\r\n\r\n"_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/main.js"_str, "GET"_str, "Range: bytes=99-\r\n"_str)
            .as_str()
            .contains("HTTP/1.1 416 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/view"_str).as_str().ends_with("\r\n\r\nhome"_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/api/missing"_str)
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/main.js/"_str).as_str().contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/%2e%2e/main.js"_str)
                    .as_str()
                    .contains("HTTP/1.1 400 "_str));
    auto navigation = files.navigation_endpoint().unwrap();
    EXPECT_TRUE(static_exchange(runtime, navigation, "/missing/"_str)
                    .as_str()
                    .ends_with("\r\n\r\nhome"_str));
    EXPECT_TRUE(
        static_exchange(runtime, navigation, "/sub/"_str).as_str().contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(static_exchange(runtime, navigation, "/main.js/"_str)
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
}
#endif
TEST(StaticFiles, MountIndexMimeAndSingleDecodedPath) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::create_dir_all(base.join("sub"_str).as_path()).unwrap();
    rstd::fs::write(base.join("index.html"_str).as_path(), "home"_bytes).unwrap();
    rstd::fs::write(base.join("sub/index.html"_str).as_path(), "child"_bytes).unwrap();
    rstd::fs::write(base.join("main.JS"_str).as_path(), "script"_bytes).unwrap();
    rstd::fs::write(base.join("unknown"_str).as_path(), "binary"_bytes).unwrap();
    rstd::fs::write(base.join("empty.txt"_str).as_path(), ""_bytes).unwrap();
    rstd::fs::write(base.join("%2e%2e.txt"_str).as_path(), "literal"_bytes).unwrap();
    auto   files = StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap()).unwrap();
    Router routes;
    routes.mount("/assets"_str, files.endpoint()).unwrap();
    auto router  = rstd::move(routes).into_endpoint();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (auto path : array<ref<str>, 2> { "/assets"_str, "/assets/"_str }) {
        auto response = static_exchange(runtime, router, path);
        EXPECT_TRUE(response.as_str().contains("HTTP/1.1 200 "_str));
        EXPECT_TRUE(response.as_str().contains("content-type: text/html; charset=utf-8\r\n"_str));
        EXPECT_TRUE(response.as_str().ends_with("\r\n\r\nhome"_str));
    }
    auto child = static_exchange(runtime, router, "/assets/sub/"_str);
    EXPECT_TRUE(child.as_str().ends_with("\r\n\r\nchild"_str));
    auto script = static_exchange(runtime, router, "/assets/main.JS?v=1"_str);
    EXPECT_TRUE(script.as_str().contains("content-type: text/javascript; charset=utf-8\r\n"_str));
    EXPECT_TRUE(script.as_str().contains("x-content-type-options: nosniff\r\n"_str));
    EXPECT_TRUE(script.as_str().ends_with("\r\n\r\nscript"_str));
    auto binary = static_exchange(runtime, router, "/assets/unknown"_str);
    EXPECT_TRUE(binary.as_str().contains("content-type: application/octet-stream\r\n"_str));
    auto literal = static_exchange(runtime, router, "/assets/%252e%252e.txt"_str);
    EXPECT_TRUE(literal.as_str().ends_with("\r\n\r\nliteral"_str));
    auto empty_file = static_exchange(runtime, router, "/assets/empty.txt"_str);
    EXPECT_TRUE(empty_file.as_str().contains("Content-Length: 0\r\n"_str));
    EXPECT_TRUE(empty_file.as_str().ends_with("\r\n\r\n"_str));
    EXPECT_TRUE(static_exchange(runtime, router, "/assets/main.JS/"_str)
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(static_exchange(runtime, router, "/assetsx/index.html"_str)
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
}
TEST(StaticFiles, RejectsDangerousSegmentsDotfilesAndSymlinks) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-paths"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::write(base.join(".env"_str).as_path(), "secret"_bytes).unwrap();
    rstd::fs::soft_link(".env"_str, base.join("link"_str).as_path()).unwrap();
    auto fifo = base.join("fifo"_str).as_path().to_cstring().unwrap();
    ASSERT_EQ(mkfifo(fifo.as_ptr(), 0600), 0);
    auto files    = StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap()).unwrap();
    auto endpoint = files.endpoint();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    for (auto target : array<ref<str>, 8> { "/%2e%2e/.env"_str,
                                            "/./file"_str,
                                            "/a%2fb"_str,
                                            "/a%5cb"_str,
                                            "/%00"_str,
                                            "/C%3a/foo"_str,
                                            "/a//b"_str,
                                            "/%ff"_str })
        EXPECT_TRUE(
            static_exchange(runtime, endpoint, target).as_str().contains("HTTP/1.1 400 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/.env"_str).as_str().contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/link"_str).as_str().contains("HTTP/1.1 403 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/fifo"_str).as_str().contains("HTTP/1.1 403 "_str));
    auto long_name = String::make("/"_str);
    for (int i = 0; i < 256; ++i) long_name.push_ascii('x');
    EXPECT_TRUE(static_exchange(runtime, endpoint, long_name.as_str())
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
}
TEST(StaticFiles, ConditionsRangeAndHeadUseFileRepresentation) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-range"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::write(base.join("file.txt"_str).as_path(), "0123456789"_bytes).unwrap();
    auto files    = StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap()).unwrap();
    auto endpoint = files.endpoint();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto full     = static_exchange(runtime, endpoint, "/file.txt"_str);
    auto tag      = static_tag(full.as_str());
    EXPECT_FALSE(tag.is_empty());
    auto range =
        static_exchange(runtime, endpoint, "/file.txt"_str, "GET"_str, "Range: bytes=2-4\r\n"_str);
    EXPECT_TRUE(range.as_str().contains("HTTP/1.1 206 "_str));
    EXPECT_TRUE(range.as_str().contains("content-range: bytes 2-4/10\r\n"_str));
    EXPECT_TRUE(range.as_str().ends_with("\r\n\r\n234"_str));
    auto head =
        static_exchange(runtime, endpoint, "/file.txt"_str, "HEAD"_str, "Range: bytes=2-4\r\n"_str);
    EXPECT_TRUE(head.as_str().contains("HTTP/1.1 200 "_str));
    EXPECT_TRUE(head.as_str().contains("Content-Length: 10\r\n"_str));
    EXPECT_TRUE(head.as_str().ends_with("\r\n\r\n"_str));
    auto condition = rstd::format("If-None-Match: {}\r\n", tag);
    auto cached =
        static_exchange(runtime, endpoint, "/file.txt"_str, "GET"_str, condition.as_str());
    EXPECT_TRUE(cached.as_str().contains("HTTP/1.1 304 "_str));
    EXPECT_FALSE(cached.as_str().contains("Content-Length:"_str));
    EXPECT_TRUE(cached.as_str().ends_with("\r\n\r\n"_str));
}
TEST(StaticFiles, SpaFallbackIsExplicitAndDoesNotHideOtherFailures) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-spa"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::write(base.join("index.html"_str).as_path(), "spa"_bytes).unwrap();
    rstd::fs::soft_link("index.html"_str, base.join("denied"_str).as_path()).unwrap();
    rstd::fs::create_dir(base.join("empty"_str).as_path()).unwrap();
    StaticFilesOptions options;
    SpaFallback        spa;
    spa.routes.push(String::make("app/settings"_str));
    spa.routes.push(String::make("denied"_str));
    spa.routes.push(String::make("empty"_str));
    options.spa.insert(rstd::move(spa));
    auto files =
        StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap(), rstd::move(options)).unwrap();
    Router routes;
    routes.mount("/site"_str, files.endpoint()).unwrap();
    auto router  = rstd::move(routes).into_endpoint();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto page    = static_exchange(runtime, router, "/site/app/settings"_str);
    EXPECT_TRUE(page.as_str().ends_with("\r\n\r\nspa"_str));
    for (auto target : array<ref<str>, 4> {
             "/site/missing.js"_str, "/site/api/users"_str, "/api/users"_str, "/site/empty"_str })
        EXPECT_TRUE(
            static_exchange(runtime, router, target).as_str().contains("HTTP/1.1 404 "_str));
    auto denied = static_exchange(runtime, router, "/site/denied"_str);
    EXPECT_TRUE(denied.as_str().contains("HTTP/1.1 403 "_str));
    auto post = static_exchange(runtime, router, "/site/app/settings"_str, "POST"_str);
    EXPECT_TRUE(post.as_str().contains("HTTP/1.1 405 "_str));
    EXPECT_TRUE(post.as_str().contains("allow: GET, HEAD\r\n"_str));
}
TEST(StaticFiles, InvalidConfigurationAndDisabledIndex) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-options"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    for (auto name :
         array<ref<str>, 3> { "../index.html"_str, "sub/index.html"_str, ".hidden"_str }) {
        StaticFilesOptions options;
        options.index.insert(String::make(name));
        EXPECT_TRUE(
            StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap(), rstd::move(options))
                .is_err());
    }
    StaticFilesOptions options;
    options.index    = None();
    options.dotfiles = true;
    rstd::fs::write(base.join(".env"_str).as_path(), "visible"_bytes).unwrap();
    auto files =
        StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap(), rstd::move(options)).unwrap();
    auto endpoint = files.endpoint();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/"_str).as_str().contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/.env"_str).as_str().ends_with("\r\n\r\nvisible"_str));
}

TEST(StaticFiles, NavigationRequiresSpaAndUsesRouterParametersWithoutRewritingTarget) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-navigation"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::write(base.join("index.html"_str).as_path(), "spa"_bytes).unwrap();
    rstd::fs::create_dir_all(base.join("chat"_str).as_path()).unwrap();
    rstd::fs::write(base.join("chat/existing"_str).as_path(), "file"_bytes).unwrap();
    rstd::fs::soft_link("../index.html"_str, base.join("chat/denied"_str).as_path()).unwrap();
    rstd::fs::create_dir(base.join("chat/empty"_str).as_path()).unwrap();
    auto plain = StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap()).unwrap();
    EXPECT_TRUE(plain.navigation_endpoint().is_err());
    StaticFilesOptions options;
    options.spa.insert(SpaFallback {});
    auto files =
        StaticFiles::make(ConfinedDir::open(base.as_path()).unwrap(), rstd::move(options)).unwrap();
    auto   navigation = files.navigation_endpoint().unwrap();
    Router site;
    site.get("/chat/{chat_id}"_str, navigation.clone()).unwrap();
    site.get("/project/{project_id}/chat/{session_id}"_str, navigation.clone()).unwrap();
    site.fallback(files.endpoint());
    Router routes;
    routes.nest("/site"_str, rstd::move(site)).unwrap();
    auto endpoint = rstd::move(routes).into_endpoint();
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto page     = static_exchange(runtime, endpoint, "/site/chat/a%20b?view=1"_str);
    EXPECT_TRUE(page.as_str().ends_with("\r\n\r\nspa"_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/project/p/chat/s"_str)
                    .as_str()
                    .ends_with("\r\n\r\nspa"_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/chat/existing"_str)
                    .as_str()
                    .ends_with("\r\n\r\nfile"_str));
    for (auto path : array<ref<str>, 3> {
             "/site/chat/empty"_str, "/site/missing.js"_str, "/site/api/missing"_str })
        EXPECT_TRUE(
            static_exchange(runtime, endpoint, path).as_str().contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/chat/denied"_str)
                    .as_str()
                    .contains("HTTP/1.1 403 "_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/chat/a%2fb"_str)
                    .as_str()
                    .contains("HTTP/1.1 400 "_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/chat/.hidden"_str)
                    .as_str()
                    .contains("HTTP/1.1 404 "_str));
    EXPECT_TRUE(static_exchange(runtime, endpoint, "/site/chat/id"_str, "POST"_str)
                    .as_str()
                    .contains("HTTP/1.1 405 "_str));
    auto head = static_exchange(runtime, endpoint, "/site/chat/id"_str, "HEAD"_str);
    EXPECT_TRUE(head.as_str().contains("Content-Length: 3\r\n"_str));
    EXPECT_TRUE(head.as_str().ends_with("\r\n\r\n"_str));
    auto range = static_exchange(
        runtime, endpoint, "/site/chat/id"_str, "GET"_str, "Range: bytes=1-2\r\n"_str);
    EXPECT_TRUE(range.as_str().contains("HTTP/1.1 206 "_str));
    EXPECT_TRUE(range.as_str().ends_with("\r\n\r\npa"_str));
    auto condition = rstd::format("If-None-Match: {}\r\n", static_tag(page.as_str()));
    EXPECT_TRUE(
        static_exchange(runtime, endpoint, "/site/chat/id"_str, "GET"_str, condition.as_str())
            .as_str()
            .contains("HTTP/1.1 304 "_str));
}

struct BlockingFileGate {
    rstd::sync::atomic::Atomic<bool>           entered { false }, release { false };
    rstd::sync::atomic::Atomic<int>            descriptor { -1 };
    rstd::sync::Mutex<Option<RequestLifetime>> lifetime;
};
auto abort_static_preparation(ConfinedDir root) -> rstd::async::coro<bool> {
    auto gate      = Arc<BlockingFileGate>::make();
    auto directory = Arc<ConfinedDir>::make(rstd::move(root));
    auto endpoint  = Endpoint::make([gate = gate.clone(), directory = rstd::move(directory)](
                                        Request request) -> rstd::async::coro<ResponseResult> {
        gate->lifetime.lock().unwrap()->insert(request.lifetime.clone());
        auto result =
            co_await request.run_blocking([gate = gate.clone(), directory = directory.clone()] {
                auto file = directory->open_file("file"_str).unwrap();
                gate->descriptor.store(file.as_raw_fd());
                gate->entered.store(true);
                while (! gate->release.load()) rstd::hint::spin_loop();
                return FileResponse::prepare(rstd::move(file)).unwrap();
            });
        EXPECT_TRUE(result.is_ok());
        co_return Ok(Response {});
    });
    auto wire      = static_wire("/"_str, "GET"_str, ""_str);
    auto task      = ConnectionTask::spawn(StaticIO { wire.clone() },
                                           Arc<Endpoint>::make(rstd::move(endpoint)),
                                           Shutdown::make().unwrap());
    while (! gate->entered.load()) co_await rstd::async::yield_now();
    task.abort();
    auto premature = co_await rstd::async::timeout(rstd::future::by_ref(task),
                                                   rstd::time::Duration::from_millis(u64(1)));
    EXPECT_TRUE(premature.is_err());
    if (premature.is_ok()) {
        gate->release.store(true);
        co_return false;
    }
    EXPECT_TRUE(gate->lifetime.lock().unwrap()->as_ref().unwrap().report().is_none());
    EXPECT_GE(fcntl(gate->descriptor.load(), F_GETFD), 0);
    gate->release.store(true);
    auto finished = co_await rstd::future::by_ref(task);
    auto report   = gate->lifetime.lock().unwrap()->as_ref().unwrap().report();
    co_return finished.is_err() && finished.unwrap_err().is_aborted() && report.is_some() &&
        report->outcome == ResponseOutcome::Cancelled&& fcntl(gate->descriptor.load(), F_GETFD) < 0;
}
TEST(StaticFiles, CancelledPreparationWaitsForWorkerAndReleasesUnclaimedFile) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-cancel"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    rstd::fs::write(base.join("file"_str).as_path(), "file"_bytes).unwrap();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    EXPECT_TRUE(
        runtime.block_on(abort_static_preparation(ConfinedDir::open(base.as_path()).unwrap())));
}
TEST(StaticFiles, UnmanagedRequestRejectsBlockingWorkBeforeSubmission) {
    lihttpto::RequestHeadDecoder decoder;
    decoder.feed("GET / HTTP/1.1\r\nHost: x\r\n\r\n"_bytes).unwrap();
    auto request = Request(
        decoder.take().unwrap(), IncomingBody::from_body(Body {}), Shutdown::make().unwrap());
    auto ran     = Arc<rstd::sync::atomic::Atomic<bool>>::make(false);
    auto future  = request.run_blocking([ran = ran.clone()] {
        ran->store(true);
        return 1;
    });
    auto runtime = rstd::async::RuntimeBuilder::current_thread().build().unwrap();
    auto result  = runtime.block_on(rstd::move(future));
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind(),
              (rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::NotConnected }));
    EXPECT_FALSE(ran->load());
}
auto cancel_queued_file(rstd::fs::File file) -> rstd::async::coro<bool> {
    auto gate = Arc<BlockingFileGate>::make();
    auto busy = rstd::async::spawn_blocking([gate = gate.clone()] {
                    gate->entered.store(true);
                    while (! gate->release.load()) rstd::hint::spin_loop();
                }).unwrap();
    while (! gate->entered.load()) co_await rstd::async::yield_now();
    auto raw       = file.as_raw_fd();
    auto stored    = Arc<rstd::sync::Mutex<Option<rstd::fs::File>>>::make(Some(rstd::move(file)));
    auto submitted = Shutdown::make().unwrap();
    auto ran       = Arc<rstd::sync::atomic::Atomic<bool>>::make(false);
    auto endpoint =
        Endpoint::make([gate      = gate.clone(),
                        stored    = rstd::move(stored),
                        submitted = submitted.clone(),
                        ran = ran.clone()](Request request) -> rstd::async::coro<ResponseResult> {
            gate->lifetime.lock().unwrap()->insert(request.lifetime.clone());
            submitted.request_stop().unwrap();
            auto result = co_await request.run_blocking(
                [file = stored->lock().unwrap()->take().unwrap(), ran = ran.clone()]() mutable {
                    ran->store(true);
                    return FileResponse::prepare(rstd::move(file)).unwrap();
                });
            co_return Err(Error { ErrorKind::Internal });
        });
    auto wire = static_wire("/"_str, "GET"_str, ""_str);
    auto task = ConnectionTask::spawn(StaticIO { wire.clone() },
                                      Arc<Endpoint>::make(rstd::move(endpoint)),
                                      Shutdown::make().unwrap());
    co_await submitted.notified();
    task.abort();
    auto finished = co_await rstd::future::by_ref(task);
    auto report   = gate->lifetime.lock().unwrap()->as_ref().unwrap().report();
    bool closed   = fcntl(raw, F_GETFD) < 0;
    gate->release.store(true);
    EXPECT_TRUE((co_await rstd::move(busy)).is_ok());
    co_return finished.is_err() && finished.unwrap_err().is_aborted() && report.is_some() &&
        report->outcome == ResponseOutcome::Cancelled && ! ran->load() && closed;
}
TEST(StaticFiles, QueuedCancellationClosesCapturedFileWithoutWaitingForUnrelatedJob) {
    auto temporary = rstd::fs::TempDir::make("liserveto-static-queued"_str).unwrap();
    auto base      = rstd::path::PathBuf::from(temporary.path());
    auto path      = base.join("file"_str);
    rstd::fs::write(path.as_path(), "file"_bytes).unwrap();
    auto runtime = rstd::async::RuntimeBuilder::current_thread()
                       .enable_all()
                       .max_blocking_threads(usize(1))
                       .build()
                       .unwrap();
    EXPECT_TRUE(
        runtime.block_on(cancel_queued_file(rstd::fs::File::open(path.as_path()).unwrap())));
}
#endif
