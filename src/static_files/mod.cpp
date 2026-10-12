module;
#include <rstd/macro.hpp>
module liserveto;
import :static_files;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using IoError     = rstd::io::error::Error;
using IoErrorKind = rstd::io::error::ErrorKind;
using rstd::sync::Arc;

struct StaticFilesState {
    ConfinedDir        root;
    StaticFilesOptions options;
    StaticFilesState(ConfinedDir root, StaticFilesOptions options)
        : root(rstd::move(root)), options(rstd::move(options)) {}
};
auto valid_static_segment(slice<u8> segment) -> bool {
    if (segment.is_empty() || segment == "."_bytes || segment == ".."_bytes) return false;
    for (auto byte : segment)
        if (byte == u8() || byte == u8('/') || byte == u8('\\') || byte == u8(':')) return false;
    return rstd::str_::from_utf8(segment).is_ok();
}
auto valid_static_path(ref<str> path, bool dotfiles) -> bool {
    if (path.is_empty()) return false;
    for (;;) {
        auto split = path.split_once("/"_str);
        auto part  = split.is_some() ? split->template get<0>() : path;
        if (! valid_static_segment(part.as_bytes())) return false;
        if (! dotfiles && part.as_bytes()[usize()] == u8('.')) return false;
        if (split.is_none()) break;
        path = split->template get<1>();
    }
    return true;
}
struct StaticPath {
    String relative;
    bool   directory {};
};
auto static_path(const Request& request, bool dotfiles) -> Result<StaticPath, u16> {
    const auto form = request.head().line.resource.form;
    if (form != lihttpto::TargetForm::Origin && form != lihttpto::TargetForm::Absolute)
        return Err(u16(400));
    StaticPath mapped;
    for (auto index = usize(); index < request.path_size(); ++index) {
        auto segment = request.path_segment(index);
        if (segment.is_empty() && index + usize(1) == request.path_size()) {
            mapped.directory = true;
            continue;
        }
        if (! valid_static_segment(segment)) return Err(u16(400));
        if (! dotfiles && segment[usize()] == u8('.')) return Err(u16(404));
        if (! mapped.relative.is_empty()) mapped.relative.push(U'/');
        mapped.relative.push_str(rstd::str_::from_utf8(segment).unwrap());
    }
    return Ok(rstd::move(mapped));
}
auto static_mime(ref<str> name) -> ref<str> {
    auto native    = rstd::path::PathBuf::from(name);
    auto extension = native.as_path().extension();
    if (extension.is_none()) return "application/octet-stream"_str;
    auto text = extension->to_str();
    if (text.is_none()) return "application/octet-stream"_str;
    auto lower = String::make(*text);
    lower.as_mut_str().make_ascii_lowercase();
    auto value = lower.as_str();
    if (value == "html"_str || value == "htm"_str) return "text/html; charset=utf-8"_str;
    if (value == "css"_str) return "text/css; charset=utf-8"_str;
    if (value == "js"_str || value == "mjs"_str) return "text/javascript; charset=utf-8"_str;
    if (value == "json"_str) return "application/json"_str;
    if (value == "txt"_str) return "text/plain; charset=utf-8"_str;
    if (value == "svg"_str) return "image/svg+xml"_str;
    if (value == "png"_str) return "image/png"_str;
    if (value == "jpg"_str || value == "jpeg"_str) return "image/jpeg"_str;
    if (value == "gif"_str) return "image/gif"_str;
    if (value == "webp"_str) return "image/webp"_str;
    if (value == "ico"_str) return "image/vnd.microsoft.icon"_str;
    if (value == "wasm"_str) return "application/wasm"_str;
    if (value == "woff"_str) return "font/woff"_str;
    if (value == "woff2"_str) return "font/woff2"_str;
    return "application/octet-stream"_str;
}
auto missing_static_entry(const IoError& error) -> bool {
    return error.kind() == IoErrorKind { IoErrorKind::NotFound } ||
           error.kind() == IoErrorKind { IoErrorKind::NotADirectory };
}
auto static_error(const IoError& error) -> u16 {
    if (missing_static_entry(error)) return u16(404);
    auto kind = error.kind();
    if (kind == IoErrorKind { IoErrorKind::InvalidFilename }) return u16(404);
    if (kind == IoErrorKind { IoErrorKind::PermissionDenied } ||
        kind == IoErrorKind { IoErrorKind::FilesystemLoop } ||
        kind == IoErrorKind { IoErrorKind::CrossesDevices })
        return u16(403);
    return u16(500);
}
auto static_status(u16 status) -> liserveto::Response {
    liserveto::Response response;
    response.head.status = lihttpto::StatusCode::make(status).unwrap();
    if (status == u16(405)) response.head.headers.add("allow"_str, "GET, HEAD"_str).unwrap();
    return response;
}
struct PreparedStaticFile {
    FileResponse file;
    String       name;
};
auto prepare_static_file(const StaticFilesState& state, StaticPath path, bool fallback)
    -> rstd::io::Result<PreparedStaticFile> {
    auto relative =
        rstd::path::PathBuf::from(path.relative.is_empty() ? "."_str : path.relative.as_str());
    auto opened = state.root.open_entry(relative.as_path());
    if (opened.is_err()) {
        if (! fallback || ! missing_static_entry(opened.unwrap_err()))
            return Err(rstd::move(opened).unwrap_err());
        auto name   = state.options.spa->file.clone();
        auto native = rstd::path::PathBuf::from(name.as_str());
        auto entry  = rstd_try(state.root.open_entry(native.as_path()));
        return Ok(PreparedStaticFile { rstd_try(FileResponse::prepare(rstd::move(entry))),
                                       rstd::move(name) });
    }
    auto entry = rstd::move(opened).unwrap();
    if (entry.metadata().is_dir()) {
        if (state.options.index.is_none())
            return Err(IoError::from_kind(IoErrorKind { IoErrorKind::NotFound }));
        auto directory = rstd_try(rstd::move(entry).into_directory());
        auto name      = state.options.index->clone();
        auto native    = rstd::path::PathBuf::from(name.as_str());
        auto index     = rstd_try(directory.open_entry(native.as_path()));
        if (! index.metadata().is_file())
            return Err(IoError::from_kind(IoErrorKind { IoErrorKind::NotFound }));
        return Ok(PreparedStaticFile { rstd_try(FileResponse::prepare(rstd::move(index))),
                                       rstd::move(name) });
    }
    if (! entry.metadata().is_file())
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::PermissionDenied }));
    if (path.directory) return Err(IoError::from_kind(IoErrorKind { IoErrorKind::NotFound }));
    return Ok(PreparedStaticFile { rstd_try(FileResponse::prepare(rstd::move(entry))),
                                   rstd::move(path.relative) });
}
auto serve_static_files(Arc<StaticFilesState> state, Request request)
    -> rstd::async::coro<ResponseResult> {
    auto method = request.head().line.method.as_str();
    if (method != "GET"_str && method != "HEAD"_str) co_return Ok(static_status(u16(405)));
    auto path = static_path(request, state->options.dotfiles);
    if (path.is_err()) co_return Ok(static_status(path.unwrap_err()));
    auto conditions = lihttpto::RequestConditions::parse(request.head().headers);
    if (conditions.is_err()) co_return Err(Error { ErrorKind::InvalidRequest });
    bool fallback {};
    if (state->options.spa.is_some())
        for (const auto& route : state->options.spa->routes)
            fallback = fallback || route == path->relative;
    auto chunk_size = state->options.chunk_size;
    auto submitted  = co_await request.run_blocking(
        [state = state.clone(), path = rstd::move(path).unwrap(), fallback]() mutable {
            return prepare_static_file(*state, rstd::move(path), fallback);
        });
    if (submitted.is_err()) co_return Err(Error { ErrorKind::Internal });
    auto prepared = rstd::move(submitted).unwrap();
    if (prepared.is_err()) co_return Ok(static_status(static_error(prepared.unwrap_err())));
    lihttpto::Headers headers;
    headers.add("content-type"_str, static_mime(prepared->name.as_str())).unwrap();
    headers.add("x-content-type-options"_str, "nosniff"_str).unwrap();
    auto response =
        rstd::move(prepared->file).respond(method, *conditions, rstd::move(headers), chunk_size);
    if (response.is_err()) co_return Err(Error { ErrorKind::Body, Some(response.unwrap_err()) });
    co_return Ok(rstd::move(response).unwrap());
}
namespace liserveto
{
StaticFiles::StaticFiles(Arc<StaticFilesState> state): state_(rstd::move(state)) {
}
StaticFiles::StaticFiles(StaticFiles&&) noexcept                    = default;
auto StaticFiles::operator=(StaticFiles&&) noexcept -> StaticFiles& = default;
StaticFiles::~StaticFiles()                                         = default;
auto StaticFiles::make(ConfinedDir root, StaticFilesOptions options)
    -> rstd::io::Result<StaticFiles> {
    bool valid = options.chunk_size != usize() && options.chunk_size <= usize(1048576);
    if (options.index.is_some()) {
        valid = valid && valid_static_path(options.index->as_str(), options.dotfiles);
        for (auto byte : options.index->as_str().as_bytes()) valid = valid && byte != u8('/');
    }
    if (options.spa.is_some()) {
        valid = valid && valid_static_path(options.spa->file.as_str(), options.dotfiles);
        for (const auto& route : options.spa->routes) {
            valid = valid && valid_static_path(route.as_str(), options.dotfiles);
            for (auto byte : route.as_str().as_bytes()) valid = valid && byte != u8('.');
        }
    }
    if (! valid) return Err(IoError::from_kind(IoErrorKind { IoErrorKind::InvalidInput }));
    return Ok(StaticFiles(Arc<StaticFilesState>::make(rstd::move(root), rstd::move(options))));
}
auto StaticFiles::endpoint() const -> Endpoint {
    return Endpoint::make([state = state_.clone()](Request request) {
        return serve_static_files(state.clone(), rstd::move(request));
    });
}
} // namespace liserveto
