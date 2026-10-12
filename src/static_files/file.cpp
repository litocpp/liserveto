module;
#include <rstd/macro.hpp>
module liserveto;
import :static_files.file;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using rstd::task::Context;
using rstd::task::Poll;

class FileFrames {
    Option<rstd::async::CompletionSource>        source_;
    Option<rstd::async::IoOperation>             reading_;
    Option<rstd::async::CompletionSourceRelease> releasing_;
    u64                                          offset_, remaining_;
    usize                                        chunk_size_;
    bool                                         done_ {};

public:
    using Item = BodyItem;
    FileFrames(rstd::fs::File file, lihttpto::ByteRange range, usize chunk_size)
        : source_(Some(rstd::async::CompletionSource::file(
              rstd::os::fd::OwnedFd::from_raw_fd(rstd::move(file).into_raw_fd())))),
          offset_(range.offset),
          remaining_(range.length),
          chunk_size_(chunk_size) {}
    auto poll_close(mut_ref<FileFrames> self, Context& cx) -> Poll<Result<empty, BodyError>> {
        self->done_    = true;
        self->reading_ = None();
        if (self->source_.is_some())
            self->releasing_.insert(rstd::move(self->source_).take().unwrap().release());
        if (self->releasing_.is_none()) return Poll<Result<empty, BodyError>>::Ready(Ok(empty {}));
        auto released = rstd::future::poll(*self->releasing_, cx);
        if (released.is_pending()) return Poll<Result<empty, BodyError>>::Pending();
        auto result      = rstd::move(released).take();
        self->releasing_ = None();
        return Poll<Result<empty, BodyError>>::Ready(
            result.is_ok() ? Result<empty, BodyError>(Ok(empty {}))
                           : Result<empty, BodyError>(Err(BodyError::Io)));
    }
    auto poll_next(mut_ref<FileFrames> self, Context& cx) -> Poll<Option<Item>> {
        if (self->done_ || self->remaining_ == u64()) return Poll<Option<Item>>::Ready(None());
        if (self->reading_.is_none()) {
            auto count = usize(
                rstd::min(self->remaining_, u64(self->chunk_size_.to_primitive())).to_primitive());
            self->reading_.insert(
                rstd::async::IoOperation::read_at(*self->source_, count, self->offset_));
        }
        auto polled = rstd::future::poll(*self->reading_, cx);
        if (polled.is_pending()) return Poll<Option<Item>>::Pending();
        auto result    = rstd::move(polled).take();
        self->reading_ = None();
        if (result.is_err()) {
            self->done_ = true;
            return Poll<Option<Item>>::Ready(Some(Item(Err(BodyError::Io))));
        }
        auto bytes = rstd::move(result).unwrap().into_data();
        if (bytes.is_empty()) {
            self->done_ = true;
            return Poll<Option<Item>>::Ready(Some(Item(Err(BodyError::Source))));
        }
        auto count = u64(bytes.len().to_primitive());
        self->offset_ += count;
        self->remaining_ -= count;
        return Poll<Option<Item>>::Ready(
            Some(Item(Ok(lihttpto::BodyFrame::Data(rstd::move(bytes))))));
    }
};

namespace liserveto
{
auto FileResponse::prepare(rstd::fs::File file) -> rstd::io::Result<FileResponse> {
    auto info = rstd_try(file.metadata());
    return prepare_metadata(rstd::move(file), info);
}
auto FileResponse::prepare(ConfinedEntry entry) -> rstd::io::Result<FileResponse> {
    auto info = entry.metadata();
    return prepare_metadata(rstd::move(entry).into_file(), info);
}
auto FileResponse::prepare_metadata(rstd::fs::File file, const rstd::fs::Metadata& info)
    -> rstd::io::Result<FileResponse> {
    if (! info.is_file())
        return Err(rstd::io::error::Error::from_kind(
            rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::InvalidInput }));
    lihttpto::RepresentationMetadata metadata;
    metadata.length = info.len();
    auto modified   = info.modified();
    if (modified.is_ok()) {
        auto time = modified->as_unix_time();
        metadata.last_modified =
            Some(rstd::min(time.seconds, rstd::time::SystemTime::now().as_unix_time().seconds));
        auto tag      = rstd::format("W/\"{}-{}-{}-{}-{}\"",
                                     info.dev(),
                                     info.ino(),
                                     info.len(),
                                     time.seconds,
                                     time.nanoseconds);
        metadata.etag = Some(lihttpto::EntityTag::parse(tag.as_str().as_bytes()).unwrap());
    }
    return Ok(FileResponse(rstd::move(file), rstd::move(metadata)));
}
auto FileResponse::respond(ref<str>                           method,
                           const lihttpto::RequestConditions& conditions,
                           lihttpto::Headers                  headers,
                           usize chunk_size) && -> Result<Response, BodyError> {
    if (file_.is_none()) return Err(BodyError::Protocol);
    if (chunk_size == usize() || chunk_size > usize(1048576)) return Err(BodyError::LimitExceeded);
    if (! headers.contains("content-type"_str))
        headers.add("content-type"_str, "application/octet-stream"_str).unwrap();
    auto source = [file = file_.take().unwrap(), chunk_size](lihttpto::ByteRange range) mutable {
        return Body::from_stream(FileFrames(rstd::move(file), range, chunk_size));
    };
    return Ok(representation_response(
        method, conditions, metadata_, rstd::move(source), rstd::move(headers)));
}
} // namespace liserveto
