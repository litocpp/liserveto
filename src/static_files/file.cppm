export module liserveto:static_files.file;
export import :static_files.representation;
export import :static_files.confined_dir;
using namespace rstd::prelude;

export namespace liserveto
{
class FileResponse {
    Option<rstd::fs::File>           file_;
    lihttpto::RepresentationMetadata metadata_;
    FileResponse(rstd::fs::File file, lihttpto::RepresentationMetadata metadata)
        : file_(Some(rstd::move(file))), metadata_(rstd::move(metadata)) {}
    static auto prepare_metadata(rstd::fs::File file, const rstd::fs::Metadata& metadata)
        -> rstd::io::Result<FileResponse>;

public:
    FileResponse(const FileResponse&) = delete;
    FileResponse(FileResponse&& other) noexcept
        : file_(other.file_.take()), metadata_(rstd::move(other.metadata_)) {}
    auto operator=(FileResponse&& other) noexcept -> FileResponse& {
        if (this == &other) return *this;
        file_     = other.file_.take();
        metadata_ = rstd::move(other.metadata_);
        return *this;
    }
    // Metadata access is synchronous: prepare outside the event loop or in an owned blocking job.
    static auto prepare(rstd::fs::File file) -> rstd::io::Result<FileResponse>;
    static auto prepare(ConfinedEntry entry) -> rstd::io::Result<FileResponse>;
    auto        metadata() const -> const lihttpto::RepresentationMetadata& { return metadata_; }
    auto        respond(ref<str>                           method,
                        const lihttpto::RequestConditions& conditions,
                        lihttpto::Headers                  headers = {},
                        usize chunk_size = usize(65536)) && -> Result<Response, BodyError>;
};
} // namespace liserveto
