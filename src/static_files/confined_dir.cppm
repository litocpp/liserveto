export module liserveto:static_files.confined_dir;
export import rstd;

using namespace rstd::prelude;

export namespace liserveto
{
class ConfinedDir;
class ConfinedEntry {
    friend class ConfinedDir;
    rstd::fs::File     file_;
    rstd::fs::Metadata metadata_;
    ConfinedEntry(rstd::fs::File file, rstd::fs::Metadata metadata)
        : file_(rstd::move(file)), metadata_(rstd::move(metadata)) {}

public:
    ConfinedEntry(const ConfinedEntry&)     = delete;
    ConfinedEntry(ConfinedEntry&&) noexcept = default;
    auto metadata() const -> const rstd::fs::Metadata& { return metadata_; }
    auto into_file() && -> rstd::fs::File { return rstd::move(file_); }
    auto into_directory() && -> rstd::io::Result<ConfinedDir>;
};
// Read-only opens beneath a trusted root handle; no symlink resolution in relative paths.
// Unsupported backends fail closed. This does not provide a content snapshot.
class ConfinedDir {
    friend class ConfinedEntry;
    rstd::fs::Dir directory_;
    explicit ConfinedDir(rstd::fs::Dir directory) noexcept: directory_(rstd::move(directory)) {}

public:
    ConfinedDir(const ConfinedDir&)                               = delete;
    ConfinedDir(ConfinedDir&&) noexcept                           = default;
    auto        operator=(ConfinedDir&&) noexcept -> ConfinedDir& = default;
    static auto open(ref<rstd::path::Path> trusted_root) -> rstd::io::Result<ConfinedDir>;
    auto        open_entry(ref<rstd::path::Path> relative) const -> rstd::io::Result<ConfinedEntry>;
    auto        open_file(ref<rstd::path::Path> relative) const -> rstd::io::Result<rstd::fs::File>;
    auto        open_dir(ref<rstd::path::Path> relative) const -> rstd::io::Result<ConfinedDir>;
    auto        self_metadata() const -> rstd::io::Result<rstd::fs::Metadata> {
        return directory_.self_metadata();
    }
    auto try_clone() const -> rstd::io::Result<ConfinedDir> {
        return directory_.try_clone().map([](rstd::fs::Dir directory) {
            return ConfinedDir(rstd::move(directory));
        });
    }
};
} // namespace liserveto
