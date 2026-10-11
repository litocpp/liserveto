export module liserveto:static_files.confined_dir;
export import rstd;

using namespace rstd::prelude;

export namespace liserveto
{
// Read-only opens beneath a trusted root handle; no symlink resolution in relative paths.
// Unsupported backends fail closed. This does not provide a content snapshot.
class ConfinedDir {
    rstd::fs::Dir directory_;
    explicit ConfinedDir(rstd::fs::Dir directory) noexcept: directory_(rstd::move(directory)) {}

public:
    ConfinedDir(const ConfinedDir&)                               = delete;
    ConfinedDir(ConfinedDir&&) noexcept                           = default;
    auto        operator=(ConfinedDir&&) noexcept -> ConfinedDir& = default;
    static auto open(ref<rstd::path::Path> trusted_root) -> rstd::io::Result<ConfinedDir>;
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
