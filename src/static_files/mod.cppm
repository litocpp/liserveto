export module liserveto:static_files;
export import :static_files.file;
export import :endpoint;
using namespace rstd::prelude;
using namespace rstd::literals;

struct StaticFilesState;
export namespace liserveto
{
struct SpaFallback {
    String file { String::make("index.html"_str) };
    // Exact decoded paths relative to this mount, without a leading slash.
    Vec<String> routes;
};
struct StaticFilesOptions {
    Option<String>      index { Some(String::make("index.html"_str)) };
    bool                dotfiles {};
    usize               chunk_size { 65536 };
    Option<SpaFallback> spa;
};
class StaticFiles {
    rstd::sync::Arc<StaticFilesState> state_;
    explicit StaticFiles(rstd::sync::Arc<StaticFilesState> state);

public:
    StaticFiles(const StaticFiles&) = delete;
    StaticFiles(StaticFiles&&) noexcept;
    auto operator=(StaticFiles&&) noexcept -> StaticFiles&;
    ~StaticFiles();
    // Open the trusted root outside the event loop; request preparation uses blocking workers.
    static auto make(ConfinedDir root, StaticFilesOptions options = {})
        -> rstd::io::Result<StaticFiles>;
    auto endpoint() const -> Endpoint;
};
} // namespace liserveto
