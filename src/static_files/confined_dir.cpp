module;
#include <rstd/macro.hpp>
#if RSTD_OS_LINUX
#include <errno.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>
#if __has_include(<linux/openat2.h>) && defined(SYS_openat2)
#include <linux/openat2.h>
#define LIBSERVETO_HAS_OPENAT2 1
#endif
#endif

module libserveto;
import :static_files.confined_dir;

using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::io::error::Error;
using rstd::io::error::ErrorKind;
using rstd::os::fd::OwnedFd;
using rstd::path::Path;

auto open_confined_at(const rstd::fs::Dir& directory, ref<Path> path, bool child_directory)
    -> rstd::io::Result<OwnedFd> {
#ifdef LIBSERVETO_HAS_OPENAT2
    auto converted = path.to_cstring();
    if (converted.is_err()) return Err(Error::from_kind(ErrorKind { ErrorKind::InvalidInput }));
    auto       value = rstd::move(converted).unwrap();
    ::open_how how {};
    how.flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    if (child_directory) how.flags |= O_DIRECTORY;
    how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS;
    for (;;) {
        auto raw = ::syscall(SYS_openat2, directory.as_raw_fd(), value.as_ptr(), &how, sizeof(how));
        if (raw >= 0) return Ok(OwnedFd::from_raw_fd(static_cast<int>(raw)));
        auto error = errno;
        if (error == ENOSYS) return Err(Error::from_kind(ErrorKind { ErrorKind::Unsupported }));
        if (error != EINTR) return Err(Error::from_raw_os_error(i32(error)));
    }
#else
    (void)directory;
    (void)path;
    (void)child_directory;
    return Err(Error::from_kind(ErrorKind { ErrorKind::Unsupported }));
#endif
}

namespace libserveto
{
auto ConfinedDir::open(ref<Path> path) -> rstd::io::Result<ConfinedDir> {
#ifdef LIBSERVETO_HAS_OPENAT2
    auto root = rstd_try(rstd::fs::Dir::open_with(
        path, rstd::fs::OpenOptions::make().read(true).custom_flags(i32(O_NOFOLLOW))));
    // Verify the required kernel constraints before granting the directory capability.
    auto fd = rstd_try(open_confined_at(root, "."_str, true));
    return Ok(ConfinedDir(rstd::fs::Dir::from_raw_fd(rstd::move(fd).into_raw_fd())));
#else
    (void)path;
    return Err(Error::from_kind(ErrorKind { ErrorKind::Unsupported }));
#endif
}
auto ConfinedDir::open_file(ref<Path> path) const -> rstd::io::Result<rstd::fs::File> {
    auto fd   = rstd_try(open_confined_at(directory_, path, false));
    auto file = rstd::fs::File(rstd::move(fd));
    auto info = rstd_try(file.metadata());
    if (! info.is_file()) return Err(Error::from_kind(ErrorKind { ErrorKind::InvalidInput }));
    return Ok(rstd::move(file));
}
auto ConfinedDir::open_dir(ref<Path> path) const -> rstd::io::Result<ConfinedDir> {
    auto fd = rstd_try(open_confined_at(directory_, path, true));
    return Ok(ConfinedDir(rstd::fs::Dir::from_raw_fd(rstd::move(fd).into_raw_fd())));
}
} // namespace libserveto
