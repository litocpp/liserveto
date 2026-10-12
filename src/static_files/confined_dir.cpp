module;
#include <rstd/macro.hpp>
#if RSTD_OS_UNIX
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#endif
#if RSTD_OS_LINUX
#include <sys/syscall.h>
#if __has_include(<linux/openat2.h>) && defined(SYS_openat2)
#include <linux/openat2.h>
#define liserveto_HAS_OPENAT2 1
#endif
#endif
#if RSTD_OS_MACOS && defined(O_RESOLVE_BENEATH) && defined(O_NOFOLLOW_ANY)
#define liserveto_HAS_BENEATH_OPENAT 1
#endif

module liserveto;
import :static_files.confined_dir;

using namespace rstd::prelude;
using namespace rstd::literals;
using IoError     = rstd::io::error::Error;
using IoErrorKind = rstd::io::error::ErrorKind;
using rstd::os::fd::OwnedFd;
using rstd::path::Path;

#ifdef liserveto_HAS_BENEATH_OPENAT
auto darwin_confined_open(const rstd::fs::Dir& directory, ref<Path> path, bool child_directory)
    -> rstd::io::Result<OwnedFd> {
    auto converted = path.to_cstring();
    if (converted.is_err())
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::InvalidInput }));
    auto value = rstd::move(converted).unwrap();
    auto flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_RESOLVE_BENEATH | O_NOFOLLOW_ANY;
    if (child_directory) flags |= O_DIRECTORY;
    for (;;) {
        auto raw = ::openat(directory.as_raw_fd(), value.as_ptr(), flags);
        if (raw >= 0) return Ok(OwnedFd::from_raw_fd(raw));
        auto error = errno;
        if (error != EINTR) return Err(IoError::from_raw_os_error(i32(error)));
    }
}
#endif

auto open_confined_at(const rstd::fs::Dir& directory, ref<Path> path, bool child_directory)
    -> rstd::io::Result<OwnedFd> {
#ifdef liserveto_HAS_OPENAT2
    auto converted = path.to_cstring();
    if (converted.is_err())
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::InvalidInput }));
    auto       value = rstd::move(converted).unwrap();
    ::open_how how {};
    how.flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    if (child_directory) how.flags |= O_DIRECTORY;
    how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS;
    for (;;) {
        auto raw = ::syscall(SYS_openat2, directory.as_raw_fd(), value.as_ptr(), &how, sizeof(how));
        if (raw >= 0) return Ok(OwnedFd::from_raw_fd(static_cast<int>(raw)));
        auto error = errno;
        if (error == ENOSYS)
            return Err(IoError::from_kind(IoErrorKind { IoErrorKind::Unsupported }));
        if (error != EINTR) return Err(IoError::from_raw_os_error(i32(error)));
    }
#elif defined(liserveto_HAS_BENEATH_OPENAT)
    return darwin_confined_open(directory, path, child_directory).map_err([](IoError error) {
        auto code = error.raw_os_error();
        if (code == Some(i32(ENOTCAPABLE)))
            return IoError::from_kind(IoErrorKind { IoErrorKind::PermissionDenied });
        if (code == Some(i32(EINVAL)) || code == Some(i32(ENOTSUP)))
            return IoError::from_kind(IoErrorKind { IoErrorKind::Unsupported });
        return error;
    });
#else
    (void)directory;
    (void)path;
    (void)child_directory;
    return Err(IoError::from_kind(IoErrorKind { IoErrorKind::Unsupported }));
#endif
}

namespace liserveto
{
auto ConfinedDir::open(ref<Path> path) -> rstd::io::Result<ConfinedDir> {
#if defined(liserveto_HAS_OPENAT2) || defined(liserveto_HAS_BENEATH_OPENAT)
    auto root = rstd_try(rstd::fs::Dir::open_with(
        path, rstd::fs::OpenOptions::make().read(true).custom_flags(i32(O_NOFOLLOW))));
    // Verify the required kernel constraints before granting the directory capability.
    auto fd = rstd_try(open_confined_at(root, "."_str, true));
#ifdef liserveto_HAS_BENEATH_OPENAT
    // Older kernels may ignore open flags; an escape must fail before granting the capability.
    auto escape = darwin_confined_open(root, "/"_str, true);
    if (escape.is_ok() || escape.unwrap_err().raw_os_error() != Some(i32(ENOTCAPABLE)))
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::Unsupported }));
#endif
    return Ok(ConfinedDir(rstd::fs::Dir::from_raw_fd(rstd::move(fd).into_raw_fd())));
#else
    (void)path;
    return Err(IoError::from_kind(IoErrorKind { IoErrorKind::Unsupported }));
#endif
}
auto ConfinedDir::open_entry(ref<Path> path) const -> rstd::io::Result<ConfinedEntry> {
    auto fd   = rstd_try(open_confined_at(directory_, path, false));
    auto file = rstd::fs::File(rstd::move(fd));
    auto info = rstd_try(file.metadata());
    return Ok(ConfinedEntry(rstd::move(file), rstd::move(info)));
}
auto ConfinedDir::open_file(ref<Path> path) const -> rstd::io::Result<rstd::fs::File> {
    auto entry = rstd_try(open_entry(path));
    if (! entry.metadata().is_file())
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::InvalidInput }));
    return Ok(rstd::move(entry).into_file());
}
auto ConfinedEntry::into_directory() && -> rstd::io::Result<ConfinedDir> {
    if (! metadata_.is_dir())
        return Err(IoError::from_kind(IoErrorKind { IoErrorKind::NotADirectory }));
    return Ok(ConfinedDir(rstd::fs::Dir::from_raw_fd(rstd::move(file_).into_raw_fd())));
}
auto ConfinedDir::open_dir(ref<Path> path) const -> rstd::io::Result<ConfinedDir> {
    auto fd = rstd_try(open_confined_at(directory_, path, true));
    return Ok(ConfinedDir(rstd::fs::Dir::from_raw_fd(rstd::move(fd).into_raw_fd())));
}
} // namespace liserveto
