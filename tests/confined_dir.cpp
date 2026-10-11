#include <rstd/test/gtest.hpp>
#include <rstd/macro.hpp>
#if RSTD_OS_LINUX
#include <fcntl.h>
#include <sys/stat.h>
#endif
import liserveto;

using namespace rstd::prelude;
using namespace rstd::literals;
using liserveto::ConfinedDir;

#if RSTD_OS_LINUX
TEST(ConfinedDir, ReadOnlyPathsLinksAndStableHandles) {
    auto temporary  = rstd::fs::TempDir::make("liserveto-confined"_str).unwrap();
    auto base       = rstd::path::PathBuf::from(temporary.path().as_os_str().to_os_string());
    auto root_path  = base.join("root"_str);
    auto child_path = root_path.join("sub"_str);
    auto file_path  = child_path.join("file"_str);
    ASSERT_TRUE(rstd::fs::create_dir_all(child_path.as_path()).is_ok());
    ASSERT_TRUE(rstd::fs::write(file_path.as_path(), "inside"_bytes).is_ok());
    auto outside = base.join("outside"_str);
    ASSERT_TRUE(rstd::fs::write(outside.as_path(), "outside"_bytes).is_ok());
    auto root = ConfinedDir::open(root_path.as_path()).unwrap();
    ASSERT_TRUE(root.self_metadata().unwrap().is_dir());
    auto file = root.open_file("sub/file"_str).unwrap();
    ASSERT_TRUE(file.metadata().unwrap().len() == u64(6));
    ASSERT_TRUE((fcntl(file.as_raw_fd(), F_GETFD) & FD_CLOEXEC) != 0);
    array<u8, 6> data {};
    ASSERT_TRUE(file.read(data.as_mut_slice()).unwrap() == usize(6));
    ASSERT_TRUE(data.as_slice() == "inside"_bytes);
    ASSERT_TRUE(file.write("no"_bytes).is_err());
    ASSERT_TRUE(root.open_file("missing"_str).unwrap_err().kind() ==
                rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::NotFound });
    ASSERT_TRUE(root.open_file("../outside"_str).is_err());
    ASSERT_TRUE(root.open_file(outside.as_path()).is_err());
    ASSERT_TRUE(root.open_file("sub/file\0ignored"_str).is_err());
    ASSERT_TRUE(root.open_file(""_str).is_err());
    ASSERT_TRUE(root.open_file("sub"_str).is_err());
    ASSERT_TRUE(root.open_dir("sub/file"_str).is_err());
    auto sub = root.open_dir("sub"_str).unwrap();
    ASSERT_TRUE(sub.open_file("file"_str).is_ok());
    ASSERT_TRUE(sub.open_file("../sub/file"_str).is_err());
    ASSERT_TRUE(root.open_dir("."_str).is_ok());
    auto file_link = root_path.join("file-link"_str);
    auto dir_link  = root_path.join("dir-link"_str);
    auto escape    = root_path.join("escape"_str);
    ASSERT_TRUE(rstd::fs::soft_link("sub/file"_str, file_link.as_path()).is_ok());
    ASSERT_TRUE(rstd::fs::soft_link("sub"_str, dir_link.as_path()).is_ok());
    ASSERT_TRUE(rstd::fs::soft_link(outside.as_path(), escape.as_path()).is_ok());
    ASSERT_TRUE(root.open_file("file-link"_str).is_err());
    ASSERT_TRUE(root.open_file("dir-link/file"_str).is_err());
    ASSERT_TRUE(root.open_dir("dir-link"_str).is_err());
    ASSERT_TRUE(root.open_file("escape"_str).is_err());
    ASSERT_TRUE(ConfinedDir::open(dir_link.as_path()).is_err());
    auto fifo = root_path.join("fifo"_str).as_path().to_cstring().unwrap();
    ASSERT_TRUE(mkfifo(fifo.as_ptr(), 0600) == 0);
    ASSERT_TRUE(root.open_file("fifo"_str).is_err());
    auto cloned = root.try_clone().unwrap();
    auto moved  = base.join("moved"_str);
    ASSERT_TRUE(rstd::fs::rename(root_path.as_path(), moved.as_path()).is_ok());
    ASSERT_TRUE(root.open_file("sub/file"_str).is_ok());
    ASSERT_TRUE(cloned.open_file("sub/file"_str).is_ok());
    ASSERT_TRUE(sub.open_file("file"_str).is_ok());
    auto old_file = file.metadata().unwrap();
    ASSERT_TRUE(rstd::fs::remove_file(moved.join("sub/file"_str).as_path()).is_ok());
    ASSERT_TRUE(file.metadata().unwrap().ino() == old_file.ino());
    ASSERT_TRUE(root.open_file("sub/file"_str).is_err());
}
#else
TEST(ConfinedDir, UnsupportedBackendFailsClosed) {
    auto opened = ConfinedDir::open("."_str);
    ASSERT_TRUE(opened.is_err());
    ASSERT_TRUE(opened.unwrap_err().kind() ==
                rstd::io::error::ErrorKind { rstd::io::error::ErrorKind::Unsupported });
}
#endif
