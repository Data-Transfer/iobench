#![allow(non_camel_case_types)]
use std::os::raw::{c_int, c_void};
use std::os::unix::io::RawFd;
pub struct Movable<T>(pub *const T);
impl<T> Movable<T> {
    pub fn get(&self) -> Option<*const T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}

pub struct MovableMut<T>(pub *mut T);
impl<T> MovableMut<T> {
    pub fn get(&self) -> Option<*mut T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}

unsafe impl<T> Send for Movable<T> {}
unsafe impl<T> Send for MovableMut<T> {}
//----------j------------------------------------------------------------------
pub fn dump(v: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::open("/dev/null")?;
    let _ = f.write(&v[..1]);
    Ok(())
}

//----------------------------------------------------------------------------
pub type ssize_t = isize;
pub type size_t = usize;
pub type off_t = isize;
extern "C" {
    pub fn pread(fd: RawFd, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t;
    pub fn pwrite(fd: RawFd, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t;
}

#[repr(C)]
pub struct IoVec {
    pub iov_base: *mut c_void,
    pub iov_len: size_t,
}

extern "C" {
    pub fn readv(fd: RawFd, bufs: *const IoVec, count: c_int) -> ssize_t;
    pub fn writev(fd: RawFd, bufs: *const IoVec, count: c_int) -> ssize_t;
    pub fn preadv(fd: RawFd, bufs: *const IoVec, count: c_int, offset: off_t) -> ssize_t;
    pub fn pwritev(fd: RawFd, bufs: *const IoVec, count: c_int, offset: off_t) -> ssize_t;
}
