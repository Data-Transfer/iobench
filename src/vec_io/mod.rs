//-----------------------------------------------------------------------------
// Cannot pass a Vec of mutable references to readv built at runtime.
// Adding a function that breaks a slice into an array of IoVecs and passes it
// to the readv function
// ----------------------------------------------------------------------------
#![allow(non_snake_case)]
use std::os::raw::{c_int, c_void};
use std::os::unix::io::AsRawFd;
use crate::utility::*;

// ----------------------------------------------------------------------------
pub fn read_vec_slice(
    file: &std::fs::File,
    buf: &mut [u8],
    chunk_size: u64,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut r = 0_usize;
    while r < buf.len() {
        let b = r;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_mut_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        println!("iovec size: {}", iovec.iov_len);
        iovecs.push(iovec);
        r += e - b;
    }
    unsafe {
        // @WARNING: partial reads possible, normally ok when total size < 2GiB
        let b = readv(fd, iovecs.as_ptr() as *const IoVec, iovecs.len() as c_int);
        if b != r as ssize_t  {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
// for (;;) {
//     written = writev(fd, iov+cur, count-cur);
//     if (written < 0) goto error;
//     while (cur < count && written >= iov[cur].iov_len)
//         written -= iov[cur++].iov_len;
//     if (cur == count) break;
//     iov[cur].iov_base = (char *)iov[cur].iov_base + written;
//     iov[cur].iov_len -= written;
// }

// ----------------------------------------------------------------------------
pub fn read_vec_slice_offset(
    file: &std::fs::File,
    buf: &mut [u8],
    chunk_size: u64,
    offset: isize,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut r = 0_usize;
    while r < buf.len() {
        let b = r;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_mut_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        r = e - b;
    }
    unsafe {
        if preadv(fd, iovecs.as_ptr() as *const IoVec, iovecs.len() as c_int, offset as off_t) != r as ssize_t {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// ----------------------------------------------------------------------------
pub fn write_vec_slice(file: &std::fs::File, buf: &[u8], chunk_size: u64) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut r = 0_usize;
    while r < buf.len() {
        let b = r;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        r += chunk_size as usize;
    }
    unsafe {
        if writev(fd, iovecs.as_ptr() as *const IoVec, iovecs.len() as c_int) != r as ssize_t {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// ----------------------------------------------------------------------------
pub fn write_vec_slice_offset(file: &std::fs::File, buf: &[u8], chunk_size: u64, offset: off_t) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut r = 0_usize;
    while r < buf.len() {
        let b = r;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        r += chunk_size as usize;
    }
    unsafe {
        if pwritev(fd, iovecs.as_ptr() as *const IoVec, iovecs.len() as c_int, offset as off_t) != r as ssize_t {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
