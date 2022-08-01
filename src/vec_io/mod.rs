//-----------------------------------------------------------------------------
// Cannot pass a Vec of mutable references to readv built at runtime.
// Adding a function that breaks a slice into an array of IoVecs and passes it
// to the readv function
// ----------------------------------------------------------------------------
#![allow(non_snake_case)]
use crate::utility::*;
use std::io::Read;
use std::io::Write;
use std::os::raw::{c_int, c_void};
use std::os::unix::io::AsRawFd;

//------------------------------------------------------------------------------
//pointer arithmetic
#[inline]
fn ptr_offset(p: *const c_void, offset: isize) -> *const c_void {
    let pi = p as isize;
    (pi + offset) as *const c_void
}
#[inline]
fn ptr_offset_mut(p: *mut c_void, offset: isize) -> *mut c_void {
    let pi = p as isize;
    (pi + offset) as *mut c_void
}
// #[inline]
// fn ptr_add(p: &mut *mut c_void, offset: isize) {
//     let pi = *p as isize;
//     *p = (pi + offset) as *mut c_void;
// }

// ----------------------------------------------------------------------------
pub fn read_vec_slice(
    file: &mut std::fs::File,
    buf: &mut [u8],
    chunk_size: u64,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut bytes = 0_usize;
    while bytes < buf.len() {
        let b = bytes;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_mut_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        bytes += e - b;
    }
    unsafe {
        // @WARNING: partial reads possible, normally ok when total size <= 2GiB
        // Addressing partial reads in the case of equal chunk size. For generic
        // case a list of offsets needs to be maintained but not in scope here.
        let mut r = 0;
        let mut iovec_offset = 0;
        // At each iteration find the partially reead iovec, read the missing bytes and
        // update the offset the element following the last one written.
        // There should be one 4096 bytes write for each 2 GiB written.
        while r < bytes && iovec_offset < iovecs.len() {
            let b = readv(
                fd,
                iovecs[iovec_offset..].as_ptr() as *const IoVec,
                (iovecs.len() - iovec_offset) as c_int,
            );
            if b < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if b as usize == bytes {
                break;
            }
            let ivec_rem = b % chunk_size as isize;
            let nc = (b as usize + chunk_size as usize - 1) as usize / chunk_size as usize;
            iovec_offset += nc;
            let iv = &iovecs[iovec_offset - 1];
            let d = if nc == iovecs.len() {
                bytes - r
            } else {
                iv.iov_len - ivec_rem as usize
            };
            let base = ptr_offset_mut(iv.iov_base, (iv.iov_len - d) as isize) as *mut u8;
            let len = d;
            let s = std::slice::from_raw_parts_mut(base, len);
            file.read_exact(s)?;
            r += b as usize + s.len();
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------------
pub fn read_vec_slice_offset(
    file: &mut std::fs::File,
    buf: &mut [u8],
    chunk_size: u64,
    mut offset: isize,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut bytes = 0_usize;
    while bytes < buf.len() {
        let b = bytes;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_mut_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        bytes += e - b;
    }
    unsafe {
        // @WARNING: partial reads possible, normally ok when total size <= 2GiB
        // Addressing partial reads in the case of equal chunk size. For generic
        // case a list of offsets needs to be maintained but not in scope here.
        let mut r = 0;
        let mut iovec_offset = 0;
        // At each iteration find the partially reead iovec, read the missing bytes and
        // update the offset the element following the last one written.
        // There should be one 4096 bytes write for each 2 GiB written.
        while r < bytes && iovec_offset < iovecs.len() {
            let b = preadv(
                fd,
                iovecs[iovec_offset..].as_ptr() as *const IoVec,
                (iovecs.len() - iovec_offset) as c_int,
                offset,
            );
            if b < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if b as usize == bytes {
                break;
            }
            let ivec_rem = b % chunk_size as isize;
            let nc = (b as usize + chunk_size as usize - 1) as usize / chunk_size as usize;
            iovec_offset += nc;
            let iv = &iovecs[iovec_offset - 1];
            let d = if nc == iovecs.len() {
                bytes - r
            } else {
                iv.iov_len - ivec_rem as usize
            };
            let base = ptr_offset_mut(iv.iov_base, (iv.iov_len - d) as isize) as *mut u8;
            let len = d;
            let s = std::slice::from_raw_parts_mut(base, len);
            file.read_exact(s)?;
            r += b as usize + s.len();
            offset = r as isize;
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------------
pub fn write_vec_slice(
    file: &mut std::fs::File,
    buf: &[u8],
    chunk_size: u64,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut bytes = 0_usize;
    while bytes < buf.len() {
        let b = bytes;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        bytes += e - b;
    }
    unsafe {
        // @WARNING: partial writes possible, normally ok when total size <= 2GiB
        // Addressing partial reads in the case of equal chunk size. For generic
        // case a list of offsets needs to be maintained but not in scope here.
        let mut w = 0;
        let mut iovec_offset = 0;
        // At each iteration find the partially written iovec, write the missing bytes and
        // update the offset the element following the last one written.
        // There should be one 4096 bytes write for each 2 GiB written.
        while w < bytes && iovec_offset < iovecs.len() {
            let b = writev(
                fd,
                iovecs[iovec_offset..].as_ptr() as *const IoVec,
                (iovecs.len() - iovec_offset) as c_int,
            );
            if b < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if b as usize == bytes {
                break;
            }
            let ivec_rem = b % chunk_size as isize;
            let nc = (b as usize + chunk_size as usize - 1) as usize / chunk_size as usize;
            iovec_offset += nc;
            let iv = &iovecs[iovec_offset - 1];
            let d = if nc == iovecs.len() {
                bytes - w
            } else {
                iv.iov_len - ivec_rem as usize
            };
            let base = ptr_offset(iv.iov_base, (iv.iov_len - d) as isize) as *const u8;
            let len = d;
            let s = std::slice::from_raw_parts(base, len);
            file.write_all(&s)?;
            w += b as usize + s.len();
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------------
pub fn write_vec_slice_offset(
    file: &mut std::fs::File,
    buf: &[u8],
    chunk_size: u64,
    mut offset: off_t,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut iovecs = Vec::new();
    let mut bytes = 0_usize;
    while bytes < buf.len() {
        let b = bytes;
        let e = (b + chunk_size as usize).min(buf.len());
        let iovec = IoVec {
            iov_base: buf[b..e].as_ptr() as *mut c_void,
            iov_len: (e - b) as size_t,
        };
        iovecs.push(iovec);
        bytes += e - b;
    }
    unsafe {
        // @WARNING: partial writes possible, normally ok when total size <= 2GiB
        // Addressing partial reads in the case of equal chunk size. For generic
        // case a list of offsets needs to be maintained but not in scope here.
        let mut w = 0;
        let mut iovec_offset = 0;
        // At each iteration find the partially written iovec, write the missing bytes and
        // update the offset the element following the last one written.
        // There should be one 4096 bytes write for each 2 GiB written.
        while w < bytes && iovec_offset < iovecs.len() {
            let b = pwritev(
                fd,
                iovecs[iovec_offset..].as_ptr() as *const IoVec,
                (iovecs.len() - iovec_offset) as c_int,
                offset
            );
            if b < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if b as usize == bytes {
                break;
            }
            let ivec_rem = b % chunk_size as isize;
            let nc = (b as usize + chunk_size as usize - 1) as usize / chunk_size as usize;
            iovec_offset += nc;
            let iv = &iovecs[iovec_offset - 1];
            let d = if nc == iovecs.len() {
                bytes - w
            } else {
                iv.iov_len - ivec_rem as usize
            };
            let base = ptr_offset(iv.iov_base, (iv.iov_len - d) as isize) as *const u8;
            let len = d;
            let s = std::slice::from_raw_parts(base, len);
            file.write_all(&s)?;
            w += b as usize + s.len();
            offset = w as isize;
        }
    }
    Ok(())
}
