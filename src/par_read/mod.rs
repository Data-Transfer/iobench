use crate::utility::*;
use crate::utility::{dump, MovableMut};
use crate::vec_io;
use memmap2::MmapOptions;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::io::{Read, Seek, SeekFrom};
use std::os::raw::c_void;
use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};

//-----------------------------------------------------------------------------
pub fn par_read_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let th = std::thread::spawn(move || {
            let mut file = std::fs::File::open(&fname)?;
            file.seek(SeekFrom::Start(offset))?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let cs = thread_span.min(fsize - offset);
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, cs as usize) };
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                r += file.read(&mut slice[b..e])?;
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_read_buf_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let th = std::thread::spawn(move || {
            let mut file = std::fs::File::open(&fname)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut br = std::io::BufReader::new(&file);
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let cs = thread_span.min(fsize - offset);
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, cs as usize) };
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                r += br.read(&mut slice[b..e])?;
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_read_pread_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let file = std::fs::File::open(fname)?;
    let fd = file.as_raw_fd();
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let th = std::thread::spawn(move || {
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let cs = thread_span.min(fsize - offset);
            let slice = unsafe { std::slice::from_raw_parts(ptr, cs as usize) };
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                let sz = (e - b) as size_t;
                let ret = unsafe {
                    pread(
                        fd,
                        slice[b..e].as_ptr() as *mut c_void,
                        sz,
                        (offset as usize + b) as off_t,
                    )
                };
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                r += ret as usize;
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_read_direct_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    if chunk_size % 512 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "O_DIRECT requires a chunk size multiple of 512'",
        ));
    }
    let fsize = filebuf.len() as u64;
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)?;
    let fd = file.as_raw_fd();
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let th = std::thread::spawn(move || {
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let cs = thread_span.min(fsize - offset);
            let slice = unsafe { std::slice::from_raw_parts(ptr, cs as usize) };
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                let sz = (e - b) as size_t;
                let ret = unsafe {
                    pread(
                        fd,
                        slice[b..e].as_ptr() as *mut c_void,
                        sz,
                        (offset as usize + b) as off_t,
                    )
                };
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                r += ret as usize;
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_read_mmap_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let file = std::sync::Arc::new(std::fs::File::open(fname)?);
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let file = file.clone();
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let th = std::thread::spawn(move || {
            let mmap = unsafe { MmapOptions::new().offset(offset).map(&*file)? };
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let cs = thread_span.min(fsize - offset);
            let slice: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(ptr, cs as usize) };
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                slice[b..e].copy_from_slice(&mmap[b..e]);
                r += e - b;
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn par_read_vec_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut threads = Vec::new();
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let th = std::thread::spawn(move || {
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let mut file = std::fs::File::open(&fname)?;
            let cs = thread_span.min(fsize - offset);
            let slice: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(ptr, cs as usize) };
            vec_io::read_vec_slice_offset(&mut file, slice, chunk_size, offset as isize)?;
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "par_read_uring_vec_all", target_os = "linux"))]
pub fn par_read_uring_vec_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let fsize = filebuf.len();
    let num_threads = num_threads as usize;
    let num_chunks = ((fsize as u64 + chunk_size - 1) / chunk_size) as usize;
    let chunk_size = chunk_size as usize;
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let num_chunks = chunks_per_thread.min(num_chunks - chunks_per_thread * (num_threads -1));
        let offset = thread_span * i;
        let mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        use std::os::unix::fs::OpenOptionsExt;
        let th = std::thread::spawn(move || {
            let mut file = if cfg!(feature = "uring_direct") {
                std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(fname)?
            } else {
                std::fs::OpenOptions::new()
                    .read(true)
                    .open(fname)?
            };
            let ptr = match mb.get()  {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = (num_chunks * chunk_size).min(fsize - offset);
            //@warning: it is not possible to use iou to read data by dynamically creating
            //a vector of mutable slices, it is therefore required to create manually an
            //array of IoVec structs which are compatible with IoSliceMut
            // - IoSliceMut contains an std::sys::io::IoSlice
            // - sts::sys::io::IoSlice contains an iovec
            // - iovec is declared as:
            //      #[repr(C)]
            //      pub struct iovec {
            //        pub iov_base: *mut c_void,
            //        pub iov_len: size_t,
            //      }
            // - IoVec is declared the same as iovec
            // - Therefore: as slice of Vec<IoVec> can be cast to a slice of Vec<IoSliceMut>
            let mut bufs = Vec::new();
             
            for i in 0..num_chunks {
                let b = i as usize * chunk_size as usize;
                let e = (b + chunk_size).min(fsize);
                let iv = unsafe{IoVec{iov_base: ptr.offset(b as isize) as *mut c_void, iov_len: e - b }};
                bufs.push(iv);
            }
            let ioslice = unsafe {std::slice::from_raw_parts_mut(bufs.as_mut_ptr() as *mut std::io::IoSliceMut, bufs.len())};
            let entries = num_chunks as u32;
            let n = {
                let mut io_uring = iou::IoUring::new(entries)?;
                unsafe {
                    let mut sq = io_uring.sq();
                    let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to prepare io_uring submission queue",
                    ))?;
                    sqe.prep_read_vectored(file.as_raw_fd(), ioslice, offset as u64);
                    io_uring.sq().submit()?;
                }

                let mut cq = io_uring.cq();
                let cqe = cq.wait_for_cqe()?;
                cqe.result()? as usize
            };
            if n != (num_chunks * chunk_size) as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("seq_write_uring_all: Failed to write data from io_uring queue, requested: {}, written: {}", chunk_size * num_chunks, n).as_str()
                ));
            }
            Ok(())
        });
        threads.push(th);
    }
    join_and_check!(threads);
    let e = t.elapsed();
    dump(&filebuf);
    Ok(e)
}


//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "par_read_uring_all", target_os = "linux"))]
pub fn par_read_uring_all(
    fname: &str,
    chunk_size: u64,
    num_threads: u64,
    filebuf: &mut [u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let fsize = filebuf.len() as u64;
    let thread_span = (fsize + num_threads - 1) / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mut mb = unsafe { MovableMut(filebuf.as_mut_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        use std::os::unix::fs::OpenOptionsExt;
        let th = std::thread::spawn(move || {
            let mut file = if cfg!(feature = "uring_direct") {
                std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(fname)?
            } else {
                std::fs::OpenOptions::new()
                    .read(true)
                    .open(fname)?
            };
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = if i != num_threads - 1 {
                thread_span
            } else {
                fsize - (num_threads - 1) * thread_span
            };
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, bytes as usize) };
            let entries = 1;
            let n = {
                let mut io_uring = iou::IoUring::new(entries)?;
                unsafe {
                    let mut sq = io_uring.sq();
                    let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to prepare io_uring submission queue",
                    ))?;
                    sqe.prep_read(file.as_raw_fd(), slice, offset);
                    io_uring.sq().submit()?;
                }
                let mut cq = io_uring.cq();
                let cqe = cq.wait_for_cqe()?;
                cqe.result()? as usize
            };
            if n != bytes as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("seq_read_uring_all: Failed to read data from io_uring queue, requested: {}, read: {}", bytes, n).as_str()
                ));
            }
            Ok(())
        });
        threads.push(th);
    }

    join_and_check!(threads);

    let e = t.elapsed();
    dump(&filebuf);
    Ok(e)
}
