use crate::utility::*;
use crate::vec_io;
use memmap2::MmapOptions;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::io::{Seek, SeekFrom, Write};
use std::os::raw::c_void;
use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};

//-----------------------------------------------------------------------------
pub fn par_write_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let th = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new().write(true).open(&fname)?;
            file.seek(SeekFrom::Start(offset))?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, (chunk_size * num_chunks) as usize) };
            let mut w = 0;
            let bytes = num_chunks * chunk_size;
            while w < bytes {
                let b = w as usize;
                let e = (b + chunk_size as usize).min(bytes as usize);
                w += file.write(&slice[b..e])? as u64;
            }
            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_write_buf_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let th = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new().write(true).open(&fname)?;
            file.seek(SeekFrom::Start(offset))?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, (chunk_size * num_chunks) as usize) };
            let mut w = 0;
            let bytes = num_chunks * chunk_size;
            use std::io::BufWriter;
            let mut bw = BufWriter::new(&mut file);
            while w < bytes {
                let b = w as usize;
                let e = (b + chunk_size as usize).min(bytes as usize);
                w += bw.write(&slice[b..e])? as u64;
            }
            bw.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_write_direct_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let th = std::thread::spawn(move || {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&fname)?;
            file.seek(SeekFrom::Start(offset))?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, (chunk_size * num_chunks) as usize) };
            let mut w = 0;
            let bytes = num_chunks * chunk_size;
            while w < bytes {
                let b = w as usize;
                let e = (b + chunk_size as usize).min(bytes as usize);
                w += file.write(&slice[b..e])? as u64;
            }
            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_write_pwrite_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let fname = fname.to_owned();
        let th = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new().write(true).open(&fname)?;
            let fd = file.as_raw_fd();
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, (chunk_size * num_chunks) as usize) };
            let mut w = 0;
            let bytes = num_chunks * chunk_size;
            while w < bytes {
                let b = w as usize;
                let e = (b + chunk_size as usize).min(bytes as usize);
                let sz = (e - b) as size_t;
                let ret = unsafe {
                    pwrite(
                        fd,
                        slice[b..e].as_ptr() as *mut c_void,
                        sz,
                        (offset as usize + b) as off_t,
                    )
                };
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                w += ret as u64;
            }
            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    //file.sync_data();

    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_write_mmap_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let fname = fname.to_owned();
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let th = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fname)?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = num_chunks * chunk_size;
            let slice =
                unsafe { std::slice::from_raw_parts(ptr, (chunk_size * num_chunks) as usize) };
            let mut mmap = unsafe {
                MmapOptions::new()
                    .len(bytes as usize)
                    .offset(offset)
                    .map_mut(&file)?
            };
            let mut w = 0;
            while w < bytes {
                let b = w as usize;
                let e = (b + chunk_size as usize).min(bytes as usize);
                mmap[b..e].copy_from_slice(&slice[b..e]);
                w += (e - b) as u64;
            }
            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn par_write_vec_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = chunk_size * num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let fname = fname.to_owned();
        let th = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new().write(true).open(&fname)?;
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = num_chunks * chunk_size;
            let slice = unsafe { std::slice::from_raw_parts(ptr, bytes as usize) };
            vec_io::write_vec_slice_offset(&mut file, slice, chunk_size, offset as isize)?;
            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    //file.sync_data();

    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "par_write_uring_vec_all", target_os = "linux"))]
pub fn par_write_uring_vec_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = chunk_size * num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let fname = fname.to_owned();
        use std::os::unix::fs::OpenOptionsExt;
        let th = std::thread::spawn(move || {
            let mut file = if cfg!(feature = "uring_direct") {
                std::fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .create(true)
                    .open(fname)?
            } else {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(fname)?
            };
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = num_chunks * chunk_size;
            let slice = unsafe { std::slice::from_raw_parts(ptr, bytes as usize) };
            let mut bufs = Vec::new();
            for c in 0..num_chunks {
                let i = c as usize * chunk_size as usize;
                let s = &slice[i..i + chunk_size as usize];
                bufs.push(std::io::IoSlice::new(s));
            }
            let entries = num_chunks as u32;
            let n = {
                let mut io_uring = iou::IoUring::new(entries)?;
                unsafe {
                    let mut sq = io_uring.sq();
                    let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to prepare io_uring submission queue",
                    ))?;
                    sqe.prep_write_vectored(file.as_raw_fd(), &bufs, offset);
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

            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }

    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "par_write_uring_all", target_os = "linux"))]
pub fn par_write_uring_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    num_threads: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut threads = Vec::new();
    let num_chunks_per_thread = (num_chunks + num_threads - 1) / num_threads;
    let num_chunks_last_thread = num_chunks - num_chunks_per_thread * (num_threads - 1);
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = chunk_size * num_chunks_per_thread * i;
        let mb = unsafe { Movable(filebuf.as_ptr().offset(offset as isize)) };
        let num_chunks = if i != num_threads - 1 {
            num_chunks_per_thread
        } else {
            num_chunks_last_thread
        };
        //@todo: use fallocate
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&fname)?;
        }
        let fname = fname.to_owned();
        use std::os::unix::fs::OpenOptionsExt;
        let th = std::thread::spawn(move || {
            let mut file = if cfg!(feature = "uring_direct") {
                std::fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .create(true)
                    .open(fname)?
            } else {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(fname)?
            };
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p,
            };
            let bytes = num_chunks * chunk_size;
            let slice = unsafe { std::slice::from_raw_parts(ptr, bytes as usize) };
            let entries = num_chunks as u32;
            let n = {
                let mut io_uring = iou::IoUring::new(entries)?;
                unsafe {
                    let mut sq = io_uring.sq();
                    let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to prepare io_uring submission queue",
                    ))?;
                    sqe.prep_write(file.as_raw_fd(), slice, offset);
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

            file.flush()?;
            Ok(())
        });
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }

    let e = t.elapsed();
    Ok(e)
}
