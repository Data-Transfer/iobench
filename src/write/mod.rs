//! Write to file using a variety of APIs.
//use glommio::{io::BufferedFile, LocalExecutor};
use memmap2::MmapOptions;
use std::io::Write;
use std::time::{Duration, Instant};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
//-----------------------------------------------------------------------------
pub fn seq_write(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fname)?;
    let buf = vec![0_u8; chunk_size as usize];
    let t = Instant::now();
    for _ in 0..num_chunks {
        file.write_all(&buf)?;
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fname)?;
    let mut r = 0_u64;
    let t = Instant::now();
    for _ in 0..num_chunks {
        let b = r as usize;
        let e = b + (chunk_size as usize);
        r += file.write(&filebuf[b..e])? as u64;
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_direct_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    if chunk_size % 512 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "O_DIRECT requires a chunk size multiple of 512'",
        ));
    }
    let mut r = 0_u64;
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)?;
    let fsize = num_chunks * chunk_size;
    let t = Instant::now();
    for _ in 0..num_chunks {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        r += file.write(&filebuf[b..e])? as u64;
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn seq_write_buf(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fname)?;
    let buf = vec![0_u8; chunk_size as usize];
    let mut br = std::io::BufWriter::new(file);
    let t = Instant::now();
    for _ in 0..num_chunks {
        br.write_all(&buf)?;
    }
    br.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_buf_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut r = 0_u64;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fname)?;
    let fsize = chunk_size * num_chunks;
    let mut br = std::io::BufWriter::new(&file);
    let t = Instant::now();
    for _ in 0..num_chunks {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        r += br.write(&filebuf[b..e])? as u64;
    }
    br.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_mmap(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(fname)?;
    let fsize = num_chunks * chunk_size;
    let buf = vec![0_u8; chunk_size as usize];
    let mut mmap = unsafe { MmapOptions::new().len(fsize as usize).map_mut(&file)? };
    let mut r = 0_u64;
    let t = Instant::now();
    for _ in 0..num_chunks {
        let b = r as usize;
        let e = b + buf.len();
        mmap[b..e].copy_from_slice(&buf);
        r += chunk_size;
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_mmap_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut r = 0_u64;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(fname)?;
    let fsize = num_chunks * chunk_size;
    let mut mmap = unsafe { MmapOptions::new().len(fsize as usize).map_mut(&file)? };
    let t = Instant::now();
    for _ in 0..num_chunks {
        let b = r as usize;
        let e = b + (chunk_size as usize);
        mmap[b..e].copy_from_slice(&filebuf[b..e]);
        r += chunk_size;
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_write_vec_all(
    fname: &str,
    chunk_size: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(fname)?;
    let t = Instant::now();
    use crate::vec_io;
    vec_io::write_vec_slice(&mut file, filebuf, chunk_size)?;
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "seq_write_uring_all", target_os = "linux"))]
pub fn seq_write_uring_all(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fname)?;
    let buf = vec![0_u8; (chunk_size * num_chunks) as usize];
    let t = Instant::now();
    let n = {
        let mut io_uring = iou::IoUring::new(1)?;

        unsafe {
            let mut sq = io_uring.sq();
            let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to prepare io_uring submission queue",
            ))?;
            use std::os::unix::io::AsRawFd;
            sqe.prep_write(file.as_raw_fd(), &*buf, 0);
            sqe.set_user_data(0xDEADBEEF);
            io_uring.sq().submit()?;
        }

        let mut cq = io_uring.cq();
        let cqe = cq.wait_for_cqe()?;
        cqe.result()? as usize
    };
    if n != (chunk_size * num_chunks) as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("seq_write_uring_all: Failed to write data from io_uring queue, requested: {}, written: {}", chunk_size * num_chunks, n).as_str())
        );
    }
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}

//-----------------------------------------------------------------------------
// @warning will normally fail for total size > (2GiB - 4kiB), limit imposed
// by vectored i/o, so partial reads/writes must be handled
#[cfg(all(feature = "seq_write_uring_vec_all", target_os = "linux"))]
pub fn seq_write_uring_vec_all(
    fname: &str,
    chunk_size: u64,
    num_chunks: u64,
    filebuf: &[u8],
) -> std::io::Result<Duration> {
    // @todo: check for alignment
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
    let mut bufs = Vec::new();
    for c in 0..num_chunks {
        let i = c as usize * chunk_size as usize;
        let s = &filebuf[i..i + chunk_size as usize];
        bufs.push(std::io::IoSlice::new(s));
    }
    let t = Instant::now();
    let entries = num_chunks as u32;
    let n = {
        let mut io_uring = iou::IoUring::new(entries)?;
        unsafe {
            let mut sq = io_uring.sq();
            let mut sqe = sq.prepare_sqe().ok_or(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to prepare io_uring submission queue",
            ))?;
            use std::os::unix::io::AsRawFd;
            sqe.prep_write_vectored(file.as_raw_fd(), &bufs, 0);
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
    let e = t.elapsed();
    Ok(e)
}
