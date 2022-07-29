//! Write to file using a variety of APIs.
//use glommio::{io::BufferedFile, LocalExecutor};
use memmap2::MmapOptions;
use std::time::{Duration, Instant};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use aligned_vec::*;
use std::io::Write;
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
pub fn seq_write_all(fname: &str, chunk_size: u64, num_chunks: u64, filebuf: &[u8]) -> std::io::Result<Duration> {
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
pub fn seq_write_direct_all(fname: &str, chunk_size: u64, num_chunks: u64, filebuf: &[u8]) -> std::io::Result<Duration> {
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
pub fn seq_buf_write(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
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
pub fn seq_buf_write_all(fname: &str, chunk_size: u64, num_chunks: u64, filebuf: &[u8]) -> std::io::Result<Duration> {
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
pub fn seq_mmap_write(fname: &str, chunk_size: u64, num_chunks: u64) -> std::io::Result<Duration> {
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
pub fn seq_mmap_write_all(fname: &str, chunk_size: u64, num_chunks: u64, filebuf: &[u8]) -> std::io::Result<Duration> {
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
pub fn seq_vec_write_all(fname: &str, chunk_size: u64, filebuf: &[u8]) -> std::io::Result<Duration> {
    let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(fname)?;
    let t = Instant::now();
    use crate::vec_io;
    vec_io::write_vec_slice(&file, filebuf, chunk_size)?;
    file.flush()?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_glommio_read(_fname: &str, _chunk_size: u64, _num_chunks: u64) -> std::io::Result<Duration> {
    todo!()
}
//-----------------------------------------------------------------------------
pub fn async1_seq_glommio_read(_fname: &str, _chunk_size: u64, _num_chunks: u64) -> std::io::Result<Duration> {
    todo!()
}

