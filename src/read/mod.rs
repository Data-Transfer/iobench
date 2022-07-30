//! Read from file using a variety of APIs.
#[cfg(any(feature="async_glommio_read", feature="seq_glommio_read"))]
use glommio::{io::BufferedFile, LocalExecutor};

use memmap2::MmapOptions;
use std::time::{Duration, Instant};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use std::io::Read;
use aligned_vec::*;
use crate::utility::dump;

//-----------------------------------------------------------------------------
pub fn seq_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let mut file = std::fs::File::open(fname)?;
    let mut buf = vec![0_u8; chunk_size as usize];
    let t = Instant::now();
    while r < fsize {
        unsafe {
            buf.set_len(chunk_size.min(fsize - r) as usize);
        }
        file.read_exact(&mut buf)?;
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&buf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_read_all(fname: &str, chunk_size: u64, filebuf: &mut[u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut r = 0_u64;
    let mut file = std::fs::File::open(fname)?;
    //let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        r += file.read(&mut filebuf[b..e])? as u64;
    }
    let e = t.elapsed();
    dump(filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_read_direct_all(fname: &str, chunk_size: u64, filebuf: &mut[u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut r = 0_u64;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)?;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        r += file.read(&mut filebuf[b..e])? as u64;
    }
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}

//-----------------------------------------------------------------------------
pub fn seq_buf_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let file = std::fs::File::open(fname)?;
    let mut buf = vec![0_u8; chunk_size as usize];
    let mut br = std::io::BufReader::new(file);
    let t = Instant::now();
    while r < fsize {
        unsafe {
            buf.set_len(chunk_size.min(fsize - r) as usize);
        }
        br.read_exact(&mut buf)?;
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&buf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_buf_read_all(fname: &str, chunk_size: u64, filebuf: &mut[u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let mut r = 0_u64;
    let file = std::fs::File::open(fname)?;
    let mut br = std::io::BufReader::new(file);
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        r += br.read(&mut filebuf[b..e])? as u64;
    }
    let e = t.elapsed();
    dump(filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_mmap_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let file = std::fs::File::open(fname)?;
    let mut buf = vec![0_u8; chunk_size as usize];
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let t = Instant::now();
    while r < fsize {
        unsafe {
            buf.set_len(chunk_size.min(fsize - r) as usize);
        }
        let b = r as usize;
        let e = b + buf.len();
        buf.copy_from_slice(&mmap[b..e]);
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&buf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_mmap_read_all(fname: &str, chunk_size: u64, filebuf: &mut[u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let file = std::fs::File::open(fname)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mut r = 0_u64;
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        filebuf[b..e].copy_from_slice(&mmap[b..e]);
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_vec_read_all(fname: &str, chunk_size: u64, filebuf: &mut [u8]) -> std::io::Result<Duration> {
    let file = std::fs::File::open(fname)?;
    let t = Instant::now();
    use crate::vec_io;
    vec_io::read_vec_slice(&file, filebuf, chunk_size)?;
    let e = t.elapsed();
    Ok(e)
}
//-----------------------------------------------------------------------------
#[cfg(feature="seq_glommio_read")]
pub fn seq_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let ex = LocalExecutor::default();
    ex.run(async {
        let mut r = 0_u64;
        let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
        let file = BufferedFile::open(fname).await?;
        let t = Instant::now();
        while r < fsize {
            let b = r as usize;
            let e = (fsize as usize).min(b + chunk_size as usize);
            filebuf[b..e].copy_from_slice(&file.read_at(b as u64, chunk_size as usize).await?);
            r += chunk_size;
        }
        let e = t.elapsed();
        dump(&filebuf)?;
        Ok(e)
    })
}
//-----------------------------------------------------------------------------
#[cfg(feature="async_glommio_read")]
pub fn async_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let ex = LocalExecutor::default();
    ex.run(async {
        let mut r = 0_u64;
        let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
        let file = BufferedFile::open(fname).await?;
        let t = Instant::now();
        let mut f = Vec::with_capacity(((fsize + chunk_size) / chunk_size) as usize);
        while r < fsize {
            let b = r as usize;
            let e = (fsize as usize).min(b + chunk_size as usize);
            f.push((b, e, file.read_at(b as u64, chunk_size as usize).await));
            r += chunk_size;
        }
        for i in f {
            filebuf[i.0..i.1].copy_from_slice(&i.2?);
        }
        let e = t.elapsed();
        dump(&filebuf)?;
        Ok(e)
    })
}
