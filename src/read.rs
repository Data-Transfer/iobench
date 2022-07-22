//! Read/Write files using a variety of APIs in serial and parallel mode
//use std::thread;
use memmap::MmapOptions;
use std::time::{Duration, Instant};
use glommio::{io::BufferedFile, LocalExecutor};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};

//-----------------------------------------------------------------------------
pub fn seq_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let mut file = std::fs::File::open(fname)?;
    let mut buf = vec![0_u8; chunk_size as usize];
    use std::io::Read;
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
pub fn seq_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let mut file = std::fs::File::open(fname)?;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
    use std::io::Read;
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        file.read_exact(&mut filebuf[b..e])?;
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_read_direct_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)?;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
    use std::io::Read;
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        file.read_exact(&mut filebuf[b..e])?;
        r += chunk_size;
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
    use std::io::Read;
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
pub fn seq_buf_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let file = std::fs::File::open(fname)?;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
    let mut br = std::io::BufReader::new(file);
    use std::io::Read;
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        br.read_exact(&mut filebuf[b..e])?;
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&filebuf)?;
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
        buf.clone_from_slice(&mmap[b..e]);
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&buf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_mmap_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let mut r = 0_u64;
    let file = std::fs::File::open(fname)?;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let t = Instant::now();
    while r < fsize {
        let b = r as usize;
        let e = (b + (chunk_size as usize)).min(fsize as usize);
        filebuf[b..e].clone_from_slice(&mmap[b..e]);
        r += chunk_size;
    }
    let e = t.elapsed();
    dump(&filebuf)?;
    Ok(e)
}
//-----------------------------------------------------------------------------
pub fn seq_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let ex = LocalExecutor::default();
    ex.run(async {
        let mut r = 0_u64;
        let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
        let file = BufferedFile::open(fname).await?;
        let t = Instant::now();
        while r < fsize {
            let b = r as usize;
            let e = (fsize as usize).min(b + chunk_size as usize);
            filebuf[b..e].clone_from_slice(&file.read_at(b as u64, chunk_size as usize).await?);
            r += chunk_size; 
        }
        let e = t.elapsed();
        dump(&filebuf)?;
        Ok(e)
    })
}
//-----------------------------------------------------------------------------
pub fn async_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
    let fsize = std::fs::metadata(fname)?.len();
    let ex = LocalExecutor::default();
    ex.run(async {
        let mut r = 0_u64;
        let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize);
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
            filebuf[i.0..i.1].clone_from_slice(&i.2?);
        }
        let e = t.elapsed();
        dump(&filebuf)?;
        Ok(e)
    })
}
//-----------------------------------------------------------------------------
//#[cfg(any(feature="seq_read_all", feature="seq_mmap_read", feature="seq_mmap_read_all"))]
pub fn aligned_vec<T: Sized>(size: usize, capacity: usize, align: usize) -> Vec<T> {
    unsafe {
        if size == 0 {
            Vec::<T>::new()
        } else {
            let size = size * std::mem::size_of::<T>();
            let capacity = (capacity * std::mem::size_of::<T>()).max(size);

            let layout = std::alloc::Layout::from_size_align_unchecked(size, align);
            let raw_ptr = std::alloc::alloc(layout) as *mut T;
            Vec::from_raw_parts(raw_ptr, size, capacity)
        }
    }
}
//-----------------------------------------------------------------------------
// #[cfg(any(feature="seq_read_all", feature="seq_mmap_read", feature="seq_mmap_read_all"))]
fn page_aligned_vec<T: Sized>(size: usize, capacity: usize) -> Vec<T>  {   
    aligned_vec::<T>(size, capacity, page_size::get())
}
//----------j------------------------------------------------------------------
fn dump(v: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::open("/dev/null")?;
    let _ = f.write(&v[..1]);
    Ok(())
}
