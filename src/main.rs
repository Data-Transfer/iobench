//! Read/Write files using a variety of APIs in serial and parallel mode
//use std::thread;
use memmap::MmapOptions;
use std::time::{Duration, Instant};
use glommio::{io::BufferedFile, LocalExecutor};

//-----------------------------------------------------------------------------
fn seq_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_buf_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_buf_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_mmap_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_mmap_read_all(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn seq_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn async_glommio_read(fname: &str, chunk_size: u64) -> std::io::Result<Duration> {
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
fn main() -> std::io::Result<()> {
    let fname = &std::env::args().nth(1).expect("Missing file name");
    let chunk_size = std::env::args()
        .nth(2)
        .expect("Missing file size")
        .parse::<u64>()
        .expect("Wrong file size");
    let fsize = (std::fs::metadata(fname)?.len() as f64) / 0x40000000 as f64;
    println!("File size: {} GiB, chunk size: {}", fsize, chunk_size);
    #[cfg(feature="seq_read")]
    println!(
        "seq_read:\t\t\t {:.2} GiB/s",
        fsize / seq_read(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature="seq_read_all")]
    println!(
        "seq_read_all:\t\t\t {:.2} GiB/s",
        fsize / seq_read_all(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature="seq_buf_read")]
    println!(
        "seq_buf_read:\t\t\t {:.2} GiB/s",
        fsize / seq_buf_read(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature="seq_buf_read_all")]
    println!(
        "seq_buf_read_all:\t\t {:.2} GiB/s",
        fsize / seq_buf_read_all(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature="seq_mmap_read")]
    println!(
        "seq_mmap_read:\t\t\t {:.2} GiB/s",
        fsize / seq_mmap_read(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature="seq_mmap_read_all")]
    println!(
        "seq_mmap_read_all:\t\t {:.2} GiB/s",
        fsize / seq_mmap_read_all(fname, chunk_size)?.as_secs_f64());
    #[cfg(feature="seq_glommio_read")]
    println!(
        "seq_glommio_read:\t\t {:.2} GiB/s",
        fsize / seq_glommio_read(fname, chunk_size)?.as_secs_f64());
    #[cfg(feature="async_glommio_read")]
    println!(
        "async_glommio_read:\t\t {:.2} GiB/s",
        fsize / async_glommio_read(fname, chunk_size)?.as_secs_f64());
    Ok(())
}

//-----------------------------------------------------------------------------
//#[cfg(any(feature="seq_read_all", feature="seq_mmap_read", feature="seq_mmap_read_all"))]
fn aligned_vec<T: Sized>(size: usize, capacity: usize, align: usize) -> Vec<T> {
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
