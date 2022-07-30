use std::time::{Instant, Duration};
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::os::raw::c_void;
use std::os::unix::io::{AsRawFd, RawFd};
use memmap2::MmapOptions;

// struct Movable<T>(*const T);
// impl<T> Movable<T> {
//     fn get(&self) -> Option<*const T> {
//         if self.0.is_null() {
//             return None;
//         }
//         Some(self.0)
//     }
// }

struct MovableMut<T>(*mut T);
impl<T> MovableMut<T> {
    fn get(&self) -> Option<*mut T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}

// unsafe impl<T> Send for Movable<T> {}
unsafe impl<T> Send for MovableMut<T> {}

type ssize_t = isize;
type size_t = usize;
type off_t = isize;
extern {
    fn pread(fd: RawFd, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t;
}

pub fn par_read_all(fname: &str, chunk_size: u64, num_threads: u64, filebuf: &mut [u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let file = std::fs::File::open(fname)?;
    let fd = file.as_raw_fd();
    let mut threads =  Vec::new();
    let thread_span = fsize / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mb = unsafe {MovableMut(filebuf.as_mut_ptr().offset(offset as isize))};
        let th = std::thread::spawn(move || {
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p
            };
            let cs = thread_span.min(fsize - offset);
            let slice = unsafe {std::slice::from_raw_parts(ptr, cs as usize)};
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len()); 
                let sz = (e - b) as size_t;
                let ret = unsafe {pread(fd, slice[b..e].as_ptr() as *mut c_void, sz as size_t, (offset as usize + b) as off_t)};
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                r += ret as usize;
            }
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


pub fn par_mmap_read_all(fname: &str, chunk_size: u64, num_threads: u64, filebuf: &mut [u8]) -> std::io::Result<Duration> {
    let fsize = filebuf.len() as u64;
    let file = std::fs::File::open(fname)?;
    let mmap = std::sync::Arc::new(unsafe { MmapOptions::new().map(&file)? });
    let mut threads =  Vec::new();
    let thread_span = fsize / num_threads;
    let t = Instant::now();
    for i in 0..num_threads {
        let offset = thread_span * i;
        let mmap = mmap.clone();
        let mb = unsafe {MovableMut(filebuf.as_mut_ptr().offset(offset as isize))};
        let th = std::thread::spawn(move || {
            let ptr = match mb.get() {
                None => return Err(IOError::new(IOErrorKind::Other, "NULL pointer")),
                Some(p) => p
            };
            let cs = thread_span.min(fsize - offset);
            let slice: &mut [u8] = unsafe {std::slice::from_raw_parts_mut(ptr, cs as usize)};
            let mut r = 0;
            while r < slice.len() {
                let b = r as usize;
                let e = (b + chunk_size as usize).min(slice.len());
                let sb = b + offset as usize;
                let se = e + offset as usize;
                slice[b..e].copy_from_slice(&mmap[sb..se]); 
                r += e - b;
            }
            Ok(())
        }); 
        threads.push(th);
    }
    for t in threads {
        if let Err(e) = t.join() {
            return Err(IOError::new(IOErrorKind::Other, format!("{:?}", e)));
        }
    }
    println!("{}", filebuf[(fsize - 1) as usize]);
    let e = t.elapsed();
    Ok(e)
}
