
pub fn par_read_all(fname: &str, chunk_size: u64; num_threads: u64, filebuf: &mut [u8]) -> std::io::Result<()> {
    let fsize = filebuf.len() as u64;
    let mut file = std::fs::File::open(fname);
    let fd = file.as_raw_fd();
    let mut threads =  Vec::new();
    let thread_span = fsize / num_threads;
    for i in 0..num_threads {
        let offset = thread_span * i;
        let th = std::thread::spawn(move || {
            let ptr = b.get().unwrap();
            let cs = chunk_size.min(fsize.len() - idx * chunk_size);
            let slice = std::slice::from_raw_parts(ptr, chunk_siz, cs);
            let r = 0;
            while r < slice.len() {
                let b = r;
                let e = (b + chunk_size).min(slice.len()); 
                let ret = pread(fd, slice[b..e].as_mut_ptr() as *mut c_void, sz as size_t, (offset + b) as off_t);
                if ret < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                r += ret;
            }

       }); 
    } 
    Ok(())
}

use std::os::raw::c_void;
use std::op::unix::io::{AsRawFd, RawFd};
type ssize_t = isize;
type size_t = usize;
type off_t = isize;
extern {
    fn pread(fd: RawFd, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t;
}
