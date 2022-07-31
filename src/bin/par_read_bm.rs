//! Parallel reading.
use aligned_vec::*;
use iobench::par_read::*;
//-----------------------------------------------------------------------------
fn main() -> std::io::Result<()> {
    let fname = &std::env::args().nth(1).expect("Missing file name");
    let chunk_size = std::env::args()
        .nth(2)
        .expect("Missing chunk size")
        .parse::<u64>()
        .expect("Wrong file size");
    let num_threads = std::env::args()
        .nth(3)
        .map_or(1, |v| v.parse::<u64>().expect("Wrong num threads number"));
    let fsize = std::fs::metadata(&fname)?.len() as f64;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    let fsize = fsize / 0x40000000 as f64;
    println!(
        "File size: {:.2} GiB, chunk size: {:.2} MiB, {} thread(s)",
        fsize,
        chunk_size as f64 / 0x100000 as f64,
        num_threads
    );
    #[cfg(feature = "par_read_all")]
    println!(
        "par_read_all:\t\t\t {:.2} GiB/s",
        fsize / par_read_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_read_buf_all")]
    println!(
        "par_read_buf_all:\t\t {:.2} GiB/s",
        fsize / par_read_buf_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_read_direct_all")]
    println!(
        "par_read_direct_all:\t\t {:.2} GiB/s",
        fsize / par_read_buf_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_read_pread_all")]
    println!(
        "par_read_pread_all:\t\t {:.2} GiB/s",
        fsize / par_read_pread_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_read_mmap_all")]
    println!(
        "par_read_mmap_all:\t\t {:.2} GiB/s",
        fsize / par_read_mmap_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_read_vec_all")]
    println!(
        "par_read_vec_all:\t\t {:.2} GiB/s",
        fsize / par_read_vec_all(fname, chunk_size, num_threads, &mut filebuf)?.as_secs_f64()
    );
    Ok(())
}
