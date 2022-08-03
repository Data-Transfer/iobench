//! Parallel reading.
use aligned_vec::*;
use iobench::par_write::*;
//-----------------------------------------------------------------------------
fn main() -> std::io::Result<()> {
    let fname = &std::env::args().nth(1).expect("Missing file name");
    let chunk_size = std::env::args()
        .nth(2)
        .expect("Missing chunk size")
        .parse::<u64>()
        .expect("Wrong file size");
    let num_chunks = std::env::args()
        .nth(3)
        .expect("Missing number of chunks")
        .parse::<u64>()
        .expect("Wrong number of chunks");
    let num_threads = std::env::args()
        .nth(4)
        .map_or(1, |v| v.parse::<u64>().expect("Wrong num threads number"));
    let fsize = num_chunks * chunk_size;
    let t = std::time::Instant::now();
    let filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    println!("Initialization time: {:.2} s", t.elapsed().as_secs_f64());
    let fsize = fsize as f64 / 0x40000000 as f64;
    println!(
        "File size: {:.2} GiB, chunk size: {:.2} MiB, {} thread(s)",
        fsize,
        chunk_size as f64 / 0x100000 as f64,
        num_threads
    );
    #[cfg(feature = "par_write_all")]
    println!(
        "par_write_all:\t\t\t {:.2} GiB/s",
        fsize / par_write_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "par_write_buf_all")]
    println!(
        "par_write_buf_all:\t\t {:.2} GiB/s",
        fsize
            / par_write_buf_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?
                .as_secs_f64()
    );
    #[cfg(feature = "par_write_direct_all")]
    println!(
        "par_write_direct_all:\t\t {:.2} GiB/s",
        fsize
            / par_write_direct_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?
                .as_secs_f64()
    );
    #[cfg(feature = "par_write_pwrite_all")]
    println!(
        "par_write_pwrite_all:\t\t {:.2} GiB/s",
        fsize
            / par_write_pwrite_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?
                .as_secs_f64()
    );
    #[cfg(feature = "par_write_mmap_all")]
    println!(
        "par_write_mmap_all:\t\t {:.2} GiB/s",
        fsize
            / par_write_mmap_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?
                .as_secs_f64()
    );
    #[cfg(feature = "par_write_vec_all")]
    println!(
        "par_write_vec_all:\t\t {:.2} GiB/s",
        fsize
            / par_write_vec_all(fname, chunk_size, num_chunks, num_threads, &filebuf)?
                .as_secs_f64()
    );
    Ok(())
}
