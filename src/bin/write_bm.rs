//! Read/Write files using a variety of APIs in serial and parallel mode
use aligned_vec::*;
use iobench::write::*;
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
        .expect("Wrong number of chunk size");
    let fsize = num_chunks * chunk_size;
    let t = std::time::Instant::now();
    let filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    println!("Initialization time: {:.2}", t.elapsed().as_secs_f64());
    let fsize = fsize as f64 / 0x40000000 as f64;
    println!(
        "File size: {:.2} GiB, chunk size: {:.2} MiB",
        fsize,
        chunk_size as f64 / 0x100000 as f64
    );
    #[cfg(feature = "seq_write")]
    println!(
        "seq_write:\t\t\t {:.2} GiB/s",
        fsize / seq_write(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_all")]
    println!(
        "seq_write_all:\t\t\t {:.2} GiB/s",
        fsize / seq_write_all(fname, chunk_size, num_chunks, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_direct_all")]
    println!(
        "seq_write_direct_all:\t\t {:.2} GiB/s",
        fsize / seq_write_direct_all(fname, chunk_size, num_chunks, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_buf")]
    println!(
        "seq_write_buf:\t\t\t {:.2} GiB/s",
        fsize / seq_write_buf(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_buf_all")]
    println!(
        "seq_write_buf_all:\t\t {:.2} GiB/s",
        fsize / seq_write_buf_all(fname, chunk_size, num_chunks, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_mmap")]
    println!(
        "seq_write_mmap:\t\t\t {:.2} GiB/s",
        fsize / seq_write_mmap(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_mmap_all")]
    println!(
        "seq_write_mmap_all:\t\t {:.2} GiB/s",
        fsize / seq_write_mmap_all(fname, chunk_size, num_chunks, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_vec_all")]
    println!(
        "seq_write_vec_all:\t\t {:.2} GiB/s",
        fsize / seq_write_vec_all(fname, chunk_size, &filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_glommio_write")]
    println!(
        "seq_glommio_write:\t\t {:.2} GiB/s",
        fsize / seq_glommio_write(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "async_glommio_write")]
    println!(
        "async_glommio_write:\t\t {:.2} GiB/s",
        fsize / async_glommio_write(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    Ok(())
}
