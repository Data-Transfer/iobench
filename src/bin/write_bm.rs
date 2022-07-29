//! Read/Write files using a variety of APIs in serial and parallel mode
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
    let fsize = ((num_chunks as u64 * chunk_size) as f64) / 0x40000000 as f64;
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
        fsize / seq_write_all(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_write_direct_all")]
    println!(
        "seq_write_direct_all:\t\t {:.2} GiB/s",
        fsize / seq_write_direct_all(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_buf_write")]
    println!(
        "seq_buf_write:\t\t\t {:.2} GiB/s",
        fsize / seq_buf_write(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_buf_write_all")]
    println!(
        "seq_buf_write_all:\t\t {:.2} GiB/s",
        fsize / seq_buf_write_all(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_mmap_read")]
    println!(
        "seq_mmap_write:\t\t\t {:.2} GiB/s",
        fsize / seq_mmap_write(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_mmap_write_all")]
    println!(
        "seq_mmap_write_all:\t\t {:.2} GiB/s",
        fsize / seq_mmap_write_all(fname, chunk_size, num_chunks)?.as_secs_f64()
    );
    #[cfg(feature = "seq_vec_write_all")]
    println!(
        "seq_vec_write_all:\t\t {:.2} GiB/s",
        fsize / seq_vec_write_all(fname, chunk_size, num_chunks)?.as_secs_f64()
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
