//! Read/Write files using a variety of APIs in serial and parallel mode
mod read;
use read::*;
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
    #[cfg(feature="seq_read_direct_all")]
    println!(
        "seq_read_direct_all:\t\t {:.2} GiB/s",
        fsize / seq_read_direct_all(fname, chunk_size)?.as_secs_f64()
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
