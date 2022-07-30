//! Read/Write files using a variety of APIs in serial and parallel mode
use aligned_vec::*;
use iobench::read::*;
//-----------------------------------------------------------------------------
fn main() -> std::io::Result<()> {
    let fname = &std::env::args().nth(1).expect("Missing file name");
    let chunk_size = std::env::args()
        .nth(2)
        .expect("Missing chunk size")
        .parse::<u64>()
        .expect("Wrong file size");
    let fsize = std::fs::metadata(&fname)?.len() as f64;
    let mut filebuf: Vec<u8> = page_aligned_vec(fsize as usize, fsize as usize, Some(0), false);
    let fsize = fsize / 0x40000000 as f64;
    println!(
        "File size: {:.2} GiB, chunk size: {:.2} MiB",
        fsize,
        chunk_size as f64 / 0x100000 as f64
    );
    #[cfg(feature = "seq_read")]
    println!(
        "seq_read:\t\t\t {:.2} GiB/s",
        fsize / seq_read(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_all")]
    println!(
        "seq_read_all:\t\t\t {:.2} GiB/s",
        fsize / seq_read_all(fname, chunk_size, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_direct_all")]
    println!(
        "seq_read_direct_all:\t\t {:.2} GiB/s",
        fsize / seq_read_direct_all(fname, chunk_size, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_buf")]
    println!(
        "seq_read_buf:\t\t\t {:.2} GiB/s",
        fsize / seq_read_buf(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_buf_all")]
    println!(
        "seq_read_buf_all:\t\t {:.2} GiB/s",
        fsize / seq_read_buf_all(fname, chunk_size, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_mmap")]
    println!(
        "seq_read_mmap:\t\t\t {:.2} GiB/s",
        fsize / seq_read_mmap(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_mmap_all")]
    println!(
        "seq_read_mmap_all:\t\t {:.2} GiB/s",
        fsize / seq_read_mmap_all(fname, chunk_size, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_read_vec_all")]
    println!(
        "seq_read_vec_all:\t\t {:.2} GiB/s",
        fsize / seq_read_vec_all(fname, chunk_size, &mut filebuf)?.as_secs_f64()
    );
    #[cfg(feature = "seq_glommio_read")]
    println!(
        "seq_glommio_read:\t\t {:.2} GiB/s",
        fsize / seq_glommio_read(fname, chunk_size)?.as_secs_f64()
    );
    #[cfg(feature = "async_glommio_read")]
    println!(
        "async_glommio_read:\t\t {:.2} GiB/s",
        fsize / async_glommio_read(fname, chunk_size)?.as_secs_f64()
    );
    Ok(())
}
