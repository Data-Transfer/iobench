use std::time::{Duration, Instant};

// Create type to move pointer can extend with new
// returning either wrapped pointer or None when pointer is null
#[derive(Copy, Clone)]
struct Movable<T>(*const T);

impl<T> Movable<T> {
    fn get(&self) -> Option<*const T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}
#[derive(Copy, Clone)]
struct MovableMut<T>(*mut T);

impl<T> MovableMut<T> {
    fn get(&self) -> Option<*mut T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}

unsafe impl<T> Send for Movable<T> {}
unsafe impl<T> Send for MovableMut<T> {}

const SIZE_U8: usize = 0x40000000;
const SIZE_U64: usize = 0x8000000;
fn cp1<T: Copy>(src: &[T], dst: &mut [T]) {
    dst.copy_from_slice(src);
}
fn cp2<T: Clone>(src: &[T], dst: &mut [T]) {
    dst.clone_from_slice(src);
}
fn cp3<T>(src: &[T], dst: &mut [T]) {
    unsafe {
        libc::memcpy(
            dst.as_mut_ptr() as *mut libc::c_void,
            src.as_ptr() as *const libc::c_void,
            dst.len(),
        );
    }
}
fn par_cp<T: 'static>(src: &[T], dst: &mut [T], n: usize) {
    assert!(src.len() % n == 0);
    let mut th = vec![];
    let cs = src.len() / n;
    for i in 0..n {
        unsafe {
            let idx = (n * i) as isize;
            let s = Movable(src.as_ptr().offset(idx));
            let d = MovableMut(dst.as_mut_ptr().offset(idx));
            th.push(std::thread::spawn(move || {
                libc::memcpy(
                    d.get().unwrap() as *mut libc::c_void,
                    s.get().unwrap() as *const libc::c_void,
                    cs,
                );
            }))
        }
    }
    for t in th {
        t.join().unwrap();
    }
}
fn par_cp2<T: 'static + Copy>(src: &[T], dst: &mut [T], n: usize) {
    assert!(src.len() % n == 0);
    let mut th = vec![];
    let cs = src.len() / n;
    for i in (0..n) {
        unsafe {
            let idx = (n * i) as isize;
            let s = Movable(src.as_ptr().offset(idx));
            let d = MovableMut(dst.as_mut_ptr().offset(idx));
            th.push(std::thread::spawn(move || {
                let src = std::slice::from_raw_parts(s.get().unwrap(), cs);
                let dst = std::slice::from_raw_parts_mut(d.get().unwrap(), cs);
                dst.copy_from_slice(src);
            }))
        }
    }
    for t in th {
        t.join().unwrap();
    }
}

#[cfg(target_arch="x86_64")]
fn cp_avx<T: 'static + Copy>(src: &[T], dst: &mut [T]) {
   for i in (0..src.len()).step_by(256) {
    unsafe {
    // let base = 0;
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 32) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 64) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 96) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 128) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 160) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 192) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    // std::arch::x86_64::_mm_prefetch(src.as_ptr().offset(i as isize + base + 256) as *const i8, std::arch::x86_64::_MM_HINT_T1);
    let d = dst.as_mut_ptr().offset(i as isize) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 32) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 32) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 64) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 64) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 96) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 96) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 128) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 128) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 160) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 160) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 192) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 192) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
    let d = dst.as_mut_ptr().offset(i as isize + 192) as *mut std::arch::x86_64::__m256i;
    let s = std::arch::x86_64::_mm256_load_si256(src.as_ptr().offset(i as isize + 192) as *const std::arch::x86_64::__m256i);
    std::arch::x86_64::_mm256_store_si256(d, s);
   }}
}

fn main() {
    // let src = vec![0_u64; SIZE_U64];
    // let mut dest = vec![0_u64; SIZE_U64];
    let src = page_aligned_vec::<u8>(SIZE_U8, SIZE_U8);
    let mut dest = page_aligned_vec::<u8>(SIZE_U8, SIZE_U8);
    let t = Instant::now();
    //cp3(&src, &mut dest);
    //par_cp(&src, &mut dest, 2);
    cp_avx(&src, &mut dest);
    let e = t.elapsed().as_millis();
    println!("{} {}", dest[100], e);
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
fn page_aligned_vec<T: Sized>(size: usize, capacity: usize) -> Vec<T> {
    aligned_vec::<T>(size, capacity, page_size::get())
}
