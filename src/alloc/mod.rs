//-----------------------------------------------------------------------------
pub fn aligned_vec<T: Sized + Copy>(
    size: usize,
    capacity: usize,
    align: usize,
    touch: Option<T>,
) -> Vec<T> {
    unsafe {
        if size == 0 {
            Vec::<T>::new()
        } else {
            let size = size * std::mem::size_of::<T>();
            let capacity = (capacity * std::mem::size_of::<T>()).max(size);

            let layout = std::alloc::Layout::from_size_align_unchecked(size, align);
            let raw_ptr = std::alloc::alloc(layout) as *mut T;
            if let Some(x) = touch {
                let mut v = Vec::from_raw_parts(raw_ptr, size, capacity);
                for i in (0..size).step_by(page_size::get()) {
                    v[i] = x;
                }
                v
            } else {
                //SLOW!
                //nix::sys::mman::mlock(raw_ptr as *const c_void, size).unwrap();
                Vec::from_raw_parts(raw_ptr, size, capacity)
            }
        }
    }
}
//-----------------------------------------------------------------------------
pub fn page_aligned_vec<T: Sized + Copy>(size: usize, capacity: usize, touch: Option<T>) -> Vec<T> {
    aligned_vec::<T>(size, capacity, page_size::get(), touch)
}
