pub struct Movable<T>(pub *const T);
impl<T> Movable<T> {
     pub fn get(&self) -> Option<*const T> {
         if self.0.is_null() {
             return None;
         }
         Some(self.0)
     }
 }

pub struct MovableMut<T>(pub *mut T);
impl<T> MovableMut<T> {
    pub fn get(&self) -> Option<*mut T> {
        if self.0.is_null() {
            return None;
        }
        Some(self.0)
    }
}

unsafe impl<T> Send for Movable<T> {}
unsafe impl<T> Send for MovableMut<T> {}
//----------j------------------------------------------------------------------
pub fn dump(v: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::open("/dev/null")?;
    let _ = f.write(&v[..1]);
    Ok(())
}
