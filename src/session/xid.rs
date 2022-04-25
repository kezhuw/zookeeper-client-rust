#[derive(Default)]
pub struct Xid {
    prev: i32,
}

impl Xid {
    pub fn next(&mut self) -> i32 {
        if self.prev == i32::MAX {
            self.prev = 1;
        } else {
            self.prev += 1;
        };
        self.prev
    }
}
