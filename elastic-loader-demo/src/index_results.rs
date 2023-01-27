use std::ops::AddAssign;

pub struct IndexResults {
    pub num_total: usize,
    pub num_created: usize,
    pub num_failed: usize,
}

impl AddAssign for IndexResults {
    fn add_assign(&mut self, other: Self) {
        self.num_total += other.num_total;
        self.num_created += other.num_created;
        self.num_failed += other.num_failed;
    }
}