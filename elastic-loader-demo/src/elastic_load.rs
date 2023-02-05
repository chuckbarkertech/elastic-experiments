use serde::{Serialize};
use std::ops::AddAssign;

pub trait ElasticLoad {
    // async fn reset_index(&self) ->  Result<(), Box<dyn std::error::Error>>;
    async fn load<T: Serialize>(&self, items: &[Box<T>]) ->  Result<ElasticLoadResults, Box<dyn std::error::Error>>;
}

pub struct ElasticLoadResults {
    pub num_total: usize,
    pub num_created: usize,
    pub num_failed: usize,
}

impl ElasticLoadResults {
    pub fn new() -> ElasticLoadResults {
        ElasticLoadResults {
            num_total: 0,
            num_created: 0,
            num_failed: 0
        }
    }
}

impl AddAssign for ElasticLoadResults {
    fn add_assign(&mut self, other: Self) {
        self.num_total += other.num_total;
        self.num_created += other.num_created;
        self.num_failed += other.num_failed;
    }
}