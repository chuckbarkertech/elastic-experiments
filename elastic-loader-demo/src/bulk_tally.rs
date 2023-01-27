use serde_json::Value;
use std::ops::AddAssign;

pub struct BulkTally {
    pub num_total: usize,
    pub num_created: usize,
    pub num_failed: usize,
}

impl AddAssign for BulkTally {
    fn add_assign(&mut self, other: Self) {
        self.num_total += other.num_total;
        self.num_created += other.num_created;
        self.num_failed += other.num_failed;
    }
}

impl BulkTally {
    pub fn new() -> BulkTally {
        BulkTally {
            num_total: 0,
            num_created: 0,
            num_failed: 0,
        }
    }
    pub async fn parse(bulk_response: elasticsearch::http::response::Response) -> Result<BulkTally, Box<dyn std::error::Error>> {
        let bulk_response = bulk_response.json::<Value>().await?;
        let items_list = bulk_response["items"].as_array().unwrap();
        let mut num_created = 0;
        let mut num_failed = 0;
        for item in items_list {
            if let Value::Object(item) = item {
                if let Value::Object(created_item) = item.get("create").unwrap() {
                    if created_item.contains_key("errors") {
                        num_failed += 1;
                    } else {
                        num_created += 1;
                    }
                } else {
                    return Err("found response besides create and errors".into());
                }
            }
        }
        Ok(BulkTally {
            num_total: num_created + num_failed,
            num_created,
            num_failed,
        })
    }
}
