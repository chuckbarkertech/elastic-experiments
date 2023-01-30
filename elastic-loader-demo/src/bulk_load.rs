use crate::elastic_load::{ElasticLoad, ElasticLoadResults};
use serde_json::Value;
use serde::{Serialize};
use elasticsearch::Elasticsearch;
use elasticsearch::http::Url;
use elasticsearch::http::transport::SingleNodeConnectionPool;
use elasticsearch::http::transport::TransportBuilder;
use elasticsearch::http::response::Response;
use elasticsearch::{BulkOperation, BulkOperations};
use elasticsearch::BulkParts;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;

pub struct BulkElasticLoad {
    last_id: usize,
    client: Elasticsearch,
}

impl BulkElasticLoad {
    pub fn new(uri: &str) -> Result<BulkElasticLoad, Box<dyn std::error::Error>> {
        let url = Url::parse(uri)?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool).disable_proxy()
            .auth(Credentials::Basic(String::from("elastic"), String::from("elastic")))
            .cert_validation(CertificateValidation::None).build()?;
        let client = Elasticsearch::new(transport);
        Ok(BulkElasticLoad {
            last_id: 0,
            client,
        })
    }

    async fn load<T: Serialize>(&mut self, items: &[Box<T>]) -> Result<Response, Box<dyn std::error::Error>> {
        let mut ops = BulkOperations::new();
        for item in items {
            self.last_id += 1;
            ops.push(BulkOperation::create(&self.last_id.to_string(), item))?;
            // ops.push(BulkOperation::index(item))?;
        }

        let response = self.client.bulk(BulkParts::Index("motor-vehicle-crashes"))
            .body(vec![ops])
            .send()
            .await?;

        Ok(response)
    }

    async fn summarize_response(&self, response: Response) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
        let response = response.json::<Value>().await?;
        let items = response["items"].as_array().unwrap();
        let mut num_created = 0;
        let mut num_failed = 0;
        for item in items {
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
        Ok(ElasticLoadResults {
            num_failed,
            num_created,
            num_total: num_created + num_failed,
        })
    }
}

impl ElasticLoad for BulkElasticLoad {
    async fn load<T: Serialize>(&mut self, items: &[Box<T>]) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
        let mut tally = ElasticLoadResults::new();
        let items_length = items.len();
        for first_idx in (0..items_length).step_by(10_000) {
            let last_idx = {
                let last_idx = first_idx + 10_000;
                if last_idx < items_length { last_idx } else { items_length }
            };
            let response = BulkElasticLoad::load(self,
                                                 &items[first_idx..last_idx]).await?;
            tally += self.summarize_response(response).await?;
        }
        Ok(tally)
    }
}