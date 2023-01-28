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
use async_trait::async_trait;

pub struct BulkElasticLoad {
    client: Elasticsearch,
}

impl BulkElasticLoad {
    pub fn new(uri: &str) -> Result<BulkElasticLoad, Box<dyn std::error::Error>> {
        let url = Url::parse(uri)?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
        let client = Elasticsearch::new(transport);
        Ok(BulkElasticLoad {
            client,
        })
    }

    async fn load<T: Serialize>(&self, items: &Vec<Box<T>>) -> Result<Response, Box<dyn std::error::Error>> {
        let mut ops = BulkOperations::new();
        for item in items {
            ops.push(BulkOperation::create(String::from("1"), item))?;
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

#[async_trait]
impl ElasticLoad for BulkElasticLoad {
    async fn load<T: Serialize + std::marker::Send + std::marker::Sync>(&self, items: &Vec<Box<T>>) -> Result<ElasticLoadResults, String> {
        let response = BulkElasticLoad::load(self, items).await.unwrap_or_else(|_| {
            panic!("breaking");
        });
        let results = self.summarize_response(response).await.unwrap_or_else(|_| {
            panic!("breaking");
        });
        Ok(results)
    }
}