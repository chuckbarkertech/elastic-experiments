use crate::elastic_load::{ElasticLoad, ElasticLoadResults};
use serde_json::{json, Value};
use serde::{Serialize};
use elasticsearch::{Elasticsearch};
use elasticsearch::http::{Url};
use elasticsearch::http::transport::SingleNodeConnectionPool;
use elasticsearch::http::transport::TransportBuilder;
use elasticsearch::http::response::Response;
use elasticsearch::{BulkOperation, BulkOperations};
use elasticsearch::BulkParts;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts};
use elasticsearch::params::Refresh;
use tokio::sync::{Semaphore};
use futures::stream::{FuturesUnordered, StreamExt};

pub struct BulkElasticLoad {
    client: Elasticsearch,
    index: Option<String>,
    batch_size: usize,
    refresh: Refresh,
    semaphore: Semaphore,
}

impl BulkElasticLoad {
    pub fn builder() -> BulkElasticLoadBuilder {
        BulkElasticLoadBuilder::new()
    }
}

impl BulkElasticLoad {
    async fn delete_index(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = &self.index {
            self.client
                .indices()
                .delete(IndicesDeleteParts::Index(&[index.as_str()]))
                .send()
                .await?;
        }
        Ok(())
    }

    async fn create_index(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = &self.index {
            self.client
                .indices()
                .create(IndicesCreateParts::Index(&index))
                .body(json!({
                    "settings": {
                        "index": {
                            "number_of_shards": "3",
                            "number_of_replicas": "1",
                            "refresh_interval": "1s",
                        }
                    }
                }))
                .send()
                .await?;
        }
        Ok(())
    }

    async fn bulk_load_data<T: Serialize>(&self, items: &[Box<T>], mut start_id: usize) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
        let permit = self.semaphore.acquire().await.unwrap();
        let mut ops = BulkOperations::new();
        for item in items {
            start_id += 1;
            ops.push(BulkOperation::create(&start_id.to_string(), item))?;
        }

        let bulk_parts = match &self.index {
            Some(index) => BulkParts::Index(index),
            None => BulkParts::None,
        };
        let response =
            self.client
                .bulk(bulk_parts)
                .refresh(self.refresh)
                .body(vec![ops])
                .send()
                .await?;

        let tally = self.summarize_bulk_load_response(response).await?;
        drop(permit);

        Ok(tally)
    }

    async fn summarize_bulk_load_response(&self, response: Response) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
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
    async fn reset_index(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.delete_index().await?;
        self.create_index().await?;
        Ok(())
    }

    async fn load<T: Serialize>(&self, items: &[Box<T>]) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
        let items_length = items.len();
        // for first_idx in (0..items_length).step_by(self.batch_size) {
        //     let last_idx = {
        //         let last_idx = first_idx + self.batch_size;
        //         if last_idx < items_length { last_idx } else { items_length }
        //     };
        //     let response =
        //         self.bulk_load_data(&items[first_idx..last_idx], first_idx).await?;
        //     tally += self.summarize_bulk_load_response(response).await?;
        // }
        let responses = (0..items_length)
            .step_by(self.batch_size)
            .map(|first_idx| {
                let last_idx = {
                    let last_idx = first_idx + self.batch_size;
                    if last_idx < items_length { last_idx } else { items_length }
                };
                let tally = self.bulk_load_data(&items[first_idx..last_idx], first_idx);
                tally
            })
            .collect::<FuturesUnordered<_>>();

        //     match response {
        //         Ok(response) => {
        //             let tally = self.summarize_bulk_load_response(response).await;
        //             match tally {
        //                 Ok(tally) => Ok(tally),
        //                 Err(e) => Err(e),
        //             }
        //         }
        //         Err(e) => Err(e),
        //     }
        //     // let tally = ;
        //     // tally
        // }


        // let mut tallies = responses.next().await
        //     .into_iter()
        //     .flatten()
        //     .map(|response| {
        //         let tally =
        //         tally
        //     })
        //     .collect::<FuturesUnordered<_>>();

        let tallies = responses
            .collect::<Vec<Result<ElasticLoadResults, Box<dyn std::error::Error>>>>().await
            .into_iter()
            // .into_iter()
            .flatten()
            .collect::<Vec<ElasticLoadResults>>();

        let mut tally_total = ElasticLoadResults::new();
        for tally in tallies {
            tally_total += tally;
        }

        //
        // let tally = ElasticLoadResults {
        //     num_failed: 0,
        //     num_created: 0,
        //     num_total: 0,
        // };
        // tally += self.summarize_bulk_load_response(response).await?;
        // let tally = ElasticLoadResults::new();
        // while let Some(response) = responses.next().await {
        // }
        Ok(tally_total)
    }
}

pub struct BulkElasticLoadBuilder {
    uri: String,
    credentials: Option<Credentials>,
    index: Option<String>,
    batch_size: usize,
    refresh: Refresh,
    throttle: usize,
}

const DEFAULT_BULK_SIZE: usize = 10_000;

impl BulkElasticLoadBuilder {
    pub fn new() -> BulkElasticLoadBuilder {
        BulkElasticLoadBuilder {
            uri: String::from("https://127.0.0.1:9200/"),
            credentials: None,
            index: None,
            batch_size: DEFAULT_BULK_SIZE,
            refresh: Refresh::False,
            throttle: 1,
        }
    }

    pub fn with_uri(mut self, uri: String) -> BulkElasticLoadBuilder {
        self.uri = uri;
        self
    }

    pub fn with_credentials(mut self, credentials: Credentials) -> BulkElasticLoadBuilder {
        self.credentials = Option::from(credentials);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> BulkElasticLoadBuilder {
        self.batch_size = batch_size;
        self
    }

    pub fn with_throttle(mut self, throttle: usize) -> BulkElasticLoadBuilder {
        self.throttle = throttle;
        self
    }

    pub fn with_refresh(mut self, refresh: Refresh) -> BulkElasticLoadBuilder {
        self.refresh = refresh;
        self
    }

    pub fn with_index(mut self, index: String) -> BulkElasticLoadBuilder {
        self.index = Some(index);
        self
    }

    pub fn build(self) -> Result<BulkElasticLoad, Box<dyn std::error::Error>> {
        let url = Url::parse(&self.uri)?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let mut transport_builder = TransportBuilder::new(conn_pool)
            .disable_proxy()
            .cert_validation(CertificateValidation::None);
        if let Some(credentials) = self.credentials {
            transport_builder = transport_builder.auth(credentials);
        }
        let transport = transport_builder.build()?;
        let client = Elasticsearch::new(transport);
        Ok(BulkElasticLoad {
            client,
            index: self.index,
            batch_size: self.batch_size,
            refresh: self.refresh,
            semaphore: Semaphore::new(self.throttle),
        })
    }
}

