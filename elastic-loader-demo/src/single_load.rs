use crate::elastic_load::{ElasticLoad, ElasticLoadResults};
use serde_json::{json};
use serde::{Serialize};
use elasticsearch::{Elasticsearch, IndexParts};
use elasticsearch::http::{Url};
use elasticsearch::http::transport::SingleNodeConnectionPool;
use elasticsearch::http::transport::TransportBuilder;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::params::Refresh;
use tokio::sync::{Semaphore};
use futures::stream::{FuturesUnordered, StreamExt};

pub struct SingleElasticLoad {
    client: Elasticsearch,
    index: String,
    semaphore: Semaphore,
    refresh: Refresh,
}

impl SingleElasticLoad {
    pub fn builder() -> SingleElasticLoadBuilder {
        SingleElasticLoadBuilder::new()
    }

    // async fn delete_index(&self) -> Result<(), Box<dyn std::error::Error>> {
    //     self.client
    //         .indices()
    //         .delete(IndicesDeleteParts::Index(&[self.index.as_str()]))
    //         .send()
    //         .await?;
    //     Ok(())
    // }
    //
    // async fn create_index(&self) -> Result<(), Box<dyn std::error::Error>> {
    //     self.client
    //         .indices()
    //         .create(IndicesCreateParts::Index(&self.index))
    //         .body(json!({
    //             "settings": {
    //                 "index": {
    //                     "number_of_shards": "3",
    //                     "number_of_replicas": "1",
    //                     "refresh_interval": "1s",
    //                 }
    //             }
    //         }))
    //         .send()
    //         .await?;
    //     Ok(())
    // }

    async fn load_item<T: Serialize>(&self, idx: usize, item: &Box<T>) -> bool {
        let _permit = self.semaphore.acquire().await.unwrap();
        let response = self.client
            .index(IndexParts::IndexId(&self.index, &idx.to_string()))
            .body(json!(item))
            .refresh(self.refresh)
            .send()
            .await;
        match response {
            Ok(response) => response.status_code().is_success(),
            Err(_) => false,
        }
    }
}

impl ElasticLoad for SingleElasticLoad {
    // async fn reset_index(&self) -> Result<(), Box<dyn std::error::Error>> {
    //     self.delete_index().await?;
    //     self.create_index().await?;
    //     Ok(())
    // }

    async fn load<T: Serialize>(&self, items: &[Box<T>]) -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
        let mut responses = items.iter().enumerate()
            .map(|(idx, item)| self.load_item(idx, item))
            .collect::<FuturesUnordered<_>>();

        let mut successes: usize = 0;
        let mut failures: usize = 0;
        while let Some(response) = responses.next().await {
            if response {
                successes += 1;
            } else {
                failures += 1;
            }
        }

        Ok(ElasticLoadResults {
            num_failed: failures,
            num_created: successes,
            num_total: failures + successes,
        })
    }
}

pub struct SingleElasticLoadBuilder {
    uri: String,
    credentials: Option<Credentials>,
    index: Option<String>,
    throttle: usize,
    refresh: Refresh,
}

const DEFAULT_SIMULTANEOUS_REQUESTS: usize = 1;

impl SingleElasticLoadBuilder {
    pub fn new() -> SingleElasticLoadBuilder {
        SingleElasticLoadBuilder {
            uri: String::from("https://127.0.0.1:9200/"),
            credentials: None,
            index: None,
            throttle: DEFAULT_SIMULTANEOUS_REQUESTS,
            refresh: Refresh::False,
        }
    }

    pub fn with_uri(mut self, uri: String) -> SingleElasticLoadBuilder {
        self.uri = uri;
        self
    }

    pub fn with_credentials(mut self, credentials: Credentials) -> SingleElasticLoadBuilder {
        self.credentials = Option::from(credentials);
        self
    }

    pub fn with_throttle(mut self,throttle: usize) -> SingleElasticLoadBuilder {
        self.throttle = throttle;
        self
    }

    pub fn with_index(mut self, index: String) -> SingleElasticLoadBuilder {
        self.index = Some(index);
        self
    }

    pub fn with_refresh(mut self, refresh: Refresh) -> SingleElasticLoadBuilder {
        self.refresh = refresh;
        self
    }

    pub fn build(self) -> Result<SingleElasticLoad, Box<dyn std::error::Error>> {
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
        match self.index {
            Some(index) => Ok(SingleElasticLoad {
                client,
                index,
                semaphore: Semaphore::new(self.throttle),
                refresh: self.refresh,
            }),
            None => Err("Index name is required.".into())
        }
    }
}

