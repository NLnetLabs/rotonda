use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::http::ProcessRequest;

#[derive(Clone)]
pub struct RouterInfo {
    pub connected_at: DateTime<Utc>,
    pub last_msg_at: Arc<RwLock<DateTime<Utc>>>,
    // this is just a place to store a strong reference to the processor
    // otherwise the weak reference held by the HTTP framework will be dropped
    pub api_processor: Option<Arc<dyn ProcessRequest>>,
}

impl RouterInfo {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            connected_at: now,
            last_msg_at: Arc::new(RwLock::new(now)),
            api_processor: None,
        }
    }
}
