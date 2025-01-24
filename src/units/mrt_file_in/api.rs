use std::path::Path;
use std::{ops::Deref, path::PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};
use log::debug;
use tokio::sync::mpsc::Sender;

use crate::http::{
    extract_params, get_param, MatchedParam, PercentDecodedPath, ProcessRequest, QueryParams
};

pub struct Processor {
    http_api_path: Arc<String>,
    queue_tx: Sender<PathBuf>,
}

impl Processor {
    pub fn new(
        http_api_path: Arc<String>,
        queue_tx: Sender<PathBuf>,
    ) -> Self {
        Self {
            http_api_path,
            queue_tx,
        }
    }
}


#[async_trait]
impl ProcessRequest for Processor {
    async fn process_request(
        &self,
        request: &Request<Body>,
    ) -> Option<Response<Body>> {
        let req_path = request.uri().decoded_path();
        dbg!(request);
        if request.method() != Method::GET {
            return None;
        }
        let action = match req_path.strip_prefix(&*self.http_api_path) {
            Some(action) => action,
            None => return None,
        };

        if action.starts_with("queue") {
            return self.queue(request).await;
        }

    None
    }

}
impl Processor {
    async fn queue(&self, request: &Request<Body>) -> Option<Response<Body>> {
        let params = extract_params(request);
        dbg!(&params);
        let filename = match get_param(&params, "file") {
            Some(MatchedParam::Exact(file)) => file,
            Some(MatchedParam::Family(..)) |
            None => return Some(Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .header("Content-Type", "text/plain")
                .body("missing or invalid required param 'file'".into())
                .unwrap())
        };
        let filename = Path::new(filename);
        if !filename.is_relative() {
            return Some(err("not relative"));
        }

        let update_dir = Path::new("test-data/").canonicalize().unwrap(); // TODO make conigurable
        let mut full_path: PathBuf = update_dir.clone();
        
        full_path.push(filename);
        debug!("fill path pre canonicalize: {}", &full_path.to_string_lossy());

        let full_path = match full_path.canonicalize() {
            Ok(path) => {
                debug!("canonicalized to {}", &path.to_string_lossy());
                path
            }
            Err(e) => {
                return Some(
                    err(format!("file does not exist or path invalid: {}", e))
                );
            }
        };
        if !full_path.ancestors().any(|a| a == update_dir) {
            return Some(err("file not under configured directory"));
        }

        debug!("queueing {}", &full_path.to_string_lossy());
        let _ = self.queue_tx.send(full_path.clone()).await;

        Some(Response::builder()
            .status(hyper::StatusCode::OK)
            .header("Content-Type", "text/plain")
            .body(format!("queued {} for processing", full_path.to_string_lossy()).into())
            .unwrap())
    }
}

fn err(msg: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(hyper::StatusCode::BAD_REQUEST)
        .header("Content-Type", "text/plain")
        .body(msg.into())
        .unwrap()
}
