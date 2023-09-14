//! The HTTP server.
//!
//! Because with HTTP you can select what information you want per request,
//! we only have one HTTP server for the entire instance. HTTP targets will
//! provide their data via a specific base path within that server.
//!
//! Server configuration happens via the [`Server`] struct that normally is
//! part of the [`Config`](crate::config::Config).

use crate::log::ExitError;
use crate::metrics;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::pin_mut;
use hyper::server::accept::Accept;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode, Uri};
use log::{error, info, trace};
use percent_encoding::percent_decode;
use serde::Deserialize;
use serde_with::{serde_as, OneOrMany};
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt::Display;
use std::net::SocketAddr;
use std::net::TcpListener as StdListener;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::{fmt, io};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use url::form_urlencoded::parse;

#[cfg(not(feature = "http-api-gzip"))]
use log::warn;

//------------ Server --------------------------------------------------------

/// The configuration for the HTTP server.
#[serde_as]
#[derive(Clone, Deserialize)]
#[cfg_attr(test, derive(Default))]
pub struct Server {
    /// The socket addresses to listen on.
    #[serde_as(deserialize_as = "OneOrMany<_>")]
    #[serde(rename = "http_listen")]
    listen: Vec<SocketAddr>,

    /// Whether or not to support GZIP response compression
    #[serde(default = "Server::default_compress_responses")]
    compress_responses: bool,
}

impl Server {
    #[cfg(feature = "http-api-gzip")]
    pub fn default_compress_responses() -> bool {
        true
    }

    #[cfg(not(feature = "http-api-gzip"))]
    pub fn default_compress_responses() -> bool {
        false
    }

    /// Runs the server.
    ///
    /// The method will start a new server listening on the sockets provided
    /// via the configuration and spawns it onto the given `runtime`. The
    /// method should be run before `runtime` is started. It will
    /// synchronously create and bind all required sockets before returning.
    ///
    /// The server will use `metrics` to produce information on its metrics
    /// related endpoints.
    pub fn run(
        &self,
        metrics: metrics::Collection,
        mut resources: Resources,
    ) -> Result<(), ExitError> {
        // Bind and collect all listeners first so we can error out
        // if any of them fails.
        let mut listeners = Vec::new();
        for addr in &self.listen {
            // Binding needs to have happened before dropping privileges
            // during detach. So we do this here synchronously.
            let listener = match StdListener::bind(addr) {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Fatal: error listening on {}: {}", addr, err);
                    return Err(ExitError);
                }
            };
            if let Err(err) = listener.set_nonblocking(true) {
                error!(
                    "Fatal: failed to set listener {} to non-blocking: {}.",
                    addr, err
                );
                return Err(ExitError);
            }
            info!("Listening for HTTP connections on {}", addr);
            listeners.push(listener);
        }

        // Pass any flags along which should be used to influence request and
        // response handling.
        resources.compress_responses = self.compress_responses;

        #[cfg(not(feature = "http-api-gzip"))]
        if resources.compress_responses {
            warn!("HTTP response GZIP compression requested but not available");
        }

        // Now spawn the listeners onto the runtime. This way, they will start
        // doing their thing as soon as the runtime is started.
        for listener in listeners {
            crate::tokio::spawn(
                &format!("http-listener[{}]", listener.local_addr().unwrap()),
                Self::single_listener(listener, metrics.clone(), resources.clone()),
            );
        }
        Ok(())
    }

    /// Runs a single HTTP listener.
    ///
    /// Currently, this async function only resolves if the underlying
    /// listener encounters an error.
    async fn single_listener(
        listener: StdListener,
        metrics: metrics::Collection,
        resources: Resources,
    ) {
        let make_service = make_service_fn(|conn: &HttpStream| {
            let metrics = metrics.clone();
            let resources = resources.clone();
            let client_ip = Arc::new(conn.sock().peer_addr().map_or_else(|_err| "-".to_string(), |addr| addr.to_string()));
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let metrics = metrics.clone();
                    let resources = resources.clone();
                    let client_ip = client_ip.clone();
                    async move {
                        if log::log_enabled!(log::Level::Trace) {
                            let request_line = format!("{} {} {:?}", req.method(), req.uri(), req.version());
                            let res = Self::handle_request(req, &metrics, &resources).await;
                            if let Ok(res) = &res {
                                let status_code = res.status().as_u16();
                                trace!("{client_ip} - - {request_line} {status_code} -");
                            }
                            res
                        } else {
                            Self::handle_request(req, &metrics, &resources).await
                        }
                    }
                }))
            }
        });
        let listener = match TcpListener::from_std(listener) {
            Ok(listener) => listener,
            Err(err) => {
                error!("Error on HTTP listener: {}", err);
                return;
            }
        };
        if let Err(err) = hyper::Server::builder(HttpAccept { sock: listener })
            .serve(make_service)
            .await
        {
            error!("HTTP server error: {}", err);
        }
    }

    /// Handles a single HTTP request.
    async fn handle_request(
        req: Request<Body>,
        metrics: &metrics::Collection,
        resources: &Resources,
    ) -> Result<Response<Body>, Infallible> {
        if *req.method() != Method::GET {
            return Ok(Self::method_not_allowed());
        }

        let res = match req.uri().decoded_path().as_ref() {
            "/metrics" => Self::metrics(metrics),
            "/status" => Self::status(metrics),
            _ => match resources.process_request(&req).await {
                Some(response) => response,
                None => Self::not_found(),
            },
        };

        Ok(Self::encode_response(req, res, resources.compress_responses).await)
    }

    /// Produces the response for a call to the `/metrics` endpoint.
    fn metrics(metrics: &metrics::Collection) -> Response<Body> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .body(metrics.assemble(metrics::OutputFormat::Prometheus).into())
            .unwrap()
    }

    /// Produces the response for a call to the `/status` endpoint.
    fn status(metrics: &metrics::Collection) -> Response<Body> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .body(metrics.assemble(metrics::OutputFormat::Plain).into())
            .unwrap()
    }

    #[cfg(not(feature = "http-api-gzip"))]
    async fn encode_response(
        _req: Request<Body>,
        res: Response<Body>,
        _compress_responses: bool,
    ) -> Response<Body> {
        res
    }

    #[cfg(feature = "http-api-gzip")]
    async fn encode_response(
        req: Request<Body>,
        res: Response<Body>,
        compress_responses: bool,
    ) -> Response<Body> {
        if compress_responses {
            // Streaming the response through the encoder as it's being built
            // would be more efficient, and maybe also only creating the encoder
            // once rather than every time, but this works for now.
            let accepts_gzip = req
                .headers()
                .get("Accept-Encoding")
                .map(|v| v.to_str().unwrap().contains("gzip"))
                .unwrap_or_default();

            if accepts_gzip {
                use flate2::{write::GzEncoder, Compression};
                use hyper::header::HeaderValue;
                use std::io::Write;

                let (mut parts, body) = res.into_parts();
                let mut compressor = GzEncoder::new(Vec::new(), Compression::default());
                compressor
                    .write_all(&hyper::body::to_bytes(body).await.unwrap())
                    .unwrap();
                let body = compressor.finish().unwrap().into();
                parts
                    .headers
                    .append("Content-Encoding", HeaderValue::from_static("gzip"));
                return Response::from_parts(parts, body);
            }
        }

        res
    }

    /// Produces the response for a Method Not Allowed error.
    fn method_not_allowed() -> Response<Body> {
        Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("Content-Type", "text/plain")
            .body("Method Not Allowed".into())
            .unwrap()
    }

    /// Produces the response for a Not Found error.
    fn not_found() -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "text/plain")
            .body("Not Found".into())
            .unwrap()
    }
}

//------------ Resources -----------------------------------------------------

/// A collection of HTTP resources to be served by the server.
///
/// This type provides a shared collection. I.e., if a value is cloned, both
/// clones will reference the same collection. Both will see newly
/// added resources.
///
/// Such new resources can be registered with the [`register`][Self::register]
/// method. An HTTP request can be processed using the
/// [`process_request`][Self::process_request] method.
#[derive(Clone, Default)]
pub struct Resources {
    /// The currently registered sources.
    sources: Arc<ArcSwap<Vec<RegisteredResource>>>,

    /// A mutex to be held during registration of a new source.
    ///
    /// Updating `sources` is done by taking the existing sources,
    /// construct a new vec, and then swapping that vec into the arc. Because
    /// of this, updates cannot be done concurrently. The mutex guarantees
    /// that.
    register: Arc<Mutex<()>>,

    /// Whether or not to support GZIP response compression
    compress_responses: bool,
}

impl Resources {
    /// Registers a new processor with the collection.
    ///
    /// The processor is given as a weak pointer so that it gets dropped
    /// when the owning component terminates.
    pub fn register(
        &self,
        process: Weak<dyn ProcessRequest>,
        component_type: &'static str,
        rel_base_url: &str,
        is_sub_resource: bool,
    ) {
        let lock = self.register.lock().unwrap();
        let old_sources = self.sources.load();
        let mut new_sources = Vec::new();
        for item in old_sources.iter() {
            if item.processor.strong_count() > 0 {
                new_sources.push(item.clone())
            }
        }

        let new_source = RegisteredResource {
            processor: process,
            component_type,
            rel_base_url: Arc::new(rel_base_url.to_string()),
            is_sub_resource,
        };
        if is_sub_resource {
            new_sources.insert(0, new_source);
        } else {
            new_sources.push(new_source);
        }

        self.sources.store(new_sources.into());
        drop(lock);
    }

    /// Processes an HTTP request.
    ///
    /// Returns some response if any of the registered processors actually
    /// processed the particular request or `None` otherwise.
    pub async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>> {
        let sources = self.sources.load();
        for item in sources.iter() {
            if let Some(process) = item.processor.upgrade() {
                if let Some(response) = process.process_request(request).await {
                    return Some(response);
                }
            }
        }
        None
    }

    pub fn rel_base_urls<'a>(&'a self) -> SmallVec<[Arc<String>; 8]> {
        self.sources
            .load()
            .iter()
            .filter(|item| !item.is_sub_resource)
            .map(|item| item.rel_base_url.clone())
            .collect()
    }

    pub fn rel_base_urls_for_component_type(
        &self,
        component_type: &'static str,
    ) -> SmallVec<[Arc<String>; 8]> {
        let mut rel_base_urls = smallvec![];
        for item in self.sources.load().iter() {
            if !item.is_sub_resource && item.component_type == component_type {
                rel_base_urls.push(item.rel_base_url.clone());
            }
        }
        rel_base_urls
    }
}

impl fmt::Debug for Resources {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let len = self.sources.load().len();
        write!(f, "Resource({} processors)", len)
    }
}

//------------ RegisteredResource --------------------------------------------

/// All information on a resource registered with a collection.
#[derive(Clone)]
struct RegisteredResource {
    /// A weak pointer to the resource’s processor.
    processor: Weak<dyn ProcessRequest>,

    /// The type of unit that registered the processor.
    component_type: &'static str,

    /// The base URL or main entrypoint of the API offered by the processor.
    rel_base_url: Arc<String>,

    /// Does this processor handle URL space below another processor?
    is_sub_resource: bool,
}

//------------ ProcessRequest ------------------------------------------------

/// A type that can process an HTTP request.
#[async_trait]
pub trait ProcessRequest: Send + Sync {
    /// Processes an HTTP request.
    ///
    /// If the processor feels responsible for the reuqest, it should return
    /// some response. This can be an error response. Otherwise it should
    /// return `None`.
    async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>>;
}

#[async_trait]
impl<T: ProcessRequest> ProcessRequest for Arc<T> {
    async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>> {
        AsRef::<T>::as_ref(self).process_request(request).await
    }
}

#[async_trait]
impl<F> ProcessRequest for F
where
    F: Fn(&Request<Body>) -> Option<Response<Body>> + Sync + Send,
{
    async fn process_request(&self, request: &Request<Body>) -> Option<Response<Body>> {
        (self)(request)
    }
}

//------------ PercentDecodedPath --------------------------------------------

pub trait PercentDecodedPath {
    fn decoded_path(&self) -> Cow<str>;
}

impl PercentDecodedPath for Uri {
    fn decoded_path(&self) -> Cow<str> {
        percent_decode(self.path().as_bytes()).decode_utf8_lossy()
    }
}

//------------ Wrapped sockets -----------------------------------------------

/// A TCP listener wrapped for use with Hyper.
struct HttpAccept {
    sock: TcpListener,
}

impl Accept for HttpAccept {
    type Conn = HttpStream;
    type Error = io::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let sock = &mut self.sock;
        pin_mut!(sock);
        match sock.poll_accept(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok((sock, _addr))) => Poll::Ready(Some(Ok(HttpStream { sock }))),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
        }
    }
}

/// A TCP stream wrapped for use with Hyper.
struct HttpStream {
    sock: TcpStream,
}

impl HttpStream {
    fn sock(&self) -> &TcpStream {
        &self.sock
    }
}

impl AsyncRead for HttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let sock = &mut self.sock;
        pin_mut!(sock);
        sock.poll_read(cx, buf)
    }
}

impl AsyncWrite for HttpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let sock = &mut self.sock;
        pin_mut!(sock);
        sock.poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let sock = &mut self.sock;
        pin_mut!(sock);
        sock.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let sock = &mut self.sock;
        pin_mut!(sock);
        sock.poll_shutdown(cx)
    }
}

// --- Query parameter handling ---------------------------------------------------------------------------------------

//pub type QueryParam = (String, String, AtomicBool);

pub struct QueryParam {
    name: String,
    value: String,
    used: AtomicBool,
}

impl QueryParam {
    pub fn new(name: String, value: String) -> Self {
        Self {
            name,
            value,
            used: AtomicBool::default(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub fn mark_used(&self) {
        self.used.store(true, Ordering::SeqCst);
    }

    pub fn used(&self) -> bool {
        self.used.load(Ordering::SeqCst)
    }
}

pub type QueryParams = [QueryParam];

pub fn extract_params(request: &Request<Body>) -> Vec<QueryParam> {
    request
        .uri()
        .query()
        .map(|v| {
            parse(v.as_bytes())
                .into_owned()
                .map(|(n, v)| QueryParam::new(n, v))
                .collect()
        })
        .unwrap_or_else(Vec::new)
}

pub fn get_param<'a>(params: &'a QueryParams, needle: &str) -> Option<MatchedParam<'a>> {
    params.iter().find_map(|query_param| {
        MatchedParam::parse(query_param, needle).map(|matched_param| {
            query_param.mark_used();
            matched_param
        })
    })
}

pub fn get_all_params<'a>(params: &'a QueryParams, needle: &str) -> Vec<MatchedParam<'a>> {
    params
        .iter()
        .filter_map(|query_param| {
            MatchedParam::parse(query_param, needle).map(|matched_param| {
                query_param.mark_used();
                matched_param
            })
        })
        .collect()
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum MatchedParam<'a> {
    Exact(&'a str),           // value
    Family(&'a str, &'a str), // family, value
}

impl<'a> MatchedParam<'a> {
    fn parse(param: &'a QueryParam, needle: &str) -> Option<MatchedParam<'a>> {
        let mut iter = param.name().split(&['[', ']']);
        match (iter.next(), iter.next()) {
            (Some(k), None) if k == needle => Some(MatchedParam::Exact(param.value())),
            (Some(k), Some(family)) if k == needle => {
                Some(MatchedParam::Family(family, param.value()))
            }
            _ => None,
        }
    }

    pub fn value(&self) -> &str {
        match self {
            MatchedParam::Exact(v) => v,
            MatchedParam::Family(_, v) => v,
        }
    }

    pub fn family(&self) -> Option<&str> {
        match self {
            MatchedParam::Exact(_) => None,
            MatchedParam::Family(family, _) => Some(family),
        }
    }
}

impl<'a> Display for MatchedParam<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchedParam::Exact(value) => f.write_str(value),
            MatchedParam::Family(family, value) => write!(f, "{}[{}]", family, value),
        }
    }
}
