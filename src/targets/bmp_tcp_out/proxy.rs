use atomic_enum::atomic_enum;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tokio::{io::AsyncWriteExt, sync::mpsc::Sender};
use tokio::{net::TcpStream, time::timeout};

use super::status_reporter::BmpProxyStatusReporter;

#[atomic_enum]
#[derive(Default)]
pub enum ProxyState {
    #[default]
    None,
    Flowing,
    Stalled,
    Recovered,
}

impl Default for AtomicProxyState {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl std::fmt::Display for ProxyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyState::None => f.write_str("None"),
            ProxyState::Flowing => f.write_str("Flowing"),
            ProxyState::Stalled => f.write_str("Stalled"),
            ProxyState::Recovered => f.write_str("Recovered"),
        }
    }
}

impl ProxyState {
    pub fn as_handler_state(&self) -> u8 {
        match self {
            ProxyState::None => 0,
            ProxyState::Flowing => 1,
            ProxyState::Stalled => 2,
            ProxyState::Recovered => 3,
        }
    }
}

#[derive(Debug)]
pub struct BmpProxy {
    is_connected: bool,
    is_err: bool,
    router_addr: SocketAddr,
    status_reporter: Arc<BmpProxyStatusReporter>,
    tx: Option<Arc<Sender<Vec<u8>>>>,
}

impl BmpProxy {
    pub fn new(
        proxy_uri: Option<Arc<String>>,
        status_reporter: Arc<BmpProxyStatusReporter>,
        router_addr: SocketAddr,
    ) -> Self {
        let tx = if let Some(proxy_uri) = &proxy_uri {
            let (tx, rx) = tokio::sync::mpsc::channel(1000);
            let task_name = format!("proxy[{}]", proxy_uri);

            status_reporter.init_per_proxy_metrics(router_addr, tx.capacity());

            crate::tokio::spawn(
                &task_name,
                Self::do_proxy(router_addr, proxy_uri.clone(), rx, status_reporter.clone()),
            );

            Some(Arc::new(tx))
        } else {
            None
        };

        Self {
            is_connected: false,
            is_err: false,
            router_addr,
            status_reporter,
            tx,
        }
    }

    pub async fn terminate(&mut self) {
        if self.tx.is_some() {
            self.status_reporter.proxy_terminate(self.router_addr);
        }
        let _ = self.tx.take();
    }

    pub async fn send(&mut self, bytes: &[u8]) {
        if let Some(tx) = &self.tx {
            match tx.try_send(bytes.to_vec()) {
                Ok(_) => {
                    if !self.is_connected {
                        self.status_reporter.proxy_queue_started(self.router_addr);
                        self.is_connected = true;
                    } else if self.is_err {
                        self.status_reporter.proxy_queue_ok(self.router_addr);
                        self.is_err = false;
                    }
                }

                Err(err) => {
                    if !self.is_err {
                        self.status_reporter
                            .proxy_queue_error(self.router_addr, err);
                        self.is_err = true;
                    }
                    self.status_reporter
                        .proxy_message_undeliverable(self.router_addr);
                }
            }

            self.status_reporter
                .proxy_queue_capacity(self.router_addr, tx.capacity());
        }
    }

    async fn do_proxy(
        router_addr: SocketAddr,
        proxy_uri: Arc<String>,
        mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
        status_reporter: Arc<BmpProxyStatusReporter>,
    ) {
        status_reporter.proxy_handler_started();

        'outer: loop {
            status_reporter.proxy_handler_connecting();

            if let Ok(Ok(mut stream)) =
                timeout(Duration::from_millis(100), TcpStream::connect(&*proxy_uri)).await
            {
                status_reporter.proxy_handler_connected();

                'inner: loop {
                    match rx.recv().await {
                        None => {
                            // None means the sender has been dropped.
                            status_reporter.proxy_handler_sender_gone();
                            break 'outer;
                        }

                        Some(bytes) => {
                            if let Err(err) = stream.write_all(&bytes).await {
                                status_reporter.proxy_handler_io_error(err);
                                break 'inner;
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        status_reporter.proxy_handler_terminated(router_addr);
    }
}
