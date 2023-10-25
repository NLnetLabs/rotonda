//! BMP message stream handler for a single connected BMP publishing client.
use std::net::SocketAddr;
use std::sync::Arc;

use roto::types::{
    builtin::BuiltinTypeValue, collections::BytesRecord,
    lazyrecord_types::BmpMessage, typevalue::TypeValue,
};
use tokio::{io::AsyncRead, net::TcpStream};

use crate::{
    comms::{Gate, GateStatus},
    payload::{Payload, Update, UpstreamStatus},
    units::{
        bmp_tcp_in::{
            io::BmpStream, status_reporter::BmpTcpInStatusReporter,
        },
        Unit,
    },
};

use super::io::FatalError;

#[allow(clippy::too_many_arguments)]
pub async fn handle_router(
    gate: Gate,
    mut tcp_stream: TcpStream,
    router_addr: SocketAddr,
    status_reporter: Arc<BmpTcpInStatusReporter>,
) {
    // Discard the write half of the TCP stream as we are a "monitoring
    // station" and per the BMP RFC 7584 specification _"No BMP message is
    // ever sent from the monitoring station to the monitored router"_.
    // See: https://datatracker.ietf.org/doc/html/rfc7854#section-3.2
    let (rx, _tx) = tcp_stream.split();

    read_from_router(gate, rx, router_addr, status_reporter).await;
}

async fn read_from_router<T: AsyncRead + Unpin>(
    gate: Gate,
    rx: T,
    router_addr: SocketAddr,
    status_reporter: Arc<BmpTcpInStatusReporter>,
) {
    // Setup BMP streaming
    let gate_worker = gate.clone();
    let mut stream = BmpStream::new(rx, gate);

    loop {
        // Read the incoming TCP stream, extracting BMP messages.
        match stream.next().await {
            Err(err) => {
                // There was a problem reading from the BMP stream.
                status_reporter.receive_io_error(router_addr, &err);

                if err.is_fatal() {
                    // Break to close our side of the connection and stop
                    // processing this BMP stream.
                    break;
                }
            }

            Ok((None, _)) => {
                // The stream consumer exited in response to a Gate
                // termination message. Break to close our side of the
                // connection and stop processing this BMP stream.
                break;
            }

            Ok((Some(msg_buf), status)) => {
                // We want the stream reading to abort as soon as the Gate
                // is terminated so we handle status updates to the Gate in
                // the stream reader. The last non-terminal status updates is
                // then passed to us here along with the next message that was
                // read.
                //
                // TODO: Should we require that we receive all non-terminal
                // gate status updates here so that none are missed?

                status_reporter.bmp_message_received(router_addr);

                // Update our behaviour to follow setting changes, if any.
                // Note that this is delayed until a BMP message was received,
                // but as we only read BMP messages it shouldn't matter that
                // we can't react to setting changes while waiting for the
                // next message but can then handle it at that point.
                match status {
                    Some(GateStatus::Reconfiguring {
                        new_config: Unit::BmpTcpIn(_unit),
                    }) => {
                        // We don't have any settings to reconfigure.
                    }

                    Some(GateStatus::ReportLinks { report }) => {
                        report.declare_source();
                    }

                    _ => { /* Nothing to do */ }
                }

                if let Ok(bmp_msg) = BmpMessage::from_octets(msg_buf) {
                    let bmp_msg = Arc::new(BytesRecord(bmp_msg));
                    let value = TypeValue::Builtin(
                        BuiltinTypeValue::BmpMessage(bmp_msg),
                    );
                    let payload = Payload::new(router_addr, value);
                    gate_worker.update_data(payload.into()).await;
                }
            }
        }
    }

    status_reporter.router_connection_lost(router_addr);

    // Notify downstream units that the data stream for this
    // particular monitored router has ended.
    let new_status = UpstreamStatus::EndOfStream {
        source_id: router_addr.into(),
    };
    gate_worker
        .update_data(Update::UpstreamStatusChange(new_status))
        .await;
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{io::ReadBuf, time::timeout};

    use crate::{
        common::status_reporter::AnyStatusReporter,
        tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        },
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn terminate_on_loss_of_parent_gate() {
        let (gate, _agent) = Gate::new(1);
        let router_addr = "127.0.0.1:8080".parse().unwrap();
        let status_reporter = Arc::new(BmpTcpInStatusReporter::default());

        struct MockRouterStream;

        impl AsyncRead for MockRouterStream {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<tokio::io::Result<()>> {
                Poll::Pending
            }
        }

        let rx = MockRouterStream;

        eprintln!("STARTING ROUTER READER");
        let fut =
            read_from_router(gate.clone(), rx, router_addr, status_reporter);

        // Without this the reader continues forever
        eprintln!("DROPPING PARENT GATE");
        drop(gate);

        eprintln!("WAITING FOR ROUTER READER TO EXIT");
        timeout(Duration::from_secs(5), fut).await.unwrap();

        eprintln!("DONE");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_count_io_errors() {
        enable_logging("trace");
        let (gate, _agent) = Gate::new(1);
        let router_addr = "127.0.0.1:8080".parse().unwrap();
        let status_reporter = Arc::new(BmpTcpInStatusReporter::default());

        struct MockRouterStream {
            interrupted_already: bool,
            router_addr: SocketAddr,
            status_reporter: Arc<BmpTcpInStatusReporter>,
        }

        impl AsyncRead for MockRouterStream {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<tokio::io::Result<()>> {
                // Fail with a non-fatal error so that reading from the router
                // continues giving us a chance to check the router specific
                // metrics rather than  returning a fatal error which would
                // cause the simulated router to be disconnected and its
                // associated metrics to be removed.
                if !self.interrupted_already {
                    self.get_mut().interrupted_already = true;
                    Poll::Ready(Err(std::io::ErrorKind::Interrupted.into()))
                } else {
                    let metrics = get_testable_metrics_snapshot(
                        &self.status_reporter.metrics().unwrap(),
                    );
                    let router_addr = self.router_addr.to_string();
                    let label = ("router", router_addr.as_str());
                    assert_eq!(
                        metrics.with_label::<usize>(
                            "bmp_tcp_in_num_bmp_messages_received",
                            label
                        ),
                        0
                    );
                    assert_eq!(
                        metrics.with_label::<usize>(
                            "bmp_tcp_in_num_receive_io_errors",
                            label
                        ),
                        1
                    );

                    // Fail with a fatal error to stop the reader polling for
                    // more data.
                    Poll::Ready(Err(std::io::ErrorKind::Other.into()))
                }
            }
        }

        let rx = MockRouterStream {
            interrupted_already: false,
            router_addr,
            status_reporter: status_reporter.clone(),
        };

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            0
        );

        read_from_router(
            gate.clone(),
            rx,
            router_addr,
            status_reporter.clone(),
        )
        .await;

        let metrics = get_testable_metrics_snapshot(
            &status_reporter.metrics().unwrap(),
        );
        assert_eq!(
            metrics.with_name::<usize>("bmp_tcp_in_connection_lost_count"),
            1
        );
    }
}
