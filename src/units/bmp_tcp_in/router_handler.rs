//! BMP message stream handler for a single connected BMP publishing client.
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::{io::AsyncRead, net::TcpStream};

use crate::{
    comms::{Gate, GateStatus},
    payload::{Payload, Update},
    units::{
        bmp_tcp_in::{io::BmpStream, status_reporter::BmpTcpInStatusReporter},
        Unit,
    },
};

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

    // Ensure that on first use the metrics for the "unknown" router are
    // correctly initialised.
    status_reporter.router_id_changed(router_addr);

    loop {
        // Read the incoming TCP stream, extracting BMP messages.
        match stream.next().await {
            Err(err) => {
                // There was a problem reading from the BMP stream.
                status_reporter.receive_io_error(router_addr, &err);

                if matches!(err.kind(), std::io::ErrorKind::UnexpectedEof) {
                    status_reporter.router_connection_lost(router_addr);

                    // Notify downstream units that the data stream for this
                    // particular monitored router has ended.
                    let payload = Payload::bmp_eof(router_addr);
                    gate_worker.update_data(Update::Single(payload)).await;
                }

                // TODO: are there kinds of error worth ignoring, e.g. some
                // sort of network timeout?

                // Break to close our side of the connection and stop
                // processing this BMP stream.
                break;
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

                let payload = Payload::bmp_msg(router_addr, msg_buf);
                gate_worker.update_data(Update::Single(payload)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{io::ReadBuf, time::timeout};

    use crate::units::bmp_tcp_in::metrics::BmpTcpInMetrics;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn terminate_on_loss_of_parent_gate() {
        let (gate, _agent) = Gate::new(1);
        let router_addr = "127.0.0.1:8080".parse().unwrap();
        let metrics = Arc::new(BmpTcpInMetrics::default());
        let status_reporter = Arc::new(BmpTcpInStatusReporter::new("mock reporter", metrics));

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
        let join_handle = read_from_router(gate.clone(), rx, router_addr, status_reporter);

        // Without this the reader continues forever
        eprintln!("DROPPING PARENT GATE");
        drop(gate);

        eprintln!("WAITING FOR ROUTER READER TO EXIT");
        timeout(Duration::from_secs(5), join_handle).await.unwrap();

        eprintln!("DONE");
    }
}
