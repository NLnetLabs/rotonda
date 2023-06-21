use bytes::{Bytes, BytesMut};
use futures::{
    future::{select, Either},
    pin_mut,
};
use routecore::bmp::message::Message as BmpMsg;
use std::{convert::TryInto, io::ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::comms::{Gate, GateStatus, Terminated};

async fn bmp_read<T: AsyncRead + Unpin>(mut rx: T) -> Result<(T, Bytes), (T, std::io::Error)> {
    let mut msg_buf = BytesMut::new();
    msg_buf.resize(5, 0u8);
    if let Err(err) = rx.read_exact(&mut msg_buf).await {
        return Err((rx, err));
    }

    // Don't call BmpMsg::check() as it requires the rest of the message to have already been read
    let _version = &msg_buf[0];
    let len = u32::from_be_bytes(msg_buf[1..5].try_into().unwrap()) as usize;
    msg_buf.resize(len, 0u8);
    if let Err(err) = rx.read_exact(&mut msg_buf[5..]).await {
        return Err((rx, err));
    }

    let msg_buf = msg_buf.freeze();

    match BmpMsg::from_octets(&msg_buf) {
        Ok(_) => Ok((rx, msg_buf)),
        Err(err) => Err((rx, std::io::Error::new(ErrorKind::Other, err.to_string()))),
    }
}

pub struct BmpStream<T: AsyncRead> {
    rx: Option<T>,
    gate: Gate,
}

impl<T: AsyncRead + Unpin> BmpStream<T> {
    pub fn new(rx: T, gate: Gate) -> Self {
        Self { rx: Some(rx), gate }
    }

    /// Retrieve the next BMP message from the stream.
    ///
    /// # Errors
    ///
    /// Returns an [std::io::Error] of the same kind as [AsyncReadExt::read_exact],
    /// i.e. [ErrorKind::UnexpectedEof] if "end of file" is encountered, or
    /// any other read error.
    ///
    /// Additionally it can also return [ErrorKind::Other] if received bytes
    /// are rejected by the BMP parser.
    ///
    /// # Cancel safety
    ///
    /// This function is NOT cancel safe. If cancelled the stream receiver
    /// will be lost and no further updates can be read from the stream.
    pub async fn next(&mut self) -> Result<(Option<Bytes>, Option<GateStatus>), std::io::Error> {
        let mut saved_gate_status = None;

        if let Some(rx) = self.rx.take() {
            let mut update_fut = Box::pin(bmp_read(rx));
            loop {
                let process = self.gate.process();
                pin_mut!(process);

                match select(process, update_fut).await {
                    Either::Left((Err(Terminated), _)) => {
                        // Unit termination signal received
                        // The unit will report this so no need to report it here.
                        return Ok((None, None));
                    }
                    Either::Left((Ok(status), next_fut)) => {
                        // Unit status update received, save it to return with the
                        // next received message so that this router handler can
                        // reconfigure itself if needed.
                        saved_gate_status = Some(status);

                        // The unit will report the status change so no need to
                        // also report it here.

                        update_fut = next_fut;
                    }
                    Either::Right((Err((_rx, err)), _)) => {
                        // Error while receiving data.
                        return Err(err);
                    }
                    Either::Right((Ok((rx, msg)), _)) => {
                        // BMP message received

                        // Save the receiver for the next call to next()
                        self.rx = Some(rx);

                        // Return the message for processing
                        return Ok((Some(msg), saved_gate_status));
                    }
                }
            }
        }

        Err(std::io::Error::new(
            ErrorKind::Other,
            "Internal error: no receiver available",
        ))
    }
}
