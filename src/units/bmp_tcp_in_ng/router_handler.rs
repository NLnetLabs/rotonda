use std::{borrow::Cow, io::Read};

use log::error;
use routecore::bmp::message_ng::io::{
    BmpHandler, BmpV3Handler, BmpVersion, PphRegister,
};
use tokio::io::AsyncRead;

use crate::units::bmp_tcp_in_ng::error::BmpNgError;

pub struct RouterHandler<R> {
    bmp_handler: BmpHandler<R>,
}

impl<R: AsyncRead + Unpin> RouterHandler<R> {
    pub fn new(stream: R) -> Self {
        let bmp_handler = BmpHandler::new(stream, PphRegister::default());
        Self { bmp_handler }
    }

    pub async fn run(mut self) -> Result<(), BmpNgError> {
        let version = self.bmp_handler.process_initiation().await;
        match version {
            BmpVersion(3) => {
                let mut v3handler: BmpV3Handler<_> = self.bmp_handler.into();
                let _ = v3handler.process(|_, _| {}).await;
                Ok(())
            }
            BmpVersion(4) => Err("BMPv4 not yet implemented".into()),
            BmpVersion(v) => Err(format!("invalid BMP version {v}").into()),
        }
    }
}
