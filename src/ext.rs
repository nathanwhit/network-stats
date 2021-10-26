use std::time::Duration;

use color_eyre::Result;
use futures::Future;
use prost::bytes::Buf;
use prost::Message;

pub trait MessageExt<M> {
    fn try_parse<B: AsRef<[u8]>>(b: B) -> color_eyre::Result<M>;

    fn to_bytes(&self) -> Vec<u8>;
}

pub trait BufExt {
    fn parse_into<M: Message + Default>(self) -> Result<M>;
}

impl<B> BufExt for B
where
    B: Buf,
{
    fn parse_into<M: Message + Default>(self) -> Result<M> {
        M::decode(self).map_err(Into::into)
    }
}

pub trait ResultExt: Sized {
    type Output;
    fn log_err(self) -> Self::Output;
}

impl<T, E> ResultExt for Result<T, E>
where
    E: std::fmt::Display,
{
    type Output = ();

    fn log_err(self) {
        match self {
            Ok(_) => {}
            Err(err) => log::error!("Error occurred: {}", err),
        }
    }
}

pub trait FutureExt: Sized {
    fn timeout(self, duration: Duration) -> tokio::time::Timeout<Self>;
}

impl<F> FutureExt for F
where
    F: Future,
{
    fn timeout(self, duration: Duration) -> tokio::time::Timeout<Self> {
        tokio::time::timeout(duration, self)
    }
}

impl<M: Message + Default> MessageExt<M> for M {
    fn try_parse<B: AsRef<[u8]>>(buf: B) -> color_eyre::Result<M> {
        M::decode(buf.as_ref()).map_err(Into::into)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buf).unwrap();
        buf
    }
}
