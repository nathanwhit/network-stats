use color_eyre::Result;
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
