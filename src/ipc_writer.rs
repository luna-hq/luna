use std::io::Result;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, runtime::Handle};

pub struct IpcWriter<'a> {
    pub stream: OwnedWriteHalf,
    pub handle: &'a Handle,
}

impl<'a> std::io::Write for IpcWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.handle.block_on(async {
            self.stream.write_all(buf).await?;
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.handle.block_on(async {
            self.stream.flush().await?;
            Ok(())
        })
    }
}
