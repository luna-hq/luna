use std::{
    io::Result,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, runtime::Handle};

pub struct IpcWriter<'a> {
    pub write_half: Arc<Mutex<OwnedWriteHalf>>,
    pub handle: &'a Handle,
}

impl<'a> std::io::Write for IpcWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.handle.block_on(async {
            self.write_half.lock().unwrap().write_all(buf).await?;
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.handle.block_on(async {
            self.write_half.lock().unwrap().flush().await?;
            Ok(())
        })
    }
}
