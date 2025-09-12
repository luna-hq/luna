use std::{
    io::Result,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncWriteExt, net::TcpStream, runtime::Handle};

pub struct IpcWriter<'a> {
    pub stream: Arc<Mutex<TcpStream>>,
    pub handle: &'a Handle,
}

impl<'a> std::io::Write for IpcWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.handle.block_on(async {
            self.stream.lock().unwrap().write_all(buf).await?;
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.handle.block_on(async {
            self.stream.lock().unwrap().flush().await?;
            Ok(())
        })
    }
}
