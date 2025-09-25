use crate::WorkCmd;
use anyhow::Result;
use async_channel::Sender as AsyncSender;
use log::*;
use memchr::memmem;
use std::{
    fmt::Write as _,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};
use tokio::{
    io::AsyncReadExt,
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    runtime::Runtime,
};

pub const DELIM: &str = "\r\n";
pub const OK: &str = "OK";
pub const EMPTY: &str = "EMPTY";

pub struct TcpServer {
    rt: Arc<Runtime>,
    host_port: String,
    tx_work: Arc<AsyncSender<WorkCmd>>,
}

impl TcpServer {
    pub fn new(rt: Arc<Runtime>, host_port: String, tx_work: Arc<AsyncSender<WorkCmd>>) -> Self {
        TcpServer { rt, host_port, tx_work }
    }

    pub fn run(&self) {
        let rt = self.rt.clone();
        let host_port = self.host_port.clone();
        let tx_work = self.tx_work.clone();
        thread::spawn(move || {
            rt.block_on(async {
                let listen = TcpListener::bind(&host_port).await.unwrap();
                info!("listening from {}", &host_port);

                loop {
                    let (stream, addr) = listen.accept().await.unwrap();
                    info!(">>> connection opened ({})", addr);

                    let s_addr = addr.to_string();
                    let tx_work = tx_work.clone();
                    let (read_half, write_half) = stream.into_split();

                    rt.spawn(async move {
                        let _ = handle_stream(read_half, Arc::new(Mutex::new(write_half)), tx_work, s_addr).await;
                    });
                }
            });
        });
    }
}

async fn handle_stream(
    mut read_half: OwnedReadHalf,
    write_half: Arc<Mutex<OwnedWriteHalf>>,
    tx_work: Arc<AsyncSender<WorkCmd>>,
    s_addr: String,
) -> Result<()> {
    loop {
        let start = Instant::now();
        defer!(info!("handle_stream/iter took {:?}", start.elapsed()));

        let mut offset = 0;
        let mut len = 0;
        let mut accum = Vec::new();
        let mut buf = vec![0; 1024];
        loop {
            match read_half.read(&mut buf).await {
                Err(_) => break,
                Ok(n) => {
                    if n == 0 {
                        break;
                    }

                    let data = &buf[0..n];
                    accum.extend_from_slice(data);

                    let delim = memmem::find(&data, DELIM.as_bytes());
                    if delim.is_some() && len < 1 {
                        offset = delim.unwrap() + 2;
                        let slen = &accum[1..delim.unwrap()];
                        len = match str::from_utf8(slen) {
                            Err(_) => 0,
                            Ok(v) => v.parse::<usize>().unwrap_or(0),
                        };
                    }

                    if ((len + offset) > 0) && accum.len() >= (len + offset) {
                        break; // got all data
                    }

                    if n >= 2 && buf[n - 2] == DELIM.as_bytes()[0] && buf[n - 1] == DELIM.as_bytes()[1] {
                        break; // end-of-stream
                    }
                }
            }
        }

        if offset == 0 && len == 0 && accum.len() == 0 {
            info!("<<< connection closed ({})", s_addr);
            break;
        }

        let line = &accum[(offset + 2)..(len + offset)];
        let s_line = String::from_utf8_lossy(line);
        info!("payload={}", s_line);

        let (tx_wait, rx_wait) = async_channel::unbounded::<usize>();
        let mut errs = vec![];

        match &accum[0] {
            b'$' => match &accum[offset..(offset + 2)] {
                b"x:" => {
                    tx_work
                        .send(WorkCmd::ProtoDuckExecute {
                            write_half: write_half.clone(),
                            query: s_line.to_string(),
                            tx_wait,
                        })
                        .await?;

                    rx_wait.recv().await?;
                    continue;
                }
                b"q:" => {
                    tx_work
                        .send(WorkCmd::ProtoDuckQuery {
                            write_half: write_half.clone(),
                            query: s_line.to_string(),
                            tx_wait,
                        })
                        .await?;

                    rx_wait.recv().await?;
                    continue;
                }
                _ => {
                    let pfx = String::from_utf8_lossy(&accum[offset..(offset + 2)]);
                    let mut err = String::new();
                    write!(&mut err, "Unknown prefix [{pfx}]")?;
                    errs.push(s_line.to_string());
                    errs.push(err);
                }
            },
            _ => {
                let cmd = String::from(accum[0] as char);
                let mut err = String::new();
                write!(&mut err, "Unknown command [{cmd}]")?;
                errs.push(s_line.to_string());
                errs.push(err);
            }
        }

        if errs.len() == 2 {
            tx_work
                .send(WorkCmd::ProtoWriteError {
                    write_half: write_half.clone(),
                    input: errs[0].clone(),
                    error: errs[1].clone(),
                    tx_wait,
                })
                .await?;

            rx_wait.recv().await?;
            continue;
        }
    }

    Ok(())
}
