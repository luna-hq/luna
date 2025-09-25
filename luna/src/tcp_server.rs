use crate::WorkCmd;
use anyhow::Result;
use async_channel::Sender as AsyncSender;
use bcrypt::verify;
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
pub const EMPTY: &str = "EMPTY";
pub const AUTH: &str = "AUTH";
pub const OK: &str = "OK";

pub struct TcpServer {
    rt: Arc<Runtime>,
    host_port: String,
    tx_work: Arc<AsyncSender<WorkCmd>>,
    pass_hash: Arc<String>,
}

impl TcpServer {
    pub fn new(
        rt: Arc<Runtime>,
        host_port: String,
        tx_work: Arc<AsyncSender<WorkCmd>>,
        pass_hash: Arc<String>,
    ) -> Self {
        TcpServer {
            rt,
            host_port,
            tx_work,
            pass_hash,
        }
    }

    pub fn run(&self) {
        let rt = self.rt.clone();
        let host_port = self.host_port.clone();
        let tx_work = self.tx_work.clone();
        let pass_hash = self.pass_hash.clone();
        thread::spawn(move || {
            rt.block_on(async {
                let listen = TcpListener::bind(&host_port).await.unwrap();
                info!("listening from {}", &host_port);

                loop {
                    let (stream, addr) = listen.accept().await.unwrap();
                    info!(">>> connection opened ({})", addr);

                    let s_addr = addr.to_string();
                    let tx_work = tx_work.clone();
                    let pass_hash = pass_hash.clone();
                    let (read_half, write_half) = stream.into_split();

                    rt.spawn(async move {
                        let _ = handle_stream(read_half, Arc::new(Mutex::new(write_half)), tx_work, s_addr, pass_hash)
                            .await;
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
    pass_hash: Arc<String>,
) -> Result<()> {
    let mut pass_ok: usize = 0;
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

        let (tx_wait, rx_wait) = async_channel::unbounded::<usize>();

        // "AUTH <password>\r\n"
        if *pass_hash != "" && pass_ok == 0 {
            info!("checking password...");
            let auth = &accum[0..(accum.len() - DELIM.len())];
            let s_auth = String::from_utf8_lossy(auth);
            let split = s_auth.split_ascii_whitespace().collect::<Vec<&str>>();
            if split.len() >= 2 {
                let valid = verify(split[1], &pass_hash)?;
                if split[0] == AUTH && valid {
                    pass_ok = 1;
                    tx_work
                        .send(WorkCmd::ProtoWriteError {
                            write_half: write_half.clone(),
                            input: AUTH.to_string(),
                            error: OK.to_string(),
                            tx_wait,
                        })
                        .await?;

                    rx_wait.recv().await?;
                    info!("{} {}", AUTH, OK);
                    continue;
                }
            }

            tx_work
                .send(WorkCmd::ProtoWriteError {
                    write_half: write_half.clone(),
                    input: AUTH.to_string(),
                    error: "DENIED".to_string(),
                    tx_wait,
                })
                .await?;

            rx_wait.recv().await?;
            error!("auth failed, closing connection ({})", s_addr);
            break;
        }

        let mut xfmt = 0;
        if len == 0 {
            xfmt += 1;
        }

        if (len + offset) >= accum.len() {
            xfmt += 1;
        }

        // For valid commands, len should be > 0 (other than AUTH).
        if xfmt > 0 {
            let input = &accum[0..(accum.len() - DELIM.len())];
            let s_input = String::from_utf8_lossy(input);
            tx_work
                .send(WorkCmd::ProtoWriteError {
                    write_half: write_half.clone(),
                    input: s_input.to_string(),
                    error: "INVALID INPUT".to_string(),
                    tx_wait,
                })
                .await?;

            rx_wait.recv().await?;
            continue;
        }

        let line = &accum[(offset + 2)..(len + offset)];
        let s_line = String::from_utf8_lossy(line);
        info!("payload: {:?}", s_line);

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
                    tx_work
                        .send(WorkCmd::ProtoWriteError {
                            write_half: write_half.clone(),
                            input: s_line.to_string(),
                            error: err,
                            tx_wait,
                        })
                        .await?;

                    rx_wait.recv().await?;
                    continue;
                }
            },
            _ => {
                let cmd = String::from(accum[0] as char);
                let mut err = String::new();
                write!(&mut err, "Unknown command [{cmd}]")?;
                tx_work
                    .send(WorkCmd::ProtoWriteError {
                        write_half: write_half.clone(),
                        input: s_line.to_string(),
                        error: err,
                        tx_wait,
                    })
                    .await?;

                rx_wait.recv().await?;
                continue;
            }
        }
    }

    Ok(())
}
