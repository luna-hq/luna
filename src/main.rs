mod ipc_writer;
mod tcp_server;
mod utils;

use anyhow::Result;
use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_channel::Receiver as AsyncReceiver;
use clap::Parser;
use ctrlc;
use duckdb::{Connection, arrow::record_batch::RecordBatch as DuckRecordBatch, params};
use hedge_rs::*;
use ipc_writer::IpcWriter;
use log::*;
use memchr::memmem;
use std::{
    collections::HashMap,
    fmt::Write as _,
    str,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::Instant,
};
use tcp_server::TcpServer;
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    runtime::Builder,
    sync::mpsc::{self as tokio_mpsc},
};

#[macro_use(defer)]
extern crate scopeguard;

pub const DELIM: &str = "\r\n";
pub const OK: &str = "+OK\r\n";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Preload CSV files (gs://bucket/prefix*.csv, s3://bucket/prefix*.csv, /local/prefix*.csv)
    #[arg(long, long, default_value = "?")]
    preload_csv: String,

    /// Node ID (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:8080")]
    node_id: String,

    /// Optional, Spanner database (for hedge-rs) (fmt: projects/p/instances/i/databases/db)
    #[arg(long, long, default_value = "?")]
    hedge_db: String,

    /// Optional, Spanner lock table (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    hedge_table: String,

    /// Optional, lock name (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    hedge_lockname: String,

    /// Host:port for the API (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:9090")]
    api_host_port: String,
}

#[derive(Clone, Debug)]
enum WorkerCtrl {
    Exit,
    HandleTcpStream {
        stream: Arc<Mutex<TcpStream>>,
    },
    HandleProto {
        stream: Arc<Mutex<TcpStream>>,
        payload: Vec<u8>,
        offset: usize,
        len: usize,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!(
        "start: api={}, node={}, lock={}/{}/{}",
        &args.api_host_port, &args.node_id, &args.hedge_db, &args.hedge_table, &args.hedge_lockname,
    );

    let base_conn = Connection::open_in_memory()?;
    base_conn.execute("INSTALL httpfs;", params![])?;
    base_conn.execute("LOAD httpfs;", params![])?;

    let (tx_ctrlc, rx_ctrlc) = channel();
    ctrlc::set_handler(move || tx_ctrlc.send(()).unwrap())?;

    let mut op = vec![];
    if args.hedge_db != "?" {
        let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();
        op = vec![Arc::new(Mutex::new(
            OpBuilder::new()
                .id(args.node_id.clone())
                .db(args.hedge_db)
                .table(args.hedge_table)
                .name(args.hedge_lockname)
                .lease_ms(3_000)
                .tx_comms(Some(tx_comms.clone()))
                .build(),
        ))];

        {
            op[0].lock().unwrap().run()?;
        }

        let id_handler = args.node_id.clone();
        thread::spawn(move || -> Result<()> {
            loop {
                match rx_comms.recv() {
                    Err(e) => error!("{e}"),
                    Ok(v) => match v {
                        Comms::ToLeader { msg, tx } => {
                            let msg_s = String::from_utf8(msg)?;
                            info!("[send()] received: {msg_s}");

                            let mut reply = String::new();
                            write!(&mut reply, "echo '{msg_s}' from leader:{}", id_handler.to_string())?;
                            tx.send(reply.as_bytes().to_vec())?;
                        }
                        Comms::Broadcast { msg, tx } => {
                            let msg_s = String::from_utf8(msg)?;
                            info!("[broadcast()] received: {msg_s}");

                            let mut reply = String::new();
                            write!(&mut reply, "echo '{msg_s}' from {}", id_handler.to_string())?;
                            tx.send(reply.as_bytes().to_vec())?;
                        }
                        Comms::OnLeaderChange(state) => {
                            info!("leader state change: {state}");
                        }
                    },
                }
            }
        });
    }

    let rt = Arc::new(Builder::new_multi_thread().enable_all().build()?);

    let (tx_work, rx_work) = async_channel::unbounded::<WorkerCtrl>();
    let rx_works: Arc<Mutex<HashMap<usize, AsyncReceiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
    let cpus = num_cpus::get();

    for i in 0..cpus {
        let rx_works_clone = rx_works.clone();

        {
            let mut rxv = rx_works_clone.lock().unwrap();
            rxv.insert(i, rx_work.clone());
        }
    }

    let mut work_handles = vec![];
    for i in 0..cpus {
        let conn = base_conn.try_clone()?;
        let rt_clone = rt.clone();
        let tx_work_clone = tx_work.clone();
        let rx_works_clone = rx_works.clone();
        work_handles.push(thread::spawn(move || {
            loop {
                let mut rx: Option<AsyncReceiver<WorkerCtrl>> = None;

                {
                    let rxval = match rx_works_clone.lock() {
                        Err(_) => return,
                        Ok(v) => v,
                    };

                    if let Some(v) = rxval.get(&i) {
                        rx = Some(v.clone());
                    }
                }

                let (tx_in, mut rx_in) = tokio_mpsc::unbounded_channel::<WorkerCtrl>();
                rt_clone.block_on(async {
                    tx_in.send(rx.unwrap().recv().await.unwrap()).unwrap();
                });

                match rx_in.blocking_recv().unwrap() {
                    WorkerCtrl::Exit => return,
                    WorkerCtrl::HandleTcpStream { stream } => {
                        let start = Instant::now();
                        defer!(info!("T{i}: WorkerCtrl::TcpStream took {:?}", start.elapsed()));

                        rt_clone.block_on(async {
                            let mut offset = 0;
                            let mut len = 0;
                            let mut accum = Vec::new();
                            let mut buf = vec![0; 1024];
                            loop {
                                match stream.lock().unwrap().read(&mut buf).await {
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

                                        if n >= 2
                                            && buf[n - 2] == DELIM.as_bytes()[0]
                                            && buf[n - 1] == DELIM.as_bytes()[1]
                                        {
                                            break; // end-of-stream
                                        }
                                    }
                                }
                            }

                            tx_work_clone
                                .send(WorkerCtrl::HandleProto {
                                    stream,
                                    payload: accum,
                                    offset,
                                    len,
                                })
                                .await
                                .unwrap();
                        });
                    }
                    WorkerCtrl::HandleProto {
                        stream,
                        payload,
                        offset,
                        len,
                    } => {
                        (|| {
                            let start = Instant::now();
                            defer!(info!("T{i}: WorkerCtrl::HandleProto took {:?}", start.elapsed()));

                            let line = &payload[(offset + 2)..(len + offset)];
                            let s_line = String::from_utf8_lossy(line);
                            info!("T{i}: payload={}", s_line);

                            let mut rbs: Vec<DuckRecordBatch> = vec![];
                            let mut err_rb = vec![];
                            let err_schema = Arc::new(Schema::new(vec![Field::new("error", DataType::Utf8, false)]));

                            match &payload[0] {
                                b'$' => match &payload[offset..(offset + 2)] {
                                    b"x:" => match conn.execute(&s_line, params![]) {
                                        Err(e) => {
                                            let mut err = String::new();
                                            write!(&mut err, "-{e}{DELIM}").unwrap();
                                            err_rb = vec![
                                                RecordBatch::try_new(
                                                    err_schema.clone(),
                                                    vec![Arc::new(StringArray::from(vec![err.as_str()]))],
                                                )
                                                .unwrap(),
                                            ];
                                        }
                                        Ok(_) => {
                                            err_rb = vec![
                                                RecordBatch::try_new(
                                                    err_schema.clone(),
                                                    vec![Arc::new(StringArray::from(vec![OK]))],
                                                )
                                                .unwrap(),
                                            ];
                                        }
                                    },
                                    b"q:" => {
                                        let mut stmt = conn.prepare(&s_line).unwrap();
                                        rbs = stmt.query_arrow([]).unwrap().collect();
                                    }
                                    _ => {}
                                },
                                _ => {
                                    let mut err = String::new();
                                    write!(&mut err, "-ERR Unknown command{DELIM}").unwrap();
                                    err_rb = vec![
                                        RecordBatch::try_new(
                                            err_schema.clone(),
                                            vec![Arc::new(StringArray::from(vec![err.as_str()]))],
                                        )
                                        .unwrap(),
                                    ];
                                }
                            }

                            let mut ipc_writer = IpcWriter {
                                stream: stream,
                                handle: rt_clone.handle(),
                            };

                            if err_rb.len() > 0 {
                                let mut writer = match StreamWriter::try_new(&mut ipc_writer, &err_schema) {
                                    Err(_) => return,
                                    Ok(v) => v,
                                };

                                let _ = writer.write(&err_rb[0]);
                                let _ = writer.finish();
                            } else {
                                let (s, _, _) = rbs[0].clone().into_parts();
                                let schema: Arc<Schema>;

                                // FIXME: There must be a better way than transmute() here.
                                unsafe {
                                    schema = std::mem::transmute(s.clone());
                                }

                                let mut writer = match StreamWriter::try_new(&mut ipc_writer, &schema) {
                                    Err(_) => return,
                                    Ok(v) => v,
                                };

                                for rb in rbs {
                                    let rb_t: RecordBatch;

                                    // FIXME: There must be a better way than transmute() here.
                                    unsafe {
                                        rb_t = std::mem::transmute(rb);
                                    }

                                    let _ = writer.write(&rb_t);
                                }

                                let _ = writer.finish();
                            }
                        })();
                    }
                }
            }
        }));
    }

    TcpServer::new(rt.clone(), args.api_host_port.clone(), tx_work.clone()).start();

    rx_ctrlc.recv()?;

    if op.len() > 0 {
        op[0].lock().unwrap().close();
    }

    for _ in work_handles.iter() {
        rt.clone().block_on(async {
            tx_work.send(WorkerCtrl::Exit).await.unwrap();
        });
    }

    for h in work_handles {
        h.join().unwrap();
    }

    Ok(())
}
