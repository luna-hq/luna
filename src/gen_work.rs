use crate::IpcWriter;
use anyhow::Result;
use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use duckdb::{Connection, arrow::record_batch::RecordBatch as DuckRecordBatch, params};
use log::*;
use memchr::memmem;
use std::{
    collections::HashMap,
    fmt::Write as _,
    mem,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Instant,
};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    runtime::Runtime,
    sync::mpsc::{self as tokio_mpsc},
};

pub const DELIM: &str = "\r\n";
pub const OK: &str = "+OK\r\n";

#[derive(Clone, Debug)]
pub enum WorkerCtrl {
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

pub struct WorkPool {
    rt: Arc<Runtime>,
    conn: Connection,
    tx_work: AsyncSender<WorkerCtrl>,
    rx_work: AsyncReceiver<WorkerCtrl>,
    work_handles: Vec<Option<JoinHandle<()>>>,
}

impl WorkPool {
    pub fn new(
        rt: Arc<Runtime>,
        conn: Connection,
        tx_work: AsyncSender<WorkerCtrl>,
        rx_work: AsyncReceiver<WorkerCtrl>,
    ) -> Self {
        WorkPool {
            rt,
            conn,
            tx_work,
            rx_work,
            work_handles: vec![],
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let rx_works: Arc<Mutex<HashMap<usize, AsyncReceiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cpus = num_cpus::get();

        for i in 0..cpus {
            let rx_works_clone = rx_works.clone();

            {
                let mut rxv = rx_works_clone.lock().unwrap();
                rxv.insert(i, self.rx_work.clone());
            }
        }

        for i in 0..cpus {
            let rt_clone = self.rt.clone();
            let conn = self.conn.try_clone()?;
            let tx_work_clone = self.tx_work.clone();
            let rx_works_clone = rx_works.clone();
            let h = thread::spawn(move || {
                loop {
                    let mut rx: Option<AsyncReceiver<WorkerCtrl>> = None;

                    {
                        let mg = match rx_works_clone.lock() {
                            Err(_) => return,
                            Ok(v) => v,
                        };

                        if let Some(v) = mg.get(&i) {
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
                                let err_schema =
                                    Arc::new(Schema::new(vec![Field::new("error", DataType::Utf8, false)]));

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
                                        _ => {
                                            let mut err = String::new();
                                            write!(&mut err, "-ERR Unknown prefix{DELIM}").unwrap();
                                            err_rb = vec![
                                                RecordBatch::try_new(
                                                    err_schema.clone(),
                                                    vec![Arc::new(StringArray::from(vec![err.as_str()]))],
                                                )
                                                .unwrap(),
                                            ];
                                        }
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
                                    let (schema, _, _) = rbs[0].clone().into_parts();
                                    let schema_t: Arc<Schema>;

                                    // FIXME: There must be a better way than transmute() here.
                                    unsafe {
                                        schema_t = mem::transmute(schema.clone());
                                    }

                                    let mut writer = match StreamWriter::try_new(&mut ipc_writer, &schema_t) {
                                        Err(_) => return,
                                        Ok(v) => v,
                                    };

                                    for rb in rbs {
                                        let rb_t: RecordBatch;

                                        // FIXME: There must be a better way than transmute() here.
                                        unsafe {
                                            rb_t = mem::transmute(rb);
                                        }

                                        let _ = writer.write(&rb_t);
                                    }

                                    let _ = writer.finish();
                                }
                            })();
                        }
                    }
                }
            });

            self.work_handles.push(Some(h));
        }

        Ok(())
    }

    pub fn close(&mut self) {
        for _ in &self.work_handles {
            self.rt.clone().block_on(async {
                self.tx_work.send(WorkerCtrl::Exit).await.unwrap();
            });
        }

        for h in self.work_handles.iter_mut() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
    }
}
