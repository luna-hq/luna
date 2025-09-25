use crate::IpcWriter;
use crate::tcp_server::{EMPTY, OK};
use anyhow::{Result, anyhow};
use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use duckdb::{Connection, arrow::record_batch::RecordBatch as DuckRecordBatch, params};
use std::{
    collections::HashMap,
    fmt::Write as _,
    mem,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};
use tokio::{net::tcp::OwnedWriteHalf, runtime::Runtime};

#[derive(Debug)]
pub enum WorkCmd {
    Exit,
    ProtoWriteError {
        write_half: Arc<Mutex<OwnedWriteHalf>>,
        input: String,
        error: String,
        tx_wait: AsyncSender<usize>,
    },
    ProtoDuckExecute {
        write_half: Arc<Mutex<OwnedWriteHalf>>,
        query: String,
        tx_wait: AsyncSender<usize>,
    },
    ProtoDuckQuery {
        write_half: Arc<Mutex<OwnedWriteHalf>>,
        query: String,
        tx_wait: AsyncSender<usize>,
    },
}

pub struct WorkPool {
    rt: Arc<Runtime>,
    conn: Connection,
    tx_work: AsyncSender<WorkCmd>,
    rx_work: AsyncReceiver<WorkCmd>,
    handles: Vec<Option<JoinHandle<Result<()>>>>,
}

impl WorkPool {
    pub fn new(
        rt: Arc<Runtime>,
        conn: Connection,
        tx_work: AsyncSender<WorkCmd>,
        rx_work: AsyncReceiver<WorkCmd>,
    ) -> Self {
        WorkPool {
            rt,
            conn,
            tx_work,
            rx_work,
            handles: vec![],
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let rx_works: Arc<Mutex<HashMap<usize, AsyncReceiver<WorkCmd>>>> = Arc::new(Mutex::new(HashMap::new()));
        let max = num_cpus::get() * 2;

        for i in 0..max {
            let rx_works_clone = rx_works.clone();

            {
                let mut mg = match rx_works_clone.lock() {
                    Err(e) => return Err(anyhow!("{e}")),
                    Ok(v) => v,
                };

                mg.insert(i, self.rx_work.clone());
            }
        }

        for i in 0..max {
            let rt_clone = self.rt.clone();
            let conn = self.conn.try_clone()?;
            let rx_works = rx_works.clone();
            let h = thread::spawn(move || -> Result<()> {
                loop {
                    let mut rx = vec![];

                    {
                        let mg = match rx_works.lock() {
                            Err(e) => return Err(anyhow!("{e}")),
                            Ok(v) => v,
                        };

                        if let Some(v) = mg.get(&i) {
                            rx = vec![v.clone()];
                        }
                    }

                    let (tx_bridge, rx_bridge) = async_channel::unbounded::<WorkCmd>();
                    rt_clone.block_on(async {
                        tx_bridge.send(rx[0].recv().await.unwrap()).await.unwrap();
                    });

                    match rx_bridge.recv_blocking()? {
                        WorkCmd::Exit => return Ok(()),
                        WorkCmd::ProtoWriteError {
                            write_half,
                            input,
                            error,
                            tx_wait,
                        } => {
                            proto_write_error(rt_clone.clone(), write_half, input, error)?;
                            tx_wait.send_blocking(0)?;
                        }
                        WorkCmd::ProtoDuckExecute {
                            write_half,
                            query,
                            tx_wait,
                        } => {
                            tx_wait.send_blocking(proto_duck_execute(
                                rt_clone.clone(),
                                conn.try_clone()?,
                                write_half,
                                query,
                            )?)?;
                        }
                        WorkCmd::ProtoDuckQuery {
                            write_half,
                            query,
                            tx_wait,
                        } => {
                            tx_wait.send_blocking(proto_duck_query(
                                rt_clone.clone(),
                                conn.try_clone()?,
                                write_half,
                                query,
                            )?)?;
                        }
                    }
                }
            });

            self.handles.push(Some(h));
        }

        Ok(())
    }

    pub fn close(&mut self) {
        for _ in &self.handles {
            self.rt.clone().block_on(async {
                self.tx_work.send(WorkCmd::Exit).await.unwrap();
            });
        }

        for h in self.handles.iter_mut() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
    }
}

fn proto_write_error(
    rt: Arc<Runtime>,
    write_half: Arc<Mutex<OwnedWriteHalf>>,
    input: String,
    error: String,
) -> Result<()> {
    let err_schema = Arc::new(Schema::new(vec![
        Field::new("input", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, false),
    ]));

    let err_rb = RecordBatch::try_new(
        err_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![input])),
            Arc::new(StringArray::from(vec![error])),
        ],
    )?;

    let mut writer = IpcWriter {
        write_half: write_half,
        handle: rt.handle(),
    };

    let mut sw = StreamWriter::try_new(&mut writer, &err_schema)?;
    let _ = sw.write(&err_rb);
    let _ = sw.finish();
    Ok(())
}

fn proto_duck_execute(
    rt: Arc<Runtime>,
    conn: Connection,
    write_half: Arc<Mutex<OwnedWriteHalf>>,
    query: String,
) -> Result<usize> {
    let mut ret: usize = 0;
    match conn.execute(&query, params![]) {
        Err(e) => {
            let mut err = String::new();
            write!(&mut err, "{e}")?;
            proto_write_error(rt.clone(), write_half, query, err)?;
        }
        Ok(n) => {
            proto_write_error(rt.clone(), write_half, query, OK.to_string())?;
            ret = n;
        }
    }

    Ok(ret)
}

fn proto_duck_query(
    rt: Arc<Runtime>,
    conn: Connection,
    write_half: Arc<Mutex<OwnedWriteHalf>>,
    query: String,
) -> Result<usize> {
    let mut rbs: Vec<DuckRecordBatch> = vec![];
    let mut errs = vec![];
    let mut stmt = vec![];
    match conn.prepare(&query) {
        Err(e) => {
            let mut err = String::new();
            write!(&mut err, "{e}")?;
            errs.push(query.clone());
            errs.push(err);
        }
        Ok(v) => stmt = vec![v],
    };

    if stmt.len() > 0 {
        match stmt[0].query_arrow([]) {
            Err(e) => {
                let mut err = String::new();
                write!(&mut err, "{e}")?;
                errs.push(query);
                errs.push(err);
            }
            Ok(v) => {
                rbs = v.collect();
                if rbs.len() == 0 {
                    errs.push(query);
                    errs.push(EMPTY.to_string());
                }
            }
        }
    }

    if errs.len() == 2 {
        proto_write_error(rt.clone(), write_half, errs[0].clone(), errs[1].clone())?;
        return Ok(0);
    }

    let mut writer = IpcWriter {
        write_half: write_half,
        handle: rt.handle(),
    };

    // NOTE: There must be a better way than transmute.
    let (schema, _, _) = rbs[0].clone().into_parts();
    let schema_t: Arc<Schema>;
    unsafe {
        schema_t = mem::transmute(schema);
    }

    let mut sw = StreamWriter::try_new(&mut writer, &schema_t)?;

    let n = rbs.len();
    for rb in rbs {
        let rb_t: RecordBatch;
        unsafe {
            rb_t = mem::transmute(rb);
        }

        let _ = sw.write(&rb_t);
    }

    let _ = sw.finish();
    Ok(n)
}
