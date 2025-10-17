mod gen_work;
mod ipc_writer;
mod op_handler;
mod tcp_server;
mod utils;

use anyhow::{Result, anyhow};
use bcrypt::hash;
use clap::Parser;
use ctrlc;
use duckdb::{Connection, params};
use gen_work::{WorkCmd, WorkPool};
use hedge_rs::*;
use ipc_writer::IpcWriter;
use log::*;
use std::{
    fmt::Write as _,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
};
use tcp_server::TcpServer;
use tokio::runtime::Builder;

#[macro_use(defer)]
extern crate scopeguard;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// API (TCP) host:port (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:7688")]
    api_host_port: String,

    /// DB home directory
    #[arg(long, long, default_value = "/tmp")]
    db_home_dir: String,

    /// Node ID (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:8080")]
    node_id: String,

    /// Optional, Spanner database (for hedge-rs)
    /// Format: "projects/{p}/instances/{i}/databases/{db}"
    #[arg(long, long, default_value = "?", verbatim_doc_comment)]
    hedge_db: String,

    /// Optional, Spanner lock table (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    hedge_table: String,

    /// Optional, lock name (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    hedge_lockname: String,

    /// Password, requires AUTH when set (other than "?")
    #[arg(long, long, default_value = "?")]
    passwd: String,

    /// Test anything, set to any value (other than "?") to enable
    #[arg(long, long, default_value = "?")]
    scratch: String,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if args.scratch != "?" {
        return scratch();
    }

    info!(
        "start: api={}, node={}, lock={}/{}/{}",
        &args.api_host_port, &args.node_id, &args.hedge_db, &args.hedge_table, &args.hedge_lockname,
    );

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
            let mut mg = match op[0].lock() {
                Err(e) => return Err(anyhow!("{e}")),
                Ok(v) => v,
            };

            mg.run()?;
        }

        op_handler::run(args.node_id.clone(), rx_comms);
    }

    let rt = Arc::new(Builder::new_multi_thread().enable_all().build()?);

    let base_conn = Connection::open_in_memory()?;
    let mut set_home_dir = String::new();
    write!(&mut set_home_dir, "SET home_directory = '{}';", args.db_home_dir)?;
    base_conn.execute(&set_home_dir, params![])?;
    base_conn.execute("INSTALL httpfs;", params![])?;
    base_conn.execute("LOAD httpfs;", params![])?;

    let (tx_work, rx_work) = async_channel::unbounded::<WorkCmd>();
    let mut wp = WorkPool::new(rt.clone(), base_conn.try_clone()?, tx_work.clone(), rx_work.clone());
    wp.run()?;

    let mut pass_hash = String::new();
    if args.passwd != "?" {
        pass_hash = hash(args.passwd, 4)?;
    }

    TcpServer::new(
        rt.clone(),
        args.api_host_port.clone(),
        Arc::new(tx_work.clone()),
        Arc::new(pass_hash),
    )
    .run();

    rx_ctrlc.recv()?;

    if op.len() > 0 {
        let mut mg = match op[0].lock() {
            Err(e) => return Err(anyhow!("{e}")),
            Ok(v) => v,
        };

        mg.close();
    }

    wp.close();
    Ok(())
}

/// Anything goes here.
fn scratch() -> Result<()> {
    let s_line = String::from("zero;one;two;three;four;five;six;seven;eight;nine;ten;");
    s_line.split(';').for_each(|s| info!("{}", s));
    Ok(())
}
