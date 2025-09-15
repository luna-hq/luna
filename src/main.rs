mod gen_work;
mod ipc_writer;
mod op_handler;
mod tcp_server;
mod utils;

use anyhow::Result;
use clap::Parser;
use ctrlc;
use duckdb::{Connection, params};
use gen_work::{WorkPool, WorkerCtrl};
use hedge_rs::*;
use ipc_writer::IpcWriter;
use log::*;
use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, Sender, channel},
};
use tcp_server::TcpServer;
use tokio::runtime::Builder;

#[macro_use(defer)]
extern crate scopeguard;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// API (TCP) host:port (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:9090")]
    api_host_port: String,

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
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

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
            op[0].lock().unwrap().run()?;
        }

        op_handler::run(args.node_id.clone(), rx_comms);
    }

    let rt = Arc::new(Builder::new_multi_thread().enable_all().build()?);

    let base_conn = Connection::open_in_memory()?;
    base_conn.execute("INSTALL httpfs;", params![])?;
    base_conn.execute("LOAD httpfs;", params![])?;

    let (tx_work, rx_work) = async_channel::unbounded::<WorkerCtrl>();
    let mut wp = WorkPool::new(rt.clone(), base_conn.try_clone()?, tx_work.clone(), rx_work.clone());
    wp.run()?;

    TcpServer::new(rt.clone(), args.api_host_port.clone(), tx_work.clone()).run();

    rx_ctrlc.recv()?;

    if op.len() > 0 {
        op[0].lock().unwrap().close();
    }

    wp.close();
    Ok(())
}
