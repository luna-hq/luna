use crate::WorkerCtrl;
use async_channel::Sender as AsyncSender;
use log::*;
use std::{sync::Arc, thread};
use tokio::{net::TcpListener, runtime::Runtime};

pub struct TcpServer {
    rt: Arc<Runtime>,
    host_port: String,
    tx_work: AsyncSender<WorkerCtrl>,
}

impl TcpServer {
    pub fn new(rt: Arc<Runtime>, host_port: String, tx_work: AsyncSender<WorkerCtrl>) -> Self {
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
                    let (stream, _) = listen.accept().await.unwrap();
                    let tx_work = tx_work.clone();
                    rt.spawn(async move {
                        tx_work.send(WorkerCtrl::HandleTcpStream { stream }).await.unwrap();
                    });
                }
            });
        });
    }
}
