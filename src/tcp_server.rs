use async_channel::Sender as AsyncSender;
use log::*;
use std::{
    sync::{Arc, Mutex},
    thread,
};
use tokio::{net::TcpListener, runtime::Runtime};

use crate::WorkerCtrl;

pub struct TcpServer {
    rt: Arc<Runtime>,
    host_port: String,
    tx_work: AsyncSender<WorkerCtrl>,
}

impl TcpServer {
    pub fn new(rt: Arc<Runtime>, host_port: String, tx_work: AsyncSender<WorkerCtrl>) -> Self {
        TcpServer { rt, host_port, tx_work }
    }

    pub fn start(&self) {
        let rt = self.rt.clone();
        let host_port = self.host_port.clone();
        let tx_work = self.tx_work.clone();
        thread::spawn(move || {
            rt.block_on(async {
                let listen = TcpListener::bind(&host_port).await.unwrap();
                info!("listening from {}", &host_port);

                loop {
                    let (stream, addr) = listen.accept().await.unwrap();
                    info!("accepted connection from {}", addr);

                    let tx_work_clone = tx_work.clone();
                    rt.spawn(async move {
                        tx_work_clone
                            .send(WorkerCtrl::HandleTcpStream {
                                stream: Arc::new(Mutex::new(stream)),
                            })
                            .await
                            .unwrap();
                    });
                }
            });
        });
    }
}
