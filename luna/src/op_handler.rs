use anyhow::Result;
use hedge_rs::*;
use log::*;
use std::{fmt::Write as _, sync::mpsc::Receiver, thread};

pub fn run(id: String, rx_comms: Receiver<Comms>) {
    thread::spawn(move || -> Result<()> {
        loop {
            match rx_comms.recv() {
                Err(e) => error!("{e}"),
                Ok(v) => match v {
                    Comms::ToLeader { msg, tx } => {
                        let msg_s = String::from_utf8(msg)?;
                        info!("[send()] received: {msg_s}");

                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from leader:{}", id.to_string())?;
                        tx.send(reply.as_bytes().to_vec())?;
                    }
                    Comms::Broadcast { msg, tx } => {
                        let msg_s = String::from_utf8(msg)?;
                        info!("[broadcast()] received: {msg_s}");

                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from {}", id.to_string())?;
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
