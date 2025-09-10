mod utils;

use anyhow::Result;
use ctrlc;
use duckdb::{
    Connection, Result as DuckResult,
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    params,
};
use hedge_rs::*;
use log::*;
use std::{
    env,
    fmt::Write as _,
    io::{BufReader, prelude::*},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::Instant,
};
use utils::cur_columns;

#[macro_use(defer)]
extern crate scopeguard;

fn main() -> Result<()> {
    env_logger::init();
    cur_columns::pub_fn();

    {
        // Simple DuckDB example:
        let conn = Connection::open_in_memory()?;

        {
            let start = Instant::now();
            defer!(info!("0-took {:?}", start.elapsed()));

            let mut q = String::new();
            write!(&mut q, "create table tmpcur as from ").unwrap();
            // write!(&mut q, "read_csv('/home/f14t/069496712340_2025-08*.csv', ").unwrap();
            // write!(&mut q, "read_csv('/home/f14t/931817257079*.csv', ").unwrap();
            write!(&mut q, "read_csv('/home/f14t/999148548534*.csv', ").unwrap();
            write!(&mut q, "header = true, ").unwrap();
            write!(&mut q, "union_by_name = true, ").unwrap();
            write!(&mut q, "files_to_sniff = -1, ").unwrap();
            write!(&mut q, "types = {{").unwrap();
            write!(&mut q, "'uuid':'VARCHAR',").unwrap();
            write!(&mut q, "'date':'DATE',").unwrap();
            write!(&mut q, "'payer':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/LeaseContractLength':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/OfferingClass':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/PurchaseOption':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/AvailabilityZone':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/ReservationARN':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/Region':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/PaymentOption':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/EndTime':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/InstanceTypeFamily':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/PurchaseTerm':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/OfferingType':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/StartTime':'VARCHAR',").unwrap();
            write!(&mut q, "'identity/LineItemId':'VARCHAR',").unwrap();
            write!(&mut q, "'identity/TimeInterval':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/InvoiceId':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/InvoicingEntity':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/BillingEntity':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/BillType':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/PayerAccountId':'VARCHAR',").unwrap();
            write!(&mut q, "'bill/BillingPeriodStartDate':'TIMESTAMP',").unwrap();
            write!(&mut q, "'bill/BillingPeriodEndDate':'TIMESTAMP',").unwrap();
            write!(&mut q, "'lineItem/UsageAccountId':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/LineItemType':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/UsageStartDate':'TIMESTAMP',").unwrap();
            write!(&mut q, "'lineItem/UsageEndDate':'TIMESTAMP',").unwrap();
            write!(&mut q, "'lineItem/ProductCode':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/UsageType':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/Operation':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/AvailabilityZone':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/ResourceId':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/UsageAmount':'DOUBLE',").unwrap();
            write!(&mut q, "'lineItem/NormalizationFactor':'DOUBLE',").unwrap();
            write!(&mut q, "'lineItem/NormalizedUsageAmount':'DOUBLE',").unwrap();
            write!(&mut q, "'lineItem/CurrencyCode':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/UnblendedRate':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/UnblendedCost':'DOUBLE',").unwrap();
            write!(&mut q, "'lineItem/BlendedRate':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/BlendedCost':'DOUBLE',").unwrap();
            write!(&mut q, "'lineItem/LineItemDescription':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/TaxType':'VARCHAR',").unwrap();
            write!(&mut q, "'lineItem/LegalEntity':'VARCHAR',").unwrap();
            write!(&mut q, "'product/ProductName':'VARCHAR',").unwrap();
            write!(&mut q, "'product/alarmType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/availability':'VARCHAR',").unwrap();
            write!(&mut q, "'product/availabilityZone':'VARCHAR',").unwrap();
            write!(&mut q, "'product/capacitystatus':'VARCHAR',").unwrap();
            write!(&mut q, "'product/classicnetworkingsupport':'VARCHAR',").unwrap();
            write!(&mut q, "'product/clockSpeed':'VARCHAR',").unwrap();
            write!(&mut q, "'product/currentGeneration':'VARCHAR',").unwrap();
            write!(&mut q, "'product/databaseEngine':'VARCHAR',").unwrap();
            write!(&mut q, "'product/dedicatedEbsThroughput':'VARCHAR',").unwrap();
            write!(&mut q, "'product/deploymentOption':'VARCHAR',").unwrap();
            write!(&mut q, "'product/description':'VARCHAR',").unwrap();
            write!(&mut q, "'product/durability':'VARCHAR',").unwrap();
            write!(&mut q, "'product/ecu':'VARCHAR',").unwrap();
            write!(&mut q, "'product/engineCode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/enhancedNetworkingSupported':'VARCHAR',").unwrap();
            write!(&mut q, "'product/eventType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/feeCode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/feeDescription':'VARCHAR',").unwrap();
            write!(&mut q, "'product/fromLocation':'VARCHAR',").unwrap();
            write!(&mut q, "'product/fromLocationType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/fromRegionCode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/gpuMemory':'VARCHAR',").unwrap();
            write!(&mut q, "'product/group':'VARCHAR',").unwrap();
            write!(&mut q, "'product/groupDescription':'VARCHAR',").unwrap();
            write!(&mut q, "'product/instanceFamily':'VARCHAR',").unwrap();
            write!(&mut q, "'product/instanceType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/instanceTypeFamily':'VARCHAR',").unwrap();
            write!(&mut q, "'product/intelAvx2Available':'VARCHAR',").unwrap();
            write!(&mut q, "'product/intelAvxAvailable':'VARCHAR',").unwrap();
            write!(&mut q, "'product/intelTurboAvailable':'VARCHAR',").unwrap();
            write!(&mut q, "'product/licenseModel':'VARCHAR',").unwrap();
            write!(&mut q, "'product/location':'VARCHAR',").unwrap();
            write!(&mut q, "'product/locationType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/logsDestination':'VARCHAR',").unwrap();
            write!(&mut q, "'product/marketoption':'VARCHAR',").unwrap();
            write!(&mut q, "'product/maxIopsBurstPerformance':'VARCHAR',").unwrap();
            write!(&mut q, "'product/maxIopsvolume':'VARCHAR',").unwrap();
            write!(&mut q, "'product/maxThroughputvolume':'VARCHAR',").unwrap();
            write!(&mut q, "'product/maxVolumeSize':'VARCHAR',").unwrap();
            write!(&mut q, "'product/memory':'VARCHAR',").unwrap();
            write!(&mut q, "'product/messageDeliveryFrequency':'VARCHAR',").unwrap();
            write!(&mut q, "'product/messageDeliveryOrder':'VARCHAR',").unwrap();
            write!(&mut q, "'product/networkPerformance':'VARCHAR',").unwrap();
            write!(&mut q, "'product/normalizationSizeFactor':'VARCHAR',").unwrap();
            write!(&mut q, "'product/operatingSystem':'VARCHAR',").unwrap();
            write!(&mut q, "'product/operation':'VARCHAR',").unwrap();
            write!(&mut q, "'product/physicalProcessor':'VARCHAR',").unwrap();
            write!(&mut q, "'product/preInstalledSw':'VARCHAR',").unwrap();
            write!(&mut q, "'product/pricingplan':'VARCHAR',").unwrap();
            write!(&mut q, "'product/processorArchitecture':'VARCHAR',").unwrap();
            write!(&mut q, "'product/processorFeatures':'VARCHAR',").unwrap();
            write!(&mut q, "'product/productFamily':'VARCHAR',").unwrap();
            write!(&mut q, "'product/provider':'VARCHAR',").unwrap();
            write!(&mut q, "'product/queueType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/region':'VARCHAR',").unwrap();
            write!(&mut q, "'product/regionCode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/requestType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/servicecode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/servicename':'VARCHAR',").unwrap();
            write!(&mut q, "'product/sku':'VARCHAR',").unwrap();
            write!(&mut q, "'product/storage':'VARCHAR',").unwrap();
            write!(&mut q, "'product/storageClass':'VARCHAR',").unwrap();
            write!(&mut q, "'product/storageMedia':'VARCHAR',").unwrap();
            write!(&mut q, "'product/subservice':'VARCHAR',").unwrap();
            write!(&mut q, "'product/tenancy':'VARCHAR',").unwrap();
            write!(&mut q, "'product/toLocation':'VARCHAR',").unwrap();
            write!(&mut q, "'product/toLocationType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/toRegionCode':'VARCHAR',").unwrap();
            write!(&mut q, "'product/transferType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/type':'VARCHAR',").unwrap();
            write!(&mut q, "'product/usagetype':'VARCHAR',").unwrap();
            write!(&mut q, "'product/vcpu':'VARCHAR',").unwrap();
            write!(&mut q, "'product/version':'VARCHAR',").unwrap();
            write!(&mut q, "'product/volumeApiName':'VARCHAR',").unwrap();
            write!(&mut q, "'product/volumeType':'VARCHAR',").unwrap();
            write!(&mut q, "'product/vpcnetworkingsupport':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/RateCode':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/RateId':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/currency':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/publicOnDemandCost':'DOUBLE',").unwrap();
            write!(&mut q, "'pricing/publicOnDemandRate':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/term':'VARCHAR',").unwrap();
            write!(&mut q, "'pricing/unit':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/AmortizedUpfrontCostForUsage':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/AmortizedUpfrontFeeForBillingPeriod':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/EffectiveCost':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/EndTime':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/ModificationStatus':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/RecurringFeeForUsage':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/StartTime':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/SubscriptionId':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/TotalReservedNormalizedUnits':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/TotalReservedUnits':'VARCHAR',").unwrap();
            write!(&mut q, "'reservation/UnitsPerReservation':'VARCHAR',").unwrap();
            write!(
                &mut q,
                "'reservation/UnusedAmortizedUpfrontFeeForBillingPeriod':'DOUBLE',"
            )
            .unwrap();
            write!(&mut q, "'reservation/UnusedNormalizedUnitQuantity':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/UnusedQuantity':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/UnusedRecurringFee':'DOUBLE',").unwrap();
            write!(&mut q, "'reservation/UpfrontValue':'DOUBLE',").unwrap();
            write!(&mut q, "'savingsPlan/TotalCommitmentToDate':'DOUBLE',").unwrap();
            write!(&mut q, "'savingsPlan/SavingsPlanARN':'VARCHAR',").unwrap();
            write!(&mut q, "'savingsPlan/SavingsPlanRate':'DOUBLE',").unwrap();
            write!(&mut q, "'savingsPlan/UsedCommitment':'DOUBLE',").unwrap();
            write!(&mut q, "'savingsPlan/SavingsPlanEffectiveCost':'DOUBLE',").unwrap();
            write!(
                &mut q,
                "'savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod':'DOUBLE',"
            )
            .unwrap();
            write!(&mut q, "'savingsPlan/RecurringCommitmentForBillingPeriod':'DOUBLE',").unwrap();
            write!(&mut q, "'tags':'VARCHAR',").unwrap();
            write!(&mut q, "'costcategories':'VARCHAR'").unwrap();
            write!(&mut q, "}})").unwrap();

            conn.execute(q.as_str(), params![])?;

            let mut stmt = conn.prepare("DESCRIBE tmpcur")?;
            let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
            if rbs.is_empty() || rbs[0].num_rows() == 0 {
                error!("No data found.");
            } else {
                print_batches(&rbs).unwrap();
            }
        }

        {
            let start = Instant::now();
            defer!(info!("1-took {:?}", start.elapsed()));

            let mut stmt = conn.prepare("select uuid from tmpcur")?;
            let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

            let mut count: u64 = 0;
            for rb in rbs.iter() {
                count += rb.num_rows() as u64;
            }

            info!("total={}, len={}", count, rbs.len());
        }
    }

    // ---

    // hedge example:
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        error!("provide the db, table, id, and test host:port args");
        return Ok(());
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();

    let op = Arc::new(Mutex::new(
        OpBuilder::new()
            .db(args[1].clone())
            .table(args[2].clone())
            .name("fmdb".to_string())
            .id(args[3].to_string())
            .lease_ms(3_000)
            .tx_comms(Some(tx_comms.clone()))
            .build(),
    ));

    {
        op.lock().unwrap().run()?;
    }

    // Start a new thread that will serve as handlers for both send() and broadcast() APIs.
    let id_handler = args[3].clone();
    thread::spawn(move || {
        loop {
            match rx_comms.recv() {
                Ok(v) => match v {
                    // This is our 'send' handler. When we are leader, we reply to all
                    // messages coming from other nodes using the send() API here.
                    Comms::ToLeader { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[send()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from leader:{}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    // This is our 'broadcast' handler. When a node broadcasts a message,
                    // through the broadcast() API, we reply here.
                    Comms::Broadcast { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[broadcast()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from {}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    Comms::OnLeaderChange(state) => {
                        info!("leader state change: {state}");
                    }
                },
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            }
        }
    });

    // Starts a new thread for our test TCP server. Messages that start with 'q' will cause the
    // server thread to terminate. Messages that begin with 'send' will send that message to
    // the current leader. Finally, messages that begin with 'broadcast' will broadcast that
    // message to all nodes in the group.
    let op_tcp = op.clone();
    let host_port = args[4].clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(host_port.to_string()).unwrap();
        for stream in listen.incoming() {
            match stream {
                Err(_) => break,
                Ok(v) => {
                    let mut reader = BufReader::new(&v);
                    let mut msg = String::new();
                    reader.read_line(&mut msg).unwrap();

                    if msg.starts_with("q") {
                        break;
                    }

                    if msg.starts_with("send") {
                        let send = msg[..msg.len() - 1].to_string();
                        match op_tcp.lock().unwrap().send(send.as_bytes().to_vec()) {
                            Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
                            Err(e) => error!("send failed: {e}"),
                        }

                        continue;
                    }

                    if msg.starts_with("broadcast") {
                        let (tx_reply, rx_reply): (Sender<Broadcast>, Receiver<Broadcast>) = channel();
                        let send = msg[..msg.len() - 1].to_string();
                        op_tcp
                            .lock()
                            .unwrap()
                            .broadcast(send.as_bytes().to_vec(), tx_reply)
                            .unwrap();

                        // Read through all the replies from all nodes. An empty
                        // id or message marks the end of the streaming reply.
                        loop {
                            match rx_reply.recv().unwrap() {
                                Broadcast::ReplyStream { id, msg, error } => {
                                    if id == "" || msg.len() == 0 {
                                        break;
                                    }

                                    if error {
                                        error!("{:?}", String::from_utf8(msg).unwrap());
                                    } else {
                                        info!("{:?}", String::from_utf8(msg).unwrap());
                                    }
                                }
                            }
                        }

                        continue;
                    }

                    info!("{msg:?} not supported");
                }
            };
        }
    });

    rx.recv()?; // wait for Ctrl-C
    op.lock().unwrap().close();

    Ok(())
}
