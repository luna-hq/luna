mod utils;

use anyhow::Result;
use clap::Parser;
use ctrlc;
use duckdb::{
    Connection,
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    params,
};
use hedge_rs::*;
use log::*;
use std::{
    env,
    fmt::Write as _,
    io::{BufReader, prelude::*},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::Instant,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[macro_use(defer)]
extern crate scopeguard;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Prefix for files to preload (gs://bucket/prefix, s3://bucket/prefix, /local/prefix)
    #[arg(long, long)]
    prefix: String,

    /// Node ID (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:8080")]
    node_id: String,

    /// Spanner database (for hedge-rs) (fmt: projects/p/instances/i/databases/db)
    #[arg(long, long, default_value = "?")]
    db_hedge: String,

    /// Spanner lock table (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    table: String,

    /// Lock name (for hedge-rs)
    #[arg(short, long, default_value = "luna")]
    name: String,

    /// Host:port for the API (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:9090")]
    api: String,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!(
        "start: prefix={}, api={}, node={}, lock={}/{}/{}, ",
        &args.prefix, &args.api, &args.node_id, &args.db_hedge, &args.table, &args.name,
    );

    'onetime: loop {
        let conn = Connection::open_in_memory()?;

        if true {
            break 'onetime;
        }

        {
            let start = Instant::now();
            defer!(info!("0-took {:?}", start.elapsed()));

            let mut q = String::new();
            write!(&mut q, "INSTALL httpfs;").unwrap();
            conn.execute(q.as_str(), params![])?;

            q.clear();
            write!(&mut q, "LOAD httpfs;").unwrap();
            conn.execute(q.as_str(), params![])?;

            q.clear();
            let key = env::var("LINKBATCHD_GCS_HMAC_KEY").unwrap();
            let secret = env::var("LINKBATCHD_GCS_HMAC_SECRET").unwrap();
            write!(&mut q, "CREATE SECRET (").unwrap();
            write!(&mut q, "TYPE gcs,").unwrap();
            write!(&mut q, "KEY_ID '{key}',").unwrap();
            write!(&mut q, "SECRET '{secret}'").unwrap();
            write!(&mut q, ");").unwrap();
            conn.execute(q.as_str(), params![])?;

            q.clear();
            write!(&mut q, "create table tmpcur as from ").unwrap();
            write!(&mut q, "read_csv('gs://awscur/{}*.csv', ", &args.prefix).unwrap();
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
            write!(&mut q, "'product/processorArchitecture':'VARCHAR',").unwrap();
            write!(&mut q, "'product/processorFeatures':'VARCHAR',").unwrap();
            write!(&mut q, "'product/productFamily':'VARCHAR',").unwrap();
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
            write!(&mut q, "}});").unwrap();
            conn.execute(q.as_str(), params![])?;

            let mut stmt = conn.prepare("DESCRIBE tmpcur;")?;
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

            let mut stmt = conn.prepare("select uuid from tmpcur;")?;
            let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

            let mut count: u64 = 0;
            for rb in rbs.iter() {
                count += rb.num_rows() as u64;
            }

            info!("total={}, len={}", count, rbs.len());
        }

        break 'onetime;
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;

    let mut op = vec![];
    if args.db_hedge != "?" {
        // We will use this channel for the 'send' and 'broadcast' features.
        // Use Sender as inputs, then we read replies through the Receiver.
        let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();

        op = vec![Arc::new(Mutex::new(
            OpBuilder::new()
                .id(args.node_id.clone())
                .db(args.db_hedge)
                .table(args.table)
                .name(args.name)
                .lease_ms(3_000)
                .tx_comms(Some(tx_comms.clone()))
                .build(),
        ))];

        {
            op[0].lock().unwrap().run()?;
        }

        // Start a new thread that will serve as handlers for both send() and broadcast() APIs.
        let id_handler = args.node_id.clone();
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
    }

    let runtime = Arc::new(tokio::runtime::Builder::new_multi_thread().enable_all().build()?);

    let api_host_port = args.api.clone();
    thread::spawn(move || {
        runtime.block_on(async {
            let listen = TcpListener::bind(&api_host_port).await.unwrap();
            info!("listening from {}", &api_host_port);

            loop {
                let (mut socket, addr) = listen.accept().await.unwrap();
                info!("accepted connection from: {}", addr);

                runtime.spawn(async move {
                    let mut buf = vec![0; 1024];
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        return;
                    }

                    socket.write_all(&buf[..n]).await.unwrap();
                    info!("echoed {} bytes back to {}", n, addr);
                });
            }
        });
    });

    rx.recv()?; // wait for Ctrl-C

    if op.len() > 0 {
        op[0].lock().unwrap().close();
    }

    Ok(())
}
