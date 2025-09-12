mod utils;

use anyhow::Result;
use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_channel::Receiver as AsyncReceiver;
use clap::Parser;
use ctrlc;
use duckdb::{
    Connection,
    arrow::{record_batch::RecordBatch as DuckRecordBatch, util::pretty::print_batches},
    params,
};
use hedge_rs::*;
use log::*;
use std::{
    collections::HashMap,
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
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    runtime::{Builder, Handle},
};

#[macro_use(defer)]
extern crate scopeguard;

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
    Dummy { s: Arc<Mutex<TcpStream>> },
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!(
        "start: api={}, node={}, lock={}/{}/{}",
        &args.api_host_port, &args.node_id, &args.hedge_db, &args.hedge_table, &args.hedge_lockname,
    );

    'onetime: loop {
        if &args.preload_csv == "?" {
            break 'onetime;
        }

        let conn = Connection::open_in_memory()?;

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
            write!(&mut q, "read_csv('{}', ", &args.preload_csv).unwrap();
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
            let rbs: Vec<DuckRecordBatch> = stmt.query_arrow([])?.collect();
            if rbs.is_empty() || rbs[0].num_rows() == 0 {
                error!("No data found.");
            } else {
                print_batches(&rbs).unwrap();
            }
        }

        {
            let start = Instant::now();
            defer!(info!("1-took {:?}", start.elapsed()));

            let mut stmt = conn.prepare("select uuid, date, payer from tmpcur;")?;
            let rbs: Vec<DuckRecordBatch> = stmt.query_arrow([])?.collect();

            let mut count: u64 = 0;
            for rb in rbs.iter() {
                count += rb.num_rows() as u64;
            }

            info!("total={}, len={}", count, rbs.len());
        }

        break 'onetime;
    }

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

        // Handler thread for both send() and broadcast() APIs.
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

    let rt = Arc::new(Builder::new_multi_thread().enable_all().build()?);

    let (tx_work, rx_work) = async_channel::unbounded::<WorkerCtrl>();
    let rx_works: Arc<Mutex<HashMap<usize, AsyncReceiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
    let cpus = num_cpus::get();

    for i in 0..cpus {
        let rx_work_clone = rx_works.clone();

        {
            let mut rxv = rx_work_clone.lock().unwrap();
            rxv.insert(i, rx_work.clone());
        }
    }

    for i in 0..cpus {
        let rt_clone = rt.clone();
        let rx_works_clone = rx_works.clone();
        thread::spawn(move || {
            loop {
                let mut rx: Option<AsyncReceiver<WorkerCtrl>> = None;

                {
                    let rxval = match rx_works_clone.lock() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("T{i}: lock failed: {e}");
                            break;
                        }
                    };

                    if let Some(v) = rxval.get(&i) {
                        rx = Some(v.clone());
                    }
                }

                let (tx_in, mut rx_in) = tokio::sync::mpsc::unbounded_channel::<WorkerCtrl>();
                rt_clone.block_on(async {
                    tx_in.send(rx.unwrap().recv().await.unwrap()).unwrap();
                });

                match rx_in.blocking_recv().unwrap() {
                    WorkerCtrl::Dummy { s } => {
                        info!("T{i}: WorkerCtrl::Dummy received");

                        let mut my_ipc_writer = MyIpcWriter {
                            stream: s,
                            handle: rt_clone.handle(),
                        };

                        let (schema, batches) = create_batches().unwrap();
                        let mut writer = StreamWriter::try_new(&mut my_ipc_writer, &schema).unwrap();

                        for b in batches.iter() {
                            writer.write(&b).unwrap();
                        }

                        writer.finish().unwrap();
                    }
                }
            }
        });
    }

    let api_host_port = args.api_host_port.clone();
    thread::spawn(move || {
        rt.block_on(async {
            let listen = TcpListener::bind(&api_host_port).await.unwrap();
            info!("listening from {}", &api_host_port);

            loop {
                let (stream, addr) = listen.accept().await.unwrap();
                info!("accepted connection from: {}", addr);

                let tx_work_clone = tx_work.clone();
                rt.spawn(async move {
                    tx_work_clone
                        .send(WorkerCtrl::Dummy {
                            s: Arc::new(Mutex::new(stream)),
                        })
                        .await
                        .unwrap();
                });
            }
        });
    });

    rx_ctrlc.recv()?; // wait for Ctrl-C

    if op.len() > 0 {
        op[0].lock().unwrap().close();
    }

    Ok(())
}

struct MyIpcWriter<'a> {
    stream: Arc<Mutex<TcpStream>>,
    handle: &'a Handle,
}

impl<'a> std::io::Write for MyIpcWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.handle.block_on(async {
            self.stream.lock().unwrap().write_all(buf).await?;
            Ok(buf.len())
        })
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.handle.block_on(async {
            self.stream.lock().unwrap().flush().await?;
            Ok(())
        })
    }
}

fn create_batches() -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    // 1. Define the schema for our data.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    // 2. Create the first batch of data.
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.1, 20.2, 30.3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    // 3. Create the second batch of data.
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(Float64Array::from(vec![40.4, 50.5])),
            Arc::new(StringArray::from(vec!["qux", "quux"])),
        ],
    )?;

    Ok((schema, vec![batch1, batch2]))
}
