use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// API (TCP) host:port (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:7688")]
    pub api_host_port: String,

    /// DB home directory
    #[arg(long, long, default_value = "/tmp")]
    pub db_home_dir: String,

    /// Node ID (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:8080")]
    pub node_id: String,

    /// Optional, Spanner database (for hedge-rs)
    /// Format: "projects/{p}/instances/{i}/databases/{db}"
    #[arg(long, long, default_value = "?", verbatim_doc_comment)]
    pub hedge_db: String,

    /// Optional, Spanner lock table (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    pub hedge_table: String,

    /// Optional, lock name (for hedge-rs)
    #[arg(long, long, default_value = "luna")]
    pub hedge_lockname: String,

    /// Password, requires AUTH when set (other than "?")
    #[arg(long, long, default_value = "?")]
    pub passwd: String,

    /// Test anything, set to any value (other than "?") to enable
    #[arg(long, long, default_value = "?")]
    pub scratch: String,
}
