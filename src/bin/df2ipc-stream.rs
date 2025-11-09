#![allow(clippy::restriction)]
use std::io;

use clap::Parser;

use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::options::CsvReadOptions;
use datafusion::datasource::file_format::options::NdJsonReadOptions;
use datafusion::execution::context::SessionContext;

use rs_df2ipc_stream::df2stdout;

struct Session(SessionContext);

impl Session {
    pub fn new() -> Self {
        Self(SessionContext::new())
    }

    pub async fn read_csv_default(&self, csv_path: &str) -> Result<DataFrame, io::Error> {
        self.0
            .read_csv(csv_path, CsvReadOptions::default())
            .await
            .map_err(io::Error::other)
    }

    pub async fn read_json_default(&self, jsonl_path: &str) -> Result<DataFrame, io::Error> {
        self.0
            .read_json(jsonl_path, NdJsonReadOptions::default())
            .await
            .map_err(io::Error::other)
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Input file path
    #[arg(value_name = "FILE")]
    path: String,
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let cli = Cli::parse();
    let sess = Session::new();
    let df = if cli.path.ends_with(".csv") {
        sess.read_csv_default(&cli.path).await?
    } else if cli.path.ends_with(".json") {
        sess.read_json_default(&cli.path).await?
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unsupported file type",
        ));
    };
    df2stdout(df).await
}
