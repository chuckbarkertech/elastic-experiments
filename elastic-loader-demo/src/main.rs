#![feature(async_fn_in_trait)]

extern crate core;

use std::thread::sleep;
use std::time::{Duration, Instant};
use elasticsearch::auth::Credentials;
use crate::bulk_load::BulkElasticLoad;
use crate::elastic_load::{ElasticLoad};
use crate::motor_vehicle_crash::MotorVehicleCrash;
use crate::single_load::SingleElasticLoad;
use clap::Parser;
use elasticsearch::params::Refresh;

mod bulk_load;
mod elastic_load;
mod motor_vehicle_crash;
mod single_load;

enum Loader {
    BulkLoader(BulkElasticLoad),
    SingleLoader(SingleElasticLoad),
}

// Run - Batch=10000, Async=1, Refresh=False
// run --package elastic-loader-demo --bin elastic-loader-demo -- --csv-path "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv" --cluster-url "https://127.0.0.1:9200/" --username "elastic" --password "elastic" --index-name "motor-vehicle-crashes" --batch-size 10000 --async-throttle 1 --refresh "false"
// Run Release - Batch=10000, Async=1, Refresh=False
// run --release --package elastic-loader-demo --bin elastic-loader-demo -- --csv-path "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv" --cluster-url "https://127.0.0.1:9200/" --username "elastic" --password "elastic" --index-name "motor-vehicle-crashes" --batch-size 10000 --async-throttle 1 --refresh "false"

#[derive(Parser)]
struct Cli {
    #[arg(long = "csv-path", value_hint = clap::ValueHint::FilePath)]
    csv_path: String,
    #[arg(long = "cluster-url", value_hint = clap::ValueHint::Url, default_value = "https://127.0.0.1:9200/")]
    cluster_url: String,
    #[arg(long = "username")]
    username: String,
    #[arg(long = "password")]
    password: String,
    #[arg(long = "index-name")]
    index_name: String,
    #[arg(long = "batch-size", default_value_t = 10_000)]
    batch_size: usize,
    #[arg(long = "async-throttle", default_value_t = 5)]
    async_throttle: usize,
    #[arg(long = "refresh", default_value = "false", value_parser =
    clap::builder::PossibleValuesParser::new(["true", "false", "wait_for"]))]
    refresh: String,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    // "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv"
    let crashes = MotorVehicleCrash::load_csv(cli.csv_path)?;

    let refresh = if cli.refresh == "true" { Refresh::True }
    else if cli.refresh == "wait_for" { Refresh::WaitFor }
    else { Refresh::False };

    let loader = if cli.batch_size > 1 {
        Loader::BulkLoader(
            BulkElasticLoad::builder()
                .with_uri(cli.cluster_url)
                .with_credentials(Credentials::Basic(
                    cli.username, cli.password))
                .with_index(cli.index_name)
                .with_throttle(cli.async_throttle)
                .with_refresh(refresh)
                .with_batch_size(cli.batch_size)
                .build()?
        )
    } else {
        Loader::SingleLoader(
            SingleElasticLoad::builder()
                .with_uri(cli.cluster_url)
                .with_credentials(Credentials::Basic(
                    cli.username, cli.password))
                .with_index(cli.index_name)
                .with_throttle(cli.async_throttle)
                .with_refresh(refresh)
                .build()?
        )
    };

    let (start, tally) = match loader {
        Loader::SingleLoader(loader) => {
            sleep(Duration::new(60, 0));
            let start = Instant::now();
            (start, loader.load(&crashes[..]).await?)
        }
        Loader::BulkLoader(loader) => {
            sleep(Duration::new(60, 0));
            let start = Instant::now();
            (start, loader.load(&crashes[..]).await?)
        }
    };

    let total_records = tally.num_total;
    let total_created = tally.num_created;
    let total_failed = tally.num_failed;

    let duration = start.elapsed();
    println!("Total Records: {total_records:?}");
    println!("Total Created: {total_created:?}");
    println!("Total Failed: {total_failed:?}");
    println!("Duration: {duration:?}");
    let mut duration_secs = usize::try_from(duration.as_secs())?;
    if duration_secs == 0 { duration_secs = 1; }
    let records_per_second = total_records / duration_secs;
    println!("Records Per Second: {records_per_second:?}");
    println!();
    println!();
    println!();

    Ok(())
}

