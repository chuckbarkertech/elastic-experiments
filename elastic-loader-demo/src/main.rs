#![feature(async_fn_in_trait)]

extern crate core;

use std::time::Instant;
use crate::bulk_load::BulkElasticLoad;
use crate::elastic_load::{ElasticLoad};
use crate::motor_vehicle_crash::MotorVehicleCrash;

mod bulk_load;
mod elastic_load;
mod motor_vehicle_crash;


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crashes = MotorVehicleCrash::load_csv(
        String::from(
            "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv"))?;


    let start = Instant::now();
    let mut loader = BulkElasticLoad::new("https://localhost:9200/")?;
    let tally = ElasticLoad::load(&mut loader, &crashes[..]).await?;

    let total_records = tally.num_total;
    let total_created = tally.num_created;
    let total_failed = tally.num_failed;

    let duration = start.elapsed();
    println!("Total Records: {total_records:?}");
    println!("Total Created: {total_created:?}");
    println!("Total Failed: {total_failed:?}");
    println!("Duration: {duration:?}");
    let records_per_second = total_records / usize::try_from(duration.as_secs())?;
    println!("Records Per Second: {records_per_second:?}");
    Ok(())
}


// async fn load_motor_vehicles() -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
//     let f = "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv";
//     let mut rdr = csv::Reader::from_path(f).unwrap();
//     let mut records: Vec<Box<MotorVehicleCrashRecord>> = Vec::with_capacity(10_000);
//     let mut loader = BulkElasticLoad::new("https://localhost:9200/")?;
//     let mut response_total = ElasticLoadResults::new();
//     for result in rdr.deserialize() {
//         let record: MotorVehicleCrashRecord = result?;
//         records.push(Box::new(record));
//         if records.len() >= 10_000 {
//             response_total += ElasticLoad::load(&mut loader, &records).await?;
//             records.clear();
//         }
//     }
//     if records.len() > 0 {
//         response_total += ElasticLoad::load(&mut loader, &records).await?;
//         records.clear();
//     }
//
//     Ok(response_total)
// }

