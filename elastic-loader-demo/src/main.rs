extern crate core;

use std::time::Instant;
use serde::{Deserialize, Serialize};
use crate::bulk_load::BulkElasticLoad;
use crate::elastic_load::{ElasticLoad, ElasticLoadResults};

mod bulk_load;
mod elastic_load;


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let bulk_totals = load_motor_vehicles().await?;

    let total_records = bulk_totals.num_total;
    let total_created = bulk_totals.num_created;
    let total_failed = bulk_totals.num_failed;

    let duration = start.elapsed();
    println!("Total Records: {total_records:?}");
    println!("Total Created: {total_created:?}");
    println!("Total Failed: {total_failed:?}");
    println!("Duration: {duration:?}");
    let records_per_second = total_records / usize::try_from(duration.as_secs())?;
    println!("Records Per Second: {records_per_second:?}");
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct MotorVehicleCrashRecord {
    year: String,
    case_vehicle_id: String,
    vehicle_body_type: String,
    registration_class: String,
    action_prior_to_accident: String,
    type_or_axles_of_truck_or_bus: String,
    direction_of_travel: String,
    fuel_type: String,
    vehicle_year: String,
    state_of_registration: String,
    number_of_occupants: String,
    engine_cylinders: String,
    vehicle_make: String,
    contributing_factor_1: String,
    contributing_factor_1_description: String,
    contributing_factor_2: String,
    contributing_factor_2_description: String,
    event_type: String,
    partial_vin: String,
}

async fn load_motor_vehicles() -> Result<ElasticLoadResults, Box<dyn std::error::Error>> {
    let f = "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv";
    let mut rdr = csv::Reader::from_path(f).unwrap();
    let mut records: Vec<Box<MotorVehicleCrashRecord>> = Vec::with_capacity(10_000);
    let loader = BulkElasticLoad::new("http://localhost:9200/")?;
    let mut response_total = ElasticLoadResults::new();
    for result in rdr.deserialize() {
        let record: MotorVehicleCrashRecord = result?;
        records.push(Box::new(record));
        if records.len() >= 10_000 {
            response_total+= ElasticLoad::load(&loader, &records).await?;
            records.clear();
        }
    }
    if records.len() > 0 {
        response_total+= ElasticLoad::load(&loader, &records).await?;
        records.clear();
    }

    Ok(response_total)
}

