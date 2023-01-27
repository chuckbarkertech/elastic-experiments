extern crate core;

use time::Instant;
use elasticsearch::http::Url;
use elasticsearch::http::transport::SingleNodeConnectionPool;
use elasticsearch::http::transport::TransportBuilder;
use elasticsearch::BulkParts;
use elasticsearch::Elasticsearch;
use elasticsearch::{BulkOperation, BulkOperations};
use tokio::time;
use serde::{Deserialize, Serialize};
use bulk_tally::BulkTally;

mod bulk_tally;
mod index_results;


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

async fn load_motor_vehicles() -> Result<BulkTally, Box<dyn std::error::Error>> {
    let f = "../../data/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv";
    let mut rdr = csv::Reader::from_path(f).unwrap();
    let mut records: Vec<MotorVehicleCrashRecord> = Vec::new();
    let mut bulk_tally = BulkTally::new();
    for result in rdr.deserialize() {
        let record: MotorVehicleCrashRecord = result?;
        records.push(record);
        if records.len() >= 10_000 {
            let next_id = bulk_tally.num_created - records.len() + 1;
            let bulk_response =
                index_cluster(next_id, &records).await?;
            bulk_tally += bulk_response;
            records.clear();
        }
    }
    if records.len() > 0 {
        let next_id = bulk_tally.num_created - records.len() + 1;
        let bulk_response =
            index_cluster(next_id, &records).await?;
        bulk_tally += bulk_response;
        records.clear();
    }

    Ok(bulk_tally)
}

async fn index_cluster(start_id: usize, records: &Vec<MotorVehicleCrashRecord>) -> Result<BulkTally, Box<dyn std::error::Error>> {
    let url = Url::parse("http://localhost:9200")?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    let mut ops = BulkOperations::new();
    let mut id: usize = start_id;
    for record in records {
        ops.push(BulkOperation::create(id.to_string(), record))?;
        id += 1;
    }

    let response = client.bulk(BulkParts::Index("motor-vehicle-crashes"))
        .body(vec![ops])
        .send()
        .await?;

    let bulk_response = BulkTally::parse(response).await?;

    Ok(bulk_response)
}

#[cfg(test)]
mod more_tests {
    #[test]
    #[ignore]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4, "result was not 4, it was {}", result);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4, "result was not 4, it was {}", result);
    }

    #[test]
    #[should_panic(expected = "get out of here")]
    fn it_panics() {
        panic!("get out of here")
    }

    #[test]
    fn it_is_ok() -> Result<(), String> {
        if 2 + 2 == 4 {
            Ok(())
        } else {
            Err(String::from("two plus two does not equal four"))
        }
    }
}
