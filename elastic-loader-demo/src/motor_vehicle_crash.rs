use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct MotorVehicleCrash {
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

impl MotorVehicleCrash {
    pub fn load_csv(filename: String) -> Result<Vec<Box<MotorVehicleCrash>>, Box<dyn std::error::Error>> {
        let mut rdr = csv::Reader::from_path(filename).unwrap();
        let mut records: Vec<Box<MotorVehicleCrash>> = Vec::with_capacity(1_000_000);
        for result in rdr.deserialize() {
            let record: MotorVehicleCrash = result?;
            records.push(Box::new(record));
        }
        Ok(records)
    }
}