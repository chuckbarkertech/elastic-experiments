# Elastic Loader Demo

## Arguments
1. --csv-path <path> - path to csv from open data motor vehicle crash history
2. --cluster-url <url> - url for cluster, ex https://127.0.0.1:9200/
3. --username <username>
4. --password <password>
5. --index-name <index-name>
6. --batch-size <batch-size> - if 1, use single requests otherwise, bulk
7. --async-throttle <async-throttle> - number of simultaneous requests
8. --refresh - type of refresh (true, false, wait_for)

## Headers

year,case_vehicle_id,vehicle_body_type,registration_class,action_prior_to_accident,type_or_axles_of_truck_or_bus,direction_of_travel,fuel_type,vehicle_year,state_of_registration,number_of_occupants,engine_cylinders,vehicle_make,contributing_factor_1,contributing_factor_1_description,contributing_factor_2,contributing_factor_2_description,event_type,partial_vin
