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
