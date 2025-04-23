# ds-flights-etl

## Build container

```shell
make build
```

## Run

### Using docker

```shell
export ALL_FLIGHTS_PATH="gs://$(gcloud config get core/project)-cf-staging/flights/tzcorr/all_flights.txt"
export AIRPORT_CSV_PATH="gs://$(gcloud config get core/project)-cf-staging/bts/airport.csv"
export IMAGE_URL="dsflightsetl:$(git rev-parse HEAD)"
poetry run python3 __main__.py \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URL}"
```
