# ds-flights-dpr

Project executes both batch and streaming processing for flight data

## Build container

```shell
make build
```

## Running stream analytics

### Running in local environment with docker

Open up the terminal and run the [harness container](https://beam.apache.org/documentation/runtime/sdk-harness-config/).
Pipeline will be ran in a separate environment where the [google application credential](https://cloud.google.com/docs/authentication/application-default-credentials) is available to client libraries.

```shell
export GOOGLE_APPLICATION_CRED_DIR=.config
export IMAGE_URL=$(git rev-parse HEAD)
docker run -v ~/${GOOGLE_APPLICATION_CRED_DIR}/:/root/${GOOGLE_APPLICATION_CRED_DIR}/ -p=50000:50000 dsflightsetl:$IMAGE_URL --worker_pool
```

In the separate terminal run below.

```shell
export BQ_STAGING_LOCATION="gs://$(gcloud config get core/project)-cf-staging/flights/staging/"
export BQ_TEMP_LOCATION="gs://$(gcloud config get core/project)-cf-staging/flights/temp/"
export ALL_FLIGHTS_PATH="gs://$(gcloud config get core/project)-cf-staging/flights/tzcorr/all_flights.txt"
export AIRPORT_CSV_PATH="gs://$(gcloud config get core/project)-cf-staging/bts/airport.csv"
poetry run python3 __main__.py \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_type="EXTERNAL" \
--environment_config="localhost:50000"\
--project=$(gcloud config get core/project) \
--staging-location=$BQ_STAGING_LOCATION \
--temp_location=$BQ_TEMP_LOCATION
```

### Running in the cloud

Build dependencies for python.

```shell
make package
```

#### TzCorr

```shell
export REGION=asia-east1
export PROJECT=$(gcloud config get core/project)
export BQ_STAGING_LOCATION="gs://${PROJECT}-cf-staging/flights/staging/"
export BQ_TEMP_LOCATION="gs://${PROJECT}-cf-staging/flights/temp/"
export ALL_FLIGHTS_PATH="gs://${PROJECT}-cf-staging/flights/tzcorr/all_flights.txt"
export AIRPORT_CSV_PATH="gs://${PROJECT}-cf-staging/bts/airport.csv"
export EXTRA_PACKAGE=dist/dsflightsetl-$(poetry version -s)-py3-none-any.whl
make package ; poetry run python3 __main__.py \
--runner=DataflowRunner \
--job_name=timecorr \
--region=$REGION \
--project=$PROJECT \
--staging_location=$BQ_STAGING_LOCATION \
--temp_location=$BQ_TEMP_LOCATION \
--extra_package $EXTRA_PACKAGE \
--max_num_workers=3 \
--service_account_email=svc-dataflow-flight-job@dsongcp-452504.iam.gserviceaccount.com
```

##### Note
You can restrict the amount of data to transform by using `--sample_rate`

```shell
export REGION=asia-east1
export PROJECT=$(gcloud config get core/project)
export BQ_STAGING_LOCATION="gs://${PROJECT}-cf-staging/flights/staging/"
export BQ_TEMP_LOCATION="gs://${PROJECT}-cf-staging/flights/temp/"
export ALL_FLIGHTS_PATH="gs://${PROJECT}-cf-staging/flights/tzcorr/all_flights.txt"
export AIRPORT_CSV_PATH="gs://${PROJECT}-cf-staging/bts/airport.csv"
export EXTRA_PACKAGE=dist/dsflightsetl-$(poetry version -s)-py3-none-any.whl
make package ; poetry run python3 __main__.py \
--runner=DataflowRunner \
--job_name=timecorr \
--region=$REGION \
--project=$PROJECT \
--staging_location=$BQ_STAGING_LOCATION \
--temp_location=$BQ_TEMP_LOCATION \
--extra_package $EXTRA_PACKAGE \
--max_num_workers=3 \
--service_account_email=svc-dataflow-flight-job@dsongcp-452504.iam.gserviceaccount.com \
--sample_rate=0.001
```

#### StreamAgg

```shell
export REGION=asia-east1
export PROJECT=$(gcloud config get core/project)
export BQ_STAGING_LOCATION="gs://${PROJECT}-cf-staging/flights/staging/"
export BQ_TEMP_LOCATION="gs://${PROJECT}-cf-staging/flights/temp/"
export EXTRA_PACKAGE=dist/dsflightsetl-$(poetry version -s)-py3-none-any.whl
make package ; poetry run python3 __main__.py \
--runner=DataflowRunner \
--job_name=streaming \
--region=$REGION \
--project=$PROJECT \
--staging_location=$BQ_STAGING_LOCATION \
--temp_location=$BQ_TEMP_LOCATION \
--extra_package $EXTRA_PACKAGE \
--max_num_workers=3 \
--service_account_email=svc-dataflow-flight-job@dsongcp-452504.iam.gserviceaccount.com \
--streaming
```

### Running the simulator

For example.

```shell
poetry run python3 scripts/simulator.py \
  --start_time "2015-05-01 00:00:00 UTC" \
  --end_time "2015-05-04 00:00:00 UTC" \
  --project_id dsongcp-452504
```
