# ds-flights-etl

## Build container

```shell
make build
```

## Run

### Using docker

```shell
IMAGE_URL="dsflightsetl:$(git rev-parse HEAD)"
poetry run python3 __main__.py \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URL}"
```