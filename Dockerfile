FROM python:3.11-slim-bookworm AS build

ENV BUILD_DIR=/build

RUN apt update && apt install -y curl && apt clean

WORKDIR $BUILD_DIR
COPY . ./
RUN pip install poetry
RUN poetry build --format wheel


FROM apache/beam_python3.11_sdk:2.64.0

COPY --from=build /build/dist/*.whl .
RUN pip install *.whl && rm -f *.whl