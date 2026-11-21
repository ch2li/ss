FROM python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyarrow==21.0.0
WORKDIR /build
COPY src/ ./src/
COPY build_native.py ./
RUN python build_native.py

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 ca-certificates && rm -rf /var/lib/apt/lists/* && useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY --from=build /build/engine /app/engine
COPY --from=build /build/lib/ /app/lib/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8
USER 1000:1000
CMD ["/app/engine"]
