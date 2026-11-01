#!/usr/bin/env bash
# Create the two topics. Run after the Kafka container is up.
set -euo pipefail

PARTITIONS="${PARTITIONS:-3}"
KT=/opt/kafka/bin/kafka-topics.sh

docker exec etl4-kafka "$KT" --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic adsb.raw --partitions "$PARTITIONS" --replication-factor 1
docker exec etl4-kafka "$KT" --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic adsb.dlq --partitions 1 --replication-factor 1
docker exec etl4-kafka "$KT" --bootstrap-server localhost:9092 --list
