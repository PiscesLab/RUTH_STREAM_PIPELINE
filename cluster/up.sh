#!/bin/bash
# Recreate the StateFun cluster, with checkpointing on or off.
#
# The containers were originally started by hand, so their configuration lived
# nowhere. This script is that configuration: the image, the network, the
# module mount, and the Flink properties, so a run can be reproduced and so
# checkpointing can be switched for an experiment.
#
# Checkpointing matters for two measurements: what it costs in throughput, and
# whether twin state survives a worker failure. It needs a checkpoint directory
# both containers can see, which is why /checkpoint-dir is a bind mount on the
# project volume rather than a path inside each container (the image's default,
# where the master and the workers would each write to their own copy).
#
#   ./cluster/up.sh                      # checkpointing on, 2 workers
#   CHECKPOINTS=off ./cluster/up.sh      # checkpointing off
#   WORKERS=4 ./cluster/up.sh            # more workers
#
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"

IMAGE=apache/flink-statefun:3.2.0
NET=statefun-net
MODULE="$REPO/statefun_app/module.yaml"
CKPT_DIR=${CKPT_DIR:-${PROJ_DIR:?PROJ_DIR is not set}/flink-checkpoints}
CHECKPOINTS=${CHECKPOINTS:-on}
WORKERS=${WORKERS:-2}
INTERVAL=${INTERVAL:-10s}
SLOTS=${SLOTS:-1}

# The StateFun image uses its own entrypoint, which ignores FLINK_PROPERTIES -
# settings passed that way are silently dropped, and checkpointing stays off.
# The configuration is therefore mounted as a file.
BASE="$REPO/cluster/flink-conf.checkpoints-${CHECKPOINTS}.yaml"
[ -f "$BASE" ] || { echo "no such config: $BASE"; exit 1; }

# SLOTS used to be declared here and never applied, and parallelism.default was
# set nowhere, so the job ran at parallelism 1: a single engine task read Kafka,
# held all the state and made every call to the serving tier. That is the
# engine-side limit the serving-tier scaling measurements ran into, so both are
# now written into the config the containers actually mount.
PARALLELISM=${PARALLELISM:-$((WORKERS * SLOTS))}
CONF="$REPO/cluster/.flink-conf.in-use.yaml"
cp "$BASE" "$CONF"
{
  echo ""
  echo "taskmanager.numberOfTaskSlots: ${SLOTS}"
  echo "parallelism.default: ${PARALLELISM}"
} >> "$CONF"

echo "checkpointing=${CHECKPOINTS} workers=${WORKERS} slots=${SLOTS} parallelism=${PARALLELISM} interval=${INTERVAL}"
mkdir -p "$CKPT_DIR"

sudo -n docker rm -f statefun-master >/dev/null 2>&1 || true
for i in $(seq 1 8); do sudo -n docker rm -f "statefun-worker${i}" >/dev/null 2>&1 || true; done
sudo -n docker rm -f statefun-worker >/dev/null 2>&1 || true
sudo -n docker network create "$NET" >/dev/null 2>&1 || true

sudo -n docker run -d --name statefun-master --network "$NET" -p 8081:8081 \
  -e ROLE=master -e MASTER_HOST=statefun-master \
  -v "$CONF":/opt/flink/conf/flink-conf.yaml \
  -v "$MODULE":/opt/statefun/modules/application-module/module.yaml \
  -v "$CKPT_DIR":/checkpoint-dir \
  "$IMAGE" >/dev/null

for i in $(seq 1 "$WORKERS"); do
  sudo -n docker run -d --name "statefun-worker${i}" --network "$NET" \
    -e ROLE=worker -e MASTER_HOST=statefun-master \
    -v "$CONF":/opt/flink/conf/flink-conf.yaml \
    -v "$MODULE":/opt/statefun/modules/application-module/module.yaml \
    -v "$CKPT_DIR":/checkpoint-dir \
    "$IMAGE" >/dev/null
done

sudo -n docker ps --format '{{.Names}}: {{.Status}}' | grep statefun
echo "checkpoints will appear under $CKPT_DIR"
