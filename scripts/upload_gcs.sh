#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 --local-dir <path> --log-date <YYYY-MM-DD>" >&2
    exit 1
}

LOCAL_DIR=""
LOG_DATE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --local-dir) LOCAL_DIR="$2"; shift 2 ;;
        --log-date)  LOG_DATE="$2";  shift 2 ;;
        *) usage ;;
    esac
done
[[ -z "$LOCAL_DIR" || -z "$LOG_DATE" ]] && usage

: "${GCS_BUCKET:?GCS_BUCKET env var not set}"
PREFIX="${GCS_PREFIX:-nginx-logs}"
DEST="gs://${GCS_BUCKET}/${PREFIX}/date=${LOG_DATE}/"

echo "Uploading ${LOCAL_DIR} -> ${DEST}"
gcloud storage rsync "${LOCAL_DIR}" "${DEST}" --recursive
