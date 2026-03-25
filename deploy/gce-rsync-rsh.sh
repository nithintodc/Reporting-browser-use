#!/usr/bin/env bash
# Rsync "remote shell" helper — do not run by hand.
# Rsync invokes: $0 INSTANCE rsync --server ...
# We forward to: gcloud compute ssh INSTANCE --zone=... -- rsync --server ...
set -euo pipefail

instance="${1:?}"
shift

zone="${GCP_ZONE:?GCP_ZONE must be set (run via deploy.sh)}"
cmd=(gcloud compute ssh "$instance" --zone="$zone")
[[ -n "${GCP_PROJECT_ID:-}" ]] && cmd+=(--project="${GCP_PROJECT_ID}")
[[ "${GCP_RSYNC_IAP:-0}" == "1" ]] && cmd+=(--tunnel-through-iap)
cmd+=(-- "$@")
exec "${cmd[@]}"
