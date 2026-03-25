#!/usr/bin/env bash
# Sync project code from this machine to the GCE VM (default: /opt/doordash-bot).
# Prerequisites: gcloud CLI, auth, and SSH to the instance (same as `gcloud compute ssh`).
#
# Usage (from repo root):
#   ./deploy.sh              # sync only
#   ./deploy.sh --install    # sync + pip install -r requirements.txt on the VM
#   ./deploy.sh --verify     # sync + run deploy/04-verify.sh on the VM
#   ./deploy.sh --delete     # rsync --delete (prune extra files on VM; see guide)
#
# Configure via environment (optional):
#   GCP_VM_NAME   default: todc-ent-applications
#   GCP_ZONE      default: us-west2-a
#   GCP_REMOTE_DIR default: /opt/doordash-bot
#   GCP_PROJECT_ID optional: pass --project (if not using gcloud default)
#   GCP_RSYNC_IAP  set to 1 to add --tunnel-through-iap to gcloud ssh/scp
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GCP_VM_NAME="${GCP_VM_NAME:-todc-ent-applications}"
GCP_ZONE="${GCP_ZONE:-us-west2-a}"
GCP_REMOTE_DIR="${GCP_REMOTE_DIR:-/opt/doordash-bot}"

RUN_INSTALL=0
RUN_VERIFY=0
RSYNC_DELETE=0
for arg in "$@"; do
  case "$arg" in
    --install) RUN_INSTALL=1 ;;
    --verify)  RUN_VERIFY=1 ;;
    --delete)  RSYNC_DELETE=1 ;;
    -h|--help)
      grep '^#' "$0" | head -n 28 | sed 's/^# \?//'
      exit 0
      ;;
  esac
done

if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install" >&2
  exit 1
fi

# Rsync runs: deploy/gce-rsync-rsh.sh INSTANCE rsync --server ...
# Wrapper runs: gcloud compute ssh INSTANCE --zone=... -- rsync ... (required "--").
RSYNC_RSH="${SCRIPT_DIR}/deploy/gce-rsync-rsh.sh"
if [[ ! -x "$RSYNC_RSH" ]]; then
  chmod +x "$RSYNC_RSH"
fi
export GCP_ZONE GCP_VM_NAME GCP_PROJECT_ID GCP_RSYNC_IAP

SSH_ARR=(gcloud compute ssh "$GCP_VM_NAME" --zone="$GCP_ZONE")
if [[ -n "${GCP_PROJECT_ID:-}" ]]; then
  SSH_ARR+=(--project="${GCP_PROJECT_ID}")
fi
if [[ "${GCP_RSYNC_IAP:-0}" == "1" ]]; then
  SSH_ARR+=(--tunnel-through-iap)
fi

echo "=============================================="
echo " Deploy to GCE: $GCP_VM_NAME ($GCP_ZONE)"
echo " Remote dir:    $GCP_REMOTE_DIR"
if [[ "$RSYNC_DELETE" == 1 ]]; then
  echo " Mode:          rsync --delete (removes extra files on VM)"
else
  echo " Mode:          rsync incremental (safe: keeps extra files e.g. .env on VM)"
fi
echo "=============================================="

echo ""
echo ">>> Ensuring remote directory exists (sudo may prompt once)..."
remote() {
  "${SSH_ARR[@]}" --command "$1"
}
remote "sudo mkdir -p '${GCP_REMOTE_DIR}' && sudo chown -R \"\$(whoami):\" '${GCP_REMOTE_DIR}'"

# rsync uses deploy/gce-rsync-rsh.sh as transport
RSYNC_EXCLUDES=(
  --exclude '.git/'
  --exclude '.venv/'
  --exclude '.env'
  --exclude '.env.*'
  --exclude 'downloads/'
  --exclude 'logs/'
  --exclude '__pycache__/'
  --exclude '*.pyc'
  --exclude '.DS_Store'
  --exclude 'todc-marketing-*.json'
  --exclude '*credentials*.json'
  --exclude '*service-account*.json'
)

RSYNC_OPTS=(-avz)
if [[ "$RSYNC_DELETE" == 1 ]]; then
  RSYNC_OPTS+=(--delete)
fi

echo ""
echo ">>> Syncing files (rsync)..."
rsync "${RSYNC_OPTS[@]}" \
  "${RSYNC_EXCLUDES[@]}" \
  -e "$RSYNC_RSH" \
  ./ "${GCP_VM_NAME}:${GCP_REMOTE_DIR}/"

if [[ "$RUN_INSTALL" == 1 ]]; then
  echo ""
  echo ">>> Installing Python dependencies on VM..."
  remote "set -e; cd '${GCP_REMOTE_DIR}' && if [[ -d .venv ]]; then source .venv/bin/activate; fi && pip install -r requirements.txt"
fi

if [[ "$RUN_VERIFY" == 1 ]]; then
  echo ""
  echo ">>> Running deploy/04-verify.sh on VM..."
  remote "set -e; cd '${GCP_REMOTE_DIR}' && bash deploy/04-verify.sh"
fi

echo ""
echo "Done. Code synced to ${GCP_VM_NAME}:${GCP_REMOTE_DIR}"
echo "Optional: SSH and run — cd ${GCP_REMOTE_DIR} && source .venv/bin/activate && python main.py"
