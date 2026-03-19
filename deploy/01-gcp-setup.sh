#!/bin/bash
# =============================================================================
# Step 1: GCP Project Setup — Run this from your LOCAL machine (laptop)
# Creates project resources: APIs, service account, secrets, firewall, VM
# =============================================================================
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURE THESE VALUES
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID env var}"
REGION="us-central1"
ZONE="us-central1-a"
VM_NAME="doordash-bot"
MACHINE_TYPE="e2-standard-4"     # 4 vCPU, 16 GB RAM
DISK_SIZE="50GB"
SERVICE_ACCOUNT_NAME="doordash-automation"
SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "============================================"
echo " GCP Setup for DoorDash Automation"
echo " Project: $PROJECT_ID"
echo " Zone:    $ZONE"
echo " VM:      $VM_NAME ($MACHINE_TYPE)"
echo "============================================"

# ──────────────────────────────────────────────────────────────────────────────
# 1. Set project & enable APIs
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Enabling APIs..."
gcloud config set project "$PROJECT_ID"

gcloud services enable \
    compute.googleapis.com \
    cloudscheduler.googleapis.com \
    sheets.googleapis.com \
    drive.googleapis.com \
    secretmanager.googleapis.com \
    logging.googleapis.com \
    cloudresourcemanager.googleapis.com

echo "    APIs enabled."

# ──────────────────────────────────────────────────────────────────────────────
# 2. Create service account (skip if exists)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Creating service account: $SA_EMAIL"
if gcloud iam service-accounts describe "$SA_EMAIL" &>/dev/null; then
    echo "    Service account already exists."
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="DoorDash Automation Bot"
    echo "    Created."
fi

# Grant roles
for ROLE in roles/logging.logWriter roles/secretmanager.secretAccessor roles/compute.instanceAdmin.v1; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="$ROLE" \
        --quiet
done
echo "    Roles assigned."

# ──────────────────────────────────────────────────────────────────────────────
# 3. Store secrets (interactive — prompts for values)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Storing secrets in Secret Manager..."
echo "    (Skip any secret that already exists by pressing Ctrl+C then re-run)"

store_secret() {
    local name="$1"
    local prompt="$2"
    if gcloud secrets describe "$name" --project="$PROJECT_ID" &>/dev/null; then
        echo "    Secret '$name' already exists — skipping."
    else
        read -sp "    Enter $prompt: " value
        echo ""
        echo -n "$value" | gcloud secrets create "$name" --data-file=- --project="$PROJECT_ID"
        echo "    Stored '$name'."
    fi
}

store_secret "DOORDASH_PASSWORD"   "DoorDash password"
store_secret "BROWSER_USE_API_KEY" "Browser Use API key"
store_secret "SLACK_WEBHOOK_URL"   "Slack webhook URL"

# ──────────────────────────────────────────────────────────────────────────────
# 4. Firewall rules
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Configuring firewall rules..."

# Block Chrome CDP from external access (CRITICAL — CDP is unauthenticated)
if gcloud compute firewall-rules describe deny-cdp-external &>/dev/null 2>&1; then
    echo "    Firewall 'deny-cdp-external' already exists."
else
    gcloud compute firewall-rules create deny-cdp-external \
        --direction=INGRESS \
        --action=DENY \
        --rules=tcp:9222 \
        --source-ranges=0.0.0.0/0 \
        --target-tags=doordash-bot \
        --priority=100 \
        --description="Block external access to Chrome CDP port"
    echo "    Created 'deny-cdp-external'."
fi

echo "    Firewall configured."

# ──────────────────────────────────────────────────────────────────────────────
# 5. Create GCE VM
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Creating GCE VM: $VM_NAME..."

if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" &>/dev/null 2>&1; then
    echo "    VM '$VM_NAME' already exists — skipping creation."
else
    gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size="$DISK_SIZE" \
        --boot-disk-type=pd-balanced \
        --service-account="$SA_EMAIL" \
        --scopes=https://www.googleapis.com/auth/cloud-platform \
        --tags=doordash-bot \
        --metadata=enable-oslogin=true
    echo "    VM created."
fi

echo ""
echo "============================================"
echo " GCP Setup Complete!"
echo ""
echo " Next steps:"
echo "   1. SSH into the VM:"
echo "      gcloud compute ssh $VM_NAME --zone=$ZONE"
echo ""
echo "   2. Run the VM setup script on the VM:"
echo "      bash deploy/02-vm-setup.sh"
echo "============================================"
