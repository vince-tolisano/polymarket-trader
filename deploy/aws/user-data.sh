#!/usr/bin/env bash
#
# EC2 user-data bootstrap for the containerized live-trader (Amazon Linux 2023,
# eu-west-1 / Dublin). Runs once at first boot as root via cloud-init.
#
# What it does:
#   1. Installs Docker + the compose plugin + git + AWS CLI.
#   2. Adds swap (Rust release builds OOM on small instances).
#   3. Clones this repo and builds the image ON the instance (arch auto-matches,
#      so a Graviton/arm64 box just works).
#   4. Pulls POLY_PRIVATE_KEY from SSM Parameter Store (SecureString) into .env.
#   5. Installs a systemd unit so the bot starts on boot and stops GRACEFULLY
#      (SIGINT + 30s) on shutdown, so it can settle/cancel orders and flush CSVs.
#
# Prereqs (see README.md): an instance IAM role allowing ssm:GetParameter (+
# kms:Decrypt) on the params below, and the key already stored in SSM.
#
# SAFETY: docker-compose.yml defaults to --dry-run. Going live is a deliberate
# edit (see README) — this script does NOT place real orders on its own.
set -euxo pipefail

# ---- Config (edit to taste) ---------------------------------------------
AWS_REGION="eu-west-1"
SSM_KEY_PARAM="/polymarket-trader/POLY_PRIVATE_KEY"
# Optional: SSM SecureString holding a GitHub token for a PRIVATE repo clone.
# Leave as-is; if the param doesn't exist the script falls back to anon clone.
SSM_GH_TOKEN_PARAM="/polymarket-trader/GITHUB_TOKEN"
REPO_URL="https://github.com/vince-tolisano/polymarket-trader.git"
BRANCH="Data"
APP_DIR="/opt/polymarket-trader"
TZ_VALUE="America/New_York"

export AWS_DEFAULT_REGION="$AWS_REGION"

# ---- Packages ------------------------------------------------------------
dnf -y update
dnf -y install docker git unzip
systemctl enable --now docker

# AWS CLI v2 (AL2023 usually ships it; install if missing).
if ! command -v aws >/dev/null 2>&1; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install
fi

# Docker Compose v2 plugin (not bundled with AL2023's docker package).
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# ---- Swap (headroom for the Rust build) ---------------------------------
if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=2048
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# ---- Fetch the repo ------------------------------------------------------
mkdir -p "$APP_DIR"
GH_TOKEN="$(aws ssm get-parameter --name "$SSM_GH_TOKEN_PARAM" --with-decryption \
  --query 'Parameter.Value' --output text 2>/dev/null || true)"
if [ -n "$GH_TOKEN" ] && [ "$GH_TOKEN" != "None" ]; then
  CLONE_URL="https://oauth2:${GH_TOKEN}@github.com/vince-tolisano/polymarket-trader.git"
else
  CLONE_URL="$REPO_URL"
fi
git clone -b "$BRANCH" "$CLONE_URL" "$APP_DIR"

# ---- Secret + .env -------------------------------------------------------
POLY_KEY="$(aws ssm get-parameter --name "$SSM_KEY_PARAM" --with-decryption \
  --query 'Parameter.Value' --output text)"
umask 077
cat > "$APP_DIR/.env" <<EOF
POLY_PRIVATE_KEY=${POLY_KEY}
TZ=${TZ_VALUE}
EOF
unset POLY_KEY GH_TOKEN

# ---- Build the image -----------------------------------------------------
cd "$APP_DIR"
docker build -t polymarket-live-trader .

# ---- systemd unit: boot start + graceful SIGINT stop ---------------------
cat > /etc/systemd/system/polymarket-trader.service <<EOF
[Unit]
Description=Polymarket live-trader (docker compose)
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${APP_DIR}
# stop -t 30 sends STOPSIGNAL (SIGINT) and waits, matching stop_grace_period.
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose stop -t 30
TimeoutStopSec=45

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now polymarket-trader.service
