# Deploy live-trader on AWS EC2 (eu-west-1 / Dublin)

A single always-on EC2 instance running the container via `docker compose`.
The image is built **on the instance**; the private key lives in **SSM Parameter
Store**; CSVs persist on the instance's EBS volume. All steps use region
`eu-west-1`.

> The bot places **real orders** when live. `docker-compose.yml` defaults to
> `--dry-run`; going live is a deliberate edit (last section). Confirm the
> deployment complies with Polymarket's terms and any law that applies to you.

## 1. Store the secret in SSM

```bash
aws ssm put-parameter --region eu-west-1 \
  --name /polymarket-trader/POLY_PRIVATE_KEY \
  --type SecureString --value 0xYOUR_PRIVATE_KEY

# Only if the GitHub repo is PRIVATE — store a token with repo read scope:
aws ssm put-parameter --region eu-west-1 \
  --name /polymarket-trader/GITHUB_TOKEN \
  --type SecureString --value ghp_xxx
```

## 2. IAM role for the instance

Create an IAM role (trusted by `ec2.amazonaws.com`), attach this policy, and
make an instance profile from it. Scope it to just these params:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": [
        "arn:aws:ssm:eu-west-1:ACCOUNT_ID:parameter/polymarket-trader/POLY_PRIVATE_KEY",
        "arn:aws:ssm:eu-west-1:ACCOUNT_ID:parameter/polymarket-trader/GITHUB_TOKEN"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "*",
      "Condition": { "StringEquals": { "kms:ViaService": "ssm.eu-west-1.amazonaws.com" } }
    }
  ]
}
```

(Attaching `AmazonSSMManagedInstanceCore` too lets you use Session Manager for a
keyless shell — handy, optional.)

## 3. Launch the instance

- **AMI:** Amazon Linux 2023.
- **Type:** `t4g.small` (Graviton/arm64, cheap) — the swap in user-data covers
  the build. Bump to `t4g.medium` if you want faster/roomier builds.
- **Region/AZ:** eu-west-1.
- **IAM instance profile:** the one from step 2.
- **Security group:** outbound all (needs HTTPS/WSS to Polymarket/Pyth/CEX).
  Inbound: none required (use SSM Session Manager), or SSH from your IP only.
- **User data:** paste `deploy/aws/user-data.sh`. Edit the config block at the
  top first if your repo URL / branch / timezone differ.
- **Storage:** default 8–20 GB gp3 is plenty for CSVs.

Cloud-init runs the script once on first boot (installs Docker, builds the
image, writes `.env` from SSM, starts the systemd unit).

## 4. Verify

```bash
# via SSM Session Manager or SSH
sudo systemctl status polymarket-trader
cd /opt/polymarket-trader
sudo docker compose ps
sudo docker compose logs -f          # watch it roll windows
ls -l data/                          # trade-<stamp>.csv appearing
tail -f /var/log/cloud-init-output.log   # if the build/bootstrap failed
```

It starts in **dry-run** (safe): full strategy, no wallet, no orders.

## 5. Go live

Edit the compose command to drop `--dry-run`, then restart:

```bash
cd /opt/polymarket-trader
sudo sed -i 's/\["--dry-run"\]/[]/' docker-compose.yml   # or edit by hand;
                                                          # add flags e.g. ["--notional","5","--min-target-dist","35"]
sudo systemctl restart polymarket-trader
sudo docker compose logs -f
```

Requires the funded Polygon wallet + one-time on-chain USDC/CTF approvals the
code doesn't do (see CLAUDE.md "Money/price conventions").

## Operations

- **Stop gracefully:** `sudo systemctl stop polymarket-trader` (SIGINT + 30s →
  settle/cancel orders, resolve final window, flush CSV). Prefer this over
  terminating the instance mid-window.
- **Update code:** `cd /opt/polymarket-trader && sudo git pull && sudo docker compose build && sudo systemctl restart polymarket-trader`.
- **Durable CSVs:** they live on EBS and survive stop/start but not instance
  termination. To keep them, snapshot the volume or sync out, e.g.
  `aws s3 sync data/ s3://your-bucket/live-trader/ --region eu-west-1` on a cron.
- **Rotate the key:** update the SSM param, re-run the `.env` write (or just
  re-launch), restart the service.
