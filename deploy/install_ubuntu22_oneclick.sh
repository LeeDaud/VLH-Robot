#!/usr/bin/env bash
set -euo pipefail

# One-click installer for Ubuntu 22.04+
# Deployment mode: same domain + /api reverse proxy
#
# Example:
# DOMAIN=launch.licheng.website \
# LETSENCRYPT_EMAIL=you@example.com \
# WS_RPC_URL=wss://... \
# HTTP_RPC_URL=https://... \
# BACKFILL_HTTP_RPC_URL=https://... \
# bash deploy/install_ubuntu22_oneclick.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="${APP_DIR:-$ROOT_DIR}"
WEB_DIR="${WEB_DIR:-/var/www/virtuals-launch-hunter}"
SERVICE_PREFIX="${SERVICE_PREFIX:-virtuals-launch-hunter}"
DOMAIN="${DOMAIN:-launch.licheng.website}"
ENABLE_HTTPS="${ENABLE_HTTPS:-1}"

RUN_USER="${SERVICE_USER:-${SUDO_USER:-$USER}}"
RUN_GROUP="${SERVICE_GROUP:-$(id -gn "$RUN_USER")}"

CONFIG_FILE="$APP_DIR/config.json"
DASHBOARD_FILE="$APP_DIR/dashboard.html"
FAVICON_FILE="$APP_DIR/favicon-vpulse.svg"

log() {
  echo "[virtuals-launch-hunter-install] $*"
}

warn() {
  echo "[virtuals-launch-hunter-install][WARN] $*" >&2
}

die() {
  echo "[virtuals-launch-hunter-install][ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

ask_if_empty() {
  local var_name="$1"
  local prompt="$2"
  local value="${!var_name:-}"
  if [[ -n "$value" ]]; then
    return 0
  fi
  if [[ -t 0 ]]; then
    read -r -p "$prompt: " value
    export "$var_name=$value"
  fi
  if [[ -z "${!var_name:-}" ]]; then
    die "$var_name is required"
  fi
}

write_json_kv() {
  local file="$1"
  local key="$2"
  local value="$3"
  python3 - "$file" "$key" "$value" <<'PY'
import json
import sys
from pathlib import Path

file, key, value = sys.argv[1], sys.argv[2], sys.argv[3]
p = Path(file)
data = json.loads(p.read_text(encoding="utf-8"))
data[key] = value
p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
}

write_json_literal() {
  local file="$1"
  local key="$2"
  local literal="$3"
  python3 - "$file" "$key" "$literal" <<'PY'
import json
import sys
from pathlib import Path

file, key, literal = sys.argv[1], sys.argv[2], sys.argv[3]
p = Path(file)
data = json.loads(p.read_text(encoding="utf-8"))
data[key] = json.loads(literal)
p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
}

update_dashboard_api_base() {
  local file="$1"
  python3 - "$file" <<'PY'
import re
import sys
from pathlib import Path

file = Path(sys.argv[1])
text = file.read_text(encoding="utf-8")
pattern = r'<meta name="vpulse-api-base" content="[^"]*"\s*/?>'
replacement = '<meta name="vpulse-api-base" content="/api" />'

if re.search(pattern, text):
    text = re.sub(pattern, replacement, text, count=1)
else:
    marker = '<meta name="theme-color" content="#0b1426" />'
    if marker in text:
        text = text.replace(marker, marker + "\n  " + replacement, 1)
    else:
        raise SystemExit("failed to set vpulse-api-base meta tag")

file.write_text(text, encoding="utf-8")
PY
}

install_packages() {
  log "Installing system packages"
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 python3-venv python3-pip nginx certbot python3-certbot-nginx curl
}

install_python_deps() {
  log "Installing Python dependencies"
  cd "$APP_DIR"
  if [[ ! -d .venv ]]; then
    python3 -m venv .venv
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
}

prepare_config() {
  log "Preparing config.json"
  cd "$APP_DIR"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    [[ -f "$APP_DIR/config.example.json" ]] || die "config.example.json not found"
    cp "$APP_DIR/config.example.json" "$CONFIG_FILE"
  fi

  ask_if_empty "WS_RPC_URL" "Enter WS_RPC_URL"
  ask_if_empty "HTTP_RPC_URL" "Enter HTTP_RPC_URL"
  if [[ -z "${BACKFILL_HTTP_RPC_URL:-}" ]]; then
    export BACKFILL_HTTP_RPC_URL="$HTTP_RPC_URL"
  fi

  write_json_kv "$CONFIG_FILE" "WS_RPC_URL" "$WS_RPC_URL"
  write_json_kv "$CONFIG_FILE" "HTTP_RPC_URL" "$HTTP_RPC_URL"
  write_json_kv "$CONFIG_FILE" "BACKFILL_HTTP_RPC_URL" "$BACKFILL_HTTP_RPC_URL"
  write_json_kv "$CONFIG_FILE" "API_HOST" "127.0.0.1"
  write_json_kv "$CONFIG_FILE" "API_PORT" "8080"
  write_json_literal "$CONFIG_FILE" "CORS_ALLOW_ORIGINS" "[]"
}

publish_frontend() {
  log "Publishing frontend files to $WEB_DIR"
  [[ -f "$DASHBOARD_FILE" ]] || die "dashboard.html not found"
  [[ -f "$FAVICON_FILE" ]] || die "favicon-vpulse.svg not found"

  update_dashboard_api_base "$DASHBOARD_FILE"

  sudo mkdir -p "$WEB_DIR"
  sudo cp "$DASHBOARD_FILE" "$WEB_DIR/dashboard.html"
  sudo cp "$FAVICON_FILE" "$WEB_DIR/favicon-vpulse.svg"
  if [[ -d "$APP_DIR/favicon" ]]; then
    sudo mkdir -p "$WEB_DIR/favicon"
    sudo cp -r "$APP_DIR/favicon/." "$WEB_DIR/favicon/"
    if [[ -f "$APP_DIR/favicon/favicon.ico" ]]; then
      sudo cp "$APP_DIR/favicon/favicon.ico" "$WEB_DIR/favicon.ico"
    fi
  fi
}

install_systemd_service() {
  log "Installing systemd service units"
  local service_file="/etc/systemd/system/${SERVICE_PREFIX}@.service"

  sudo tee "$service_file" >/dev/null <<EOF
[Unit]
Description=Virtuals-Launch-Hunter role %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
Group=$RUN_GROUP
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/python $APP_DIR/virtuals_bot.py --config $APP_DIR/config.json --role %i
Restart=always
RestartSec=3
TimeoutStopSec=30
KillSignal=SIGINT
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

  sudo systemctl daemon-reload
  sudo systemctl enable --now \
    "${SERVICE_PREFIX}@writer" \
    "${SERVICE_PREFIX}@realtime" \
    "${SERVICE_PREFIX}@backfill"
}

install_nginx_site() {
  log "Installing nginx site config for domain: $DOMAIN"
  local site_available="/etc/nginx/sites-available/${SERVICE_PREFIX}.conf"
  local site_enabled="/etc/nginx/sites-enabled/${SERVICE_PREFIX}.conf"

  sudo tee "$site_available" >/dev/null <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    root $WEB_DIR;
    index dashboard.html;

    location = / {
        try_files /dashboard.html =404;
    }

    location = /dashboard.html {
        try_files /dashboard.html =404;
    }

    location = /favicon-vpulse.svg {
        try_files /favicon-vpulse.svg =404;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8080/;
        proxy_http_version 1.1;

        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    location / {
        try_files \$uri =404;
    }
}
EOF

  sudo rm -f /etc/nginx/sites-enabled/default
  sudo ln -sfn "$site_available" "$site_enabled"
  sudo nginx -t
  sudo systemctl reload nginx
}

setup_https() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    log "Skipping HTTPS setup (ENABLE_HTTPS=$ENABLE_HTTPS)"
    return 0
  fi

  if [[ -z "${LETSENCRYPT_EMAIL:-}" ]]; then
    warn "LETSENCRYPT_EMAIL is empty, skip certbot."
    warn "Run manually: sudo certbot --nginx -d $DOMAIN"
    return 0
  fi

  log "Applying HTTPS certificate via certbot"
  sudo certbot --nginx \
    --non-interactive \
    --agree-tos \
    --redirect \
    -m "$LETSENCRYPT_EMAIL" \
    -d "$DOMAIN" || {
      warn "certbot failed. You can retry manually later."
      return 0
    }
}

show_status() {
  log "Service status"
  sudo systemctl --no-pager --full status "${SERVICE_PREFIX}@writer" || true
  sudo systemctl --no-pager --full status "${SERVICE_PREFIX}@realtime" || true
  sudo systemctl --no-pager --full status "${SERVICE_PREFIX}@backfill" || true

  log "Health check (local API)"
  curl -fsS http://127.0.0.1:8080/health | head -c 600 || true
  echo

  log "Done. Open: https://$DOMAIN/"
  log "API: https://$DOMAIN/api/health"
}

main() {
  need_cmd python3
  need_cmd sudo
  [[ -f "$APP_DIR/virtuals_bot.py" ]] || die "virtuals_bot.py not found in APP_DIR=$APP_DIR"
  [[ -f "$APP_DIR/requirements.txt" ]] || die "requirements.txt not found in APP_DIR=$APP_DIR"

  install_packages
  install_python_deps
  prepare_config
  publish_frontend
  install_systemd_service
  install_nginx_site
  setup_https
  show_status
}

main "$@"
