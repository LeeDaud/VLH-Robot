#!/usr/bin/env bash
set -euo pipefail

# One-click server update script.
# Default targets are compatible with this project deployment guide.
#
# Usage:
#   bash deploy/update_server_oneclick.sh
#
# Common overrides:
#   BRANCH=main SERVICE_PREFIX=virtuals-launch-hunter WEB_DIR=/var/www/virtuals-launch-hunter bash deploy/update_server_oneclick.sh
#   FRONTEND_API_BASE=/api bash deploy/update_server_oneclick.sh
#   SKIP_PIP=1 bash deploy/update_server_oneclick.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="${APP_DIR:-$ROOT_DIR}"
WEB_DIR="${WEB_DIR:-}"
SERVICE_PREFIX="${SERVICE_PREFIX:-}"
SERVICE_ROLES="${SERVICE_ROLES:-writer realtime backfill}"
REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
FRONTEND_API_BASE="${FRONTEND_API_BASE:-}"
SKIP_PIP="${SKIP_PIP:-0}"
SKIP_FRONTEND="${SKIP_FRONTEND:-0}"
SKIP_NGINX="${SKIP_NGINX:-0}"
LOG_TAG="${LOG_TAG:-virtuals-launch-hunter-update}"

log() {
  echo "[$LOG_TAG] $*"
}

warn() {
  echo "[$LOG_TAG][WARN] $*" >&2
}

die() {
  echo "[$LOG_TAG][ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

service_prefix_exists() {
  local prefix="$1"
  systemctl is-enabled "${prefix}@writer" >/dev/null 2>&1 && return 0
  systemctl is-active "${prefix}@writer" >/dev/null 2>&1 && return 0
  systemctl list-unit-files --type=service --no-legend "${prefix}@.service" 2>/dev/null \
    | grep -q "${prefix}@.service" && return 0
  return 1
}

resolve_service_prefix() {
  if [[ -n "$SERVICE_PREFIX" ]]; then
    echo "$SERVICE_PREFIX"
    return 0
  fi
  if service_prefix_exists "virtuals-launch-hunter"; then
    echo "virtuals-launch-hunter"
    return 0
  fi
  if service_prefix_exists "vpulse"; then
    echo "vpulse"
    return 0
  fi
  echo "virtuals-launch-hunter"
}

resolve_web_dir() {
  if [[ -n "$WEB_DIR" ]]; then
    echo "$WEB_DIR"
    return 0
  fi
  if [[ -d /var/www/virtuals-launch-hunter ]]; then
    echo "/var/www/virtuals-launch-hunter"
    return 0
  fi
  if [[ -d /var/www/vpulse ]]; then
    echo "/var/www/vpulse"
    return 0
  fi
  echo "/var/www/virtuals-launch-hunter"
}

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

resolve_branch() {
  if [[ -n "$BRANCH" ]]; then
    echo "$BRANCH"
    return 0
  fi
  local current
  current="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [[ -z "$current" || "$current" == "HEAD" ]]; then
    die "cannot detect current branch automatically, please set BRANCH=<branch>"
  fi
  echo "$current"
}

extract_api_base() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  sed -n 's/.*<meta name="vpulse-api-base" content="\([^"]*\)".*/\1/p' "$file" | head -n1
}

patch_dashboard_api_base() {
  local file="$1"
  local api_base="$2"
  "$PYTHON_BIN" - "$file" "$api_base" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
api_base = sys.argv[2]
if '"' in api_base:
    raise SystemExit('FRONTEND_API_BASE cannot contain double quote')

text = path.read_text(encoding='utf-8')
pattern = r'<meta name="vpulse-api-base" content="[^"]*"\s*/?>'
replacement = f'<meta name="vpulse-api-base" content="{api_base}" />'
if not re.search(pattern, text):
    raise SystemExit('vpulse-api-base meta tag not found in dashboard.html')
text = re.sub(pattern, replacement, text, count=1)
path.write_text(text, encoding='utf-8')
PY
}

update_code() {
  cd "$APP_DIR"
  [[ -d .git ]] || die "$APP_DIR is not a git repository"
  local target_branch
  target_branch="$(resolve_branch)"
  log "Updating code from $REMOTE/$target_branch"
  if ! git diff --quiet || ! git diff --cached --quiet; then
    warn "local tracked changes detected, using git pull --rebase --autostash"
  fi
  git fetch "$REMOTE" "$target_branch"
  git pull --rebase --autostash "$REMOTE" "$target_branch"
}

update_python_deps() {
  if [[ "$SKIP_PIP" == "1" ]]; then
    log "Skipping Python dependency update (SKIP_PIP=1)"
    return 0
  fi
  cd "$APP_DIR"
  if [[ ! -d .venv ]]; then
    log "Creating virtual environment: $APP_DIR/.venv"
    "$PYTHON_BIN" -m venv .venv
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
}

publish_frontend() {
  if [[ "$SKIP_FRONTEND" == "1" ]]; then
    log "Skipping frontend publish (SKIP_FRONTEND=1)"
    return 0
  fi
  [[ -f "$APP_DIR/dashboard.html" ]] || die "missing $APP_DIR/dashboard.html"
  [[ -f "$APP_DIR/favicon-vpulse.svg" ]] || die "missing $APP_DIR/favicon-vpulse.svg"

  local api_base="$FRONTEND_API_BASE"
  if [[ -z "$api_base" ]]; then
    api_base="$(extract_api_base "$WEB_DIR/dashboard.html" || true)"
  fi

  local tmp_dashboard
  tmp_dashboard="$(mktemp)"
  cp "$APP_DIR/dashboard.html" "$tmp_dashboard"

  if [[ -n "$api_base" ]]; then
    patch_dashboard_api_base "$tmp_dashboard" "$api_base"
    log "Frontend API base set to: $api_base"
  fi

  run_root mkdir -p "$WEB_DIR"
  run_root cp "$tmp_dashboard" "$WEB_DIR/dashboard.html"
  run_root cp "$APP_DIR/favicon-vpulse.svg" "$WEB_DIR/favicon-vpulse.svg"
  if [[ -d "$APP_DIR/favicon" ]]; then
    run_root mkdir -p "$WEB_DIR/favicon"
    run_root cp -r "$APP_DIR/favicon/." "$WEB_DIR/favicon/"
    if [[ -f "$APP_DIR/favicon/favicon.ico" ]]; then
      run_root cp "$APP_DIR/favicon/favicon.ico" "$WEB_DIR/favicon.ico"
    fi
  fi
  rm -f "$tmp_dashboard"
}

restart_services() {
  local units=()
  local role
  for role in $SERVICE_ROLES; do
    units+=("${SERVICE_PREFIX}@${role}")
  done
  log "Restarting role services"
  run_root systemctl daemon-reload
  run_root systemctl restart "${units[@]}"
}

reload_nginx() {
  if [[ "$SKIP_NGINX" == "1" ]]; then
    log "Skipping nginx reload (SKIP_NGINX=1)"
    return 0
  fi
  if run_root systemctl is-active nginx >/dev/null 2>&1; then
    run_root nginx -t
    run_root systemctl reload nginx
    log "Nginx reloaded"
  else
    warn "nginx is not active, skipped reload"
  fi
}

main() {
  need_cmd git
  need_cmd "$PYTHON_BIN"
  WEB_DIR="$(resolve_web_dir)"
  SERVICE_PREFIX="$(resolve_service_prefix)"
  cd "$APP_DIR"
  update_code
  update_python_deps
  publish_frontend
  restart_services
  reload_nginx
  log "Update finished successfully"
}

main "$@"
