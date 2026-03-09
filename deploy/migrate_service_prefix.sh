#!/usr/bin/env bash
set -euo pipefail

# Migrate systemd service prefix, e.g.:
#   vpulse@writer -> virtuals-launch-hunter@writer
#
# Usage:
#   bash deploy/migrate_service_prefix.sh
#
# Overrides:
#   OLD_PREFIX=vpulse NEW_PREFIX=virtuals-launch-hunter bash deploy/migrate_service_prefix.sh
#   SERVICE_ROLES="writer realtime backfill" bash deploy/migrate_service_prefix.sh

OLD_PREFIX="${OLD_PREFIX:-vpulse}"
NEW_PREFIX="${NEW_PREFIX:-virtuals-launch-hunter}"
SERVICE_ROLES="${SERVICE_ROLES:-writer realtime backfill}"
LOG_TAG="${LOG_TAG:-virtuals-launch-hunter-migrate}"

log() {
  echo "[$LOG_TAG] $*"
}

die() {
  echo "[$LOG_TAG][ERROR] $*" >&2
  exit 1
}

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

resolve_unit_file() {
  local prefix="$1"
  local path=""
  if [[ -f "/etc/systemd/system/${prefix}@.service" ]]; then
    path="/etc/systemd/system/${prefix}@.service"
  elif [[ -f "/lib/systemd/system/${prefix}@.service" ]]; then
    path="/lib/systemd/system/${prefix}@.service"
  elif [[ -f "/usr/lib/systemd/system/${prefix}@.service" ]]; then
    path="/usr/lib/systemd/system/${prefix}@.service"
  fi
  echo "$path"
}

main() {
  [[ "$OLD_PREFIX" != "$NEW_PREFIX" ]] || die "OLD_PREFIX and NEW_PREFIX cannot be the same"

  local old_unit_file
  old_unit_file="$(resolve_unit_file "$OLD_PREFIX")"
  [[ -n "$old_unit_file" ]] || die "cannot find unit file for prefix: $OLD_PREFIX"

  local new_unit_file="/etc/systemd/system/${NEW_PREFIX}@.service"
  local old_units=()
  local new_units=()
  local role
  for role in $SERVICE_ROLES; do
    old_units+=("${OLD_PREFIX}@${role}")
    new_units+=("${NEW_PREFIX}@${role}")
  done

  log "Copying unit template: $old_unit_file -> $new_unit_file"
  run_root cp "$old_unit_file" "$new_unit_file"
  run_root chmod 644 "$new_unit_file"

  log "Reloading systemd daemon"
  run_root systemctl daemon-reload

  log "Stopping old services: ${old_units[*]}"
  run_root systemctl stop "${old_units[@]}" || true

  log "Disabling old services: ${old_units[*]}"
  run_root systemctl disable "${old_units[@]}" || true

  log "Enabling and starting new services: ${new_units[*]}"
  run_root systemctl enable --now "${new_units[@]}"

  log "Migration complete. New service status:"
  local unit
  for unit in "${new_units[@]}"; do
    run_root systemctl --no-pager --full status "$unit" || true
  done
}

main "$@"
