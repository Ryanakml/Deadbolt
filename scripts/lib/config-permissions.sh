#!/usr/bin/env bash
# Shared validation for staging configuration that contains deployment secrets.

validate_staging_config_permissions() {
  local config_file="$1"
  local config_group="${DEADBOLT_CONFIG_GROUP:-deadbolt-deploy}"
  local mode owner group dir_mode dir_owner dir_group

  mode=$(stat -c '%a' "$config_file" 2>/dev/null || stat -f '%Lp' "$config_file")
  mode="${mode#0}"
  if [[ ! "$mode" =~ ^[0-7]{3}$ ]] || [[ "${mode: -1}" != "0" ]] || { [[ "${mode: -2:1}" != "0" ]] && [[ "${mode: -2:1}" != "4" ]]; }; then
    echo "SECURITY VIOLATION: Configuration file $config_file must not grant world access or group write access (got $mode)." >&2
    echo "Remediation: use root:${config_group} with mode 640 (or 600 only for a root-only, non-deployment file)." >&2
    return 1
  fi

  # The production secret path is deliberately stricter than local/test overrides.
  # A deploy user needs group read plus parent-directory traversal, but never write.
  if [[ "$config_file" == /etc/deadbolt/* ]]; then
    owner=$(stat -c '%U' "$config_file" 2>/dev/null || stat -f '%Su' "$config_file")
    group=$(stat -c '%G' "$config_file" 2>/dev/null || stat -f '%Sg' "$config_file")
    dir_mode=$(stat -c '%a' /etc/deadbolt 2>/dev/null || stat -f '%Lp' /etc/deadbolt)
    dir_mode="${dir_mode#0}"
    dir_owner=$(stat -c '%U' /etc/deadbolt 2>/dev/null || stat -f '%Su' /etc/deadbolt)
    dir_group=$(stat -c '%G' /etc/deadbolt 2>/dev/null || stat -f '%Sg' /etc/deadbolt)
    if [[ "$owner" != root || "$group" != "$config_group" || "$mode" != 640 || "$dir_owner" != root || "$dir_group" != "$config_group" || "$dir_mode" != 750 ]]; then
      echo "SECURITY VIOLATION: $config_file must be root:${config_group} 640 and /etc/deadbolt must be root:${config_group} 750." >&2
      return 1
    fi
  fi
}
