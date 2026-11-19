#!/bin/bash
# Shared by both steps: reads the database connection from config/db.ini
# (never from the DAG and never from the command line of the DAG task).
#
# Settings you can override with environment variables:
#   DB_CONF  section of config/db.ini to use (default: dwh_prod)
#   NZ_BIN   folder of the Netezza client tools (default: IBM's install path)

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UTIL_DIR="${REPO_DIR}/utils"
DB_CONF="${DB_CONF:-dwh_prod}"
NZ_BIN="${NZ_BIN:-/usr/local/nz/bin64}"

read -r ip port username enc_password database < <(python3 "${UTIL_DIR}/get_db_config.py" "${DB_CONF}")
password=$(sh "${UTIL_DIR}/decode.sh" "${enc_password}")

# Run one block of SQL against the warehouse.
run_sql() {
    "${NZ_BIN}/nzodbcsql" -h "$ip" -d "$database" -u "$username" -p "$password" -q "$1"
}
