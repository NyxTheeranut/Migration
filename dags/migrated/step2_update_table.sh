#!/bin/bash
# Step 2: replace the changed sales in the fact table, then drop the temp table.
set -euo pipefail
source "$(dirname "$0")/db_env.sh"

echo ">> Job [START] at: $(date)."

# One transaction: if the INSERT fails, the DELETE is rolled back too,
# so the fact table never loses rows halfway through.
#
# Delete by SALE_KEY only (it is unique per sale). The old version also required
# SALE_DATE to be in the temp table; when a sale's date was corrected, its old
# row survived and the sale ended up in the table twice.
query="
BEGIN;

DELETE FROM ${database}..FCT_SALES_DAILY
WHERE SALE_KEY IN (SELECT SALE_KEY FROM ${database}..ZTMP_FCT_SALES_DAILY);

INSERT INTO ${database}..FCT_SALES_DAILY
SELECT * FROM ${database}..ZTMP_FCT_SALES_DAILY;

COMMIT;

DROP TABLE ${database}..ZTMP_FCT_SALES_DAILY IF EXISTS;
"

echo "$query"
run_sql "$query"

echo ">> Job [FINISH] at: $(date)."
