#!/bin/bash
# Step 1: copy the last 15 days of new or changed sales into a temp table.
set -euo pipefail
source "$(dirname "$0")/db_env.sh"

echo ">> Job [START] at: $(date)."

query="
DROP TABLE ${database}..ZTMP_FCT_SALES_DAILY IF EXISTS;

CREATE TABLE ${database}..ZTMP_FCT_SALES_DAILY AS
SELECT
  SALE_KEY
, SALE_ID
, SALE_DATE
, CUSTOMER_KEY
, CUSTOMER_ID
, PRODUCT_KEY
, PRODUCT_ID
, CHANNEL_CD
, REGION_CD
, CAST(SALE_AMT AS NUMERIC(18,2)) AS SALE_AMT   -- money stays exact (no float)
, CAST(DISC_AMT AS NUMERIC(18,2)) AS DISC_AMT
, CAST(NET_AMT  AS NUMERIC(18,2)) AS NET_AMT
, STATUS_CD
, INS_DT
, UPD_DT
, CAST(TO_CHAR(NOW(), 'YYYYMMDD') AS INT) AS LOADDATE
FROM SOURCEDB.FCT_SALES_DAILY
WHERE (UPD_DT >= TO_DATE(SYSDATE) - 15 OR INS_DT >= TO_DATE(SYSDATE) - 15)
DISTRIBUTE ON (SALE_KEY, CUSTOMER_KEY, PRODUCT_KEY);   -- same as the fact table, so the INSERT stays on each node
"

echo "$query"
run_sql "$query"

echo ">> Job [FINISH] at: $(date)."
