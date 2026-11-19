# BEFORE the migration (Airflow 1.x). Kept as found; see docs/migration_pattern.md.
# DATABASE CONFIG (passed as arguments from DAG)
ip=$1
port=$2
userlogin=$3
password=$4
database=$5

# date variables
run_date=$(date +%Y%m%d)
past_15_days=$(date -d "$(date +%Y-%m-%d) -15 days" +%Y%m%d)

nl="-----------------------------------------------------------"

echo "$nl"
echo ">> Job [START] at: $(date)."

query="
-----------------------------------------------------------
/* DROP TEMP TABLE IF EXISTS */
-----------------------------------------------------------
DROP TABLE DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY IF EXISTS;

-----------------------------------------------------------
/* LOAD UPDATED RECORDS FROM SOURCE (LAST 15 DAYS) */
-----------------------------------------------------------
CREATE TABLE DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY AS

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
, CAST(SALE_AMT    AS DOUBLE PRECISION) AS SALE_AMT
, CAST(DISC_AMT    AS DOUBLE PRECISION) AS DISC_AMT
, CAST(NET_AMT     AS DOUBLE PRECISION) AS NET_AMT
, STATUS_CD
, INS_DT
, UPD_DT
, CAST(TO_CHAR(NOW(), 'YYYYMMDD') AS INT) AS LOADDATE

FROM SOURCEDB.FCT_SALES_DAILY

WHERE (UPD_DT >= TO_DATE(SYSDATE) - 15 OR INS_DT >= TO_DATE(SYSDATE) - 15)

DISTRIBUTE ON (SALE_KEY, CUSTOMER_KEY, PRODUCT_KEY)
;
-----------------------------------------------------------
"
echo "$query"
/usr/local/nz/bin64/nzodbcsql -h $ip -d $database -u $userlogin -p $password -q "$query"

echo "$nl"
echo ">> Job [FINISH] at: $(date)."
