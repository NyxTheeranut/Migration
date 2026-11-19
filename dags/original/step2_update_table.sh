# BEFORE the migration (Airflow 1.x). Kept as found; see docs/migration_pattern.md.
# DATABASE CONFIG (passed as arguments from DAG)
ip=$1
port=$2
userlogin=$3
password=$4
database=$5

nl="-----------------------------------------------------------"

echo "$nl"
echo ">> Job [START] at: $(date)."

query="
-----------------------------------------------------------
/* DELETE EXISTING RECORDS THAT OVERLAP WITH TEMP TABLE */
-----------------------------------------------------------
DELETE FROM DATAWAREHOUSE..FCT_SALES_DAILY
WHERE SALE_KEY IN (SELECT SALE_KEY FROM DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY)
  AND SALE_DATE IN (SELECT SALE_DATE FROM DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY);

-----------------------------------------------------------
/* INSERT UPDATED RECORDS FROM TEMP TABLE */
-----------------------------------------------------------
INSERT INTO DATAWAREHOUSE..FCT_SALES_DAILY
SELECT * FROM DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY;

-----------------------------------------------------------
/* CLEANUP TEMP TABLE */
-----------------------------------------------------------
DROP TABLE DATAWAREHOUSE..ZTMP_FCT_SALES_DAILY IF EXISTS;
-----------------------------------------------------------
"
echo "$query"
/usr/local/nz/bin64/nzodbcsql -h $ip -d $database -u $userlogin -p $password -q "$query"

echo "$nl"
echo ">> Job [FINISH] at: $(date)."
