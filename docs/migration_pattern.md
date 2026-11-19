# Migration pattern: Airflow 1.x to 3.x

The same steps apply to every DAG in this kind of migration.

## 1. Imports

| Airflow 1.x | Airflow 3.x |
|---|---|
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.bash_operator import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.dummy_operator import DummyOperator` | removed (it was imported but never used) |

In Airflow 3, DAG authors import from `airflow.sdk`, and the common operators
moved into the `apache-airflow-providers-standard` package.

## 2. The DAG object

**Before:**
```python
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="30 5 * * *")
```

**After:**
```python
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=CronDataIntervalTimetable(cron="30 5 * * *", timezone="Asia/Bangkok"),
    catchup=False,
)
```

- `schedule_interval` no longer exists in Airflow 3: use `schedule`.
- A cron string alone runs in Airflow's default timezone (UTC unless the
  server is configured otherwise). The timetable makes "05:30" mean 05:30 in
  Bangkok on any server, with the same data-interval behaviour as before.
- `catchup=False`: if Airflow was down for a few days, run only the latest day.
  The query already reloads 15 days, so missed days are covered anyway.

### The `default_args` mistake

The first migrated version had `params=default_args`. It does not raise an
error, which is why it is easy to miss: Airflow stores the dictionary as DAG
params (values for templates) instead of copying it onto each task. Loaded in
Airflow 3.3.2:

```
params=default_args       ->  step1_load_data: retries=0
default_args=default_args ->  step1_load_data: retries=2, step2_update_table: retries=2
```

## 3. Dependencies

```python
step2.set_upstream(step1)    # before
step1 >> step2               # after: reads left to right, same meaning
```

## 4. Credentials

**Before:** the DAG read the password and passed it to each script as an
argument (`step1_load_data.sh $ip $port $username $password $database`), so it
showed up in the task's command line.

**After:** the DAG passes nothing. Both scripts source `db_env.sh`, which reads
the connection from `config/db.ini` (outside git) and decodes the password:

```bash
read -r ip port username enc_password database < <(python3 "${UTIL_DIR}/get_db_config.py" "${DB_CONF}")
password=$(sh "${UTIL_DIR}/decode.sh" "${enc_password}")
```

Base64 hides the password from a quick glance; it is not encryption. The file
is protected with `chmod 600`. A secrets manager would be the next step.

## 5. Alerts

| Before | After |
|---|---|
| Discord webhook | Microsoft Teams webhook |
| A message for every task success and failure | A message for every task failure (after retries), one "done" message per run |
| Webhook URL inside the helper | Webhook URL from an environment variable |

## 6. The SQL: temp table, then delete and insert

```sql
-- Step 1: the changed rows
CREATE TABLE DB..ZTMP_FCT_SALES_DAILY AS
SELECT ... FROM SOURCEDB.FCT_SALES_DAILY
WHERE UPD_DT >= TO_DATE(SYSDATE) - 15 OR INS_DT >= TO_DATE(SYSDATE) - 15
DISTRIBUTE ON (SALE_KEY, CUSTOMER_KEY, PRODUCT_KEY);

-- Step 2: replace them, all or nothing
BEGIN;
DELETE FROM DB..FCT_SALES_DAILY
WHERE SALE_KEY IN (SELECT SALE_KEY FROM DB..ZTMP_FCT_SALES_DAILY);
INSERT INTO DB..FCT_SALES_DAILY SELECT * FROM DB..ZTMP_FCT_SALES_DAILY;
COMMIT;
DROP TABLE DB..ZTMP_FCT_SALES_DAILY IF EXISTS;
```

### Why the date condition was removed from the DELETE

The old DELETE was:

```sql
WHERE SALE_KEY  IN (SELECT SALE_KEY  FROM tmp)
  AND SALE_DATE IN (SELECT SALE_DATE FROM tmp)
```

Example: sale 1 was stored on Jan 1, then the source corrected it to Jan 3.

| | Fact table after the load |
|---|---|
| Old DELETE | sale 1 on Jan 1 **and** sale 1 on Jan 3 (counted twice) |
| New DELETE | sale 1 on Jan 3 only |

The old row's date (Jan 1) is not in the temp table, so the old DELETE kept it.
`SALE_KEY` alone identifies a sale, so it is the right thing to match on.

### Why one transaction

Without `BEGIN ... COMMIT`, each statement commits on its own. If the INSERT
failed after the DELETE succeeded, those sales would be missing from the fact
table until someone noticed. Inside a transaction, a failure rolls the DELETE
back too, and the retry starts from a clean state.

### Why NUMERIC for money

`DOUBLE PRECISION` is a binary float: `0.1 + 0.2` is `0.30000000000000004`.
Summed over millions of rows, totals drift away from the source system.
`NUMERIC(18,2)` stores amounts exactly.

### Why DISTRIBUTE ON

Netezza spreads rows across its data slices by the distribution key. The temp
table uses the same key as the fact table, so the `INSERT ... SELECT` moves
each row to a slice it is already on, with no redistribution across the
network. A good key has many distinct values (no skew) and matches how the
table is joined.
