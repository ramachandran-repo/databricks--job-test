-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Databricks notebook source
-- MAGIC from datetime import datetime
-- MAGIC import os, re, json
-- MAGIC
-- MAGIC current_date = datetime.now().strftime("%Y-%m-%d")
-- MAGIC dbutils.widgets.text("date", current_date, "Process Date (YYYY-MM-DD)")
-- MAGIC date_val = dbutils.widgets.get("date").strip()
-- MAGIC if date_val == "":
-- MAGIC     date_val = current_date
-- MAGIC elif not re.match(r"^\d{4}-\d{2}-\d{2}$", date_val):
-- MAGIC     raise ValueError(f"Invalid date format: {date_val}")
-- MAGIC
-- MAGIC IS_JOB = "DATABRICKS_JOB_ID" in os.environ
-- MAGIC stage = "1"  # for now, fake stage for testing
-- MAGIC spark.conf.set("date", date_val)
-- MAGIC spark.conf.set("is_job", str(IS_JOB))
-- MAGIC spark.conf.set("stage", stage)
-- MAGIC print(f"DATE={date_val}, IS_JOB={IS_JOB}, STAGE={stage}")
-- MAGIC

-- COMMAND ----------

-- 1 demo
BEGIN
  IF (('${stage}' = '1') OR ('${is_job}' = 'False' AND ('${stage}' = '') THEN
    SELECT 'stage1' as stage, '${date}' AS run_date, current_timestamp() AS ts;
  END IF;
END;


-- COMMAND ----------

select current_timestamp() as test;

-- COMMAND ----------

-- 2 demo
BEGIN
  IF (('${stage}' = '2') OR ('${is_job}' = 'False' AND ('${stage}' = '') THEN
    SELECT 'stage2' as stage, '${date}' AS run_date, current_timestamp() AS ts;
  END IF;
END;


-- COMMAND ----------

select 'testing query' as test