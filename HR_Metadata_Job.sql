-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC # ------------------------------------------------------------------------------
-- MAGIC #   HR_Metadata_Job Notebook
-- MAGIC #   Source: Git Repo (/Repos/your-git-org/your-repo)
-- MAGIC #   Owner : Data Engineering Platform Team
-- MAGIC #   Purpose:
-- MAGIC #       Shared SQL-first notebook orchestrated via Databricks Workflows.
-- MAGIC #       Runs stage-labelled SQL cells in parallel across tasks.
-- MAGIC #       Handles both interactive/manual and scheduled job executions.
-- MAGIC # ------------------------------------------------------------------------------
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC import os, re, json
-- MAGIC
-- MAGIC # --- WIDGET: date -------------------------------------------------------------
-- MAGIC # Default = today's date. Used across manual + Job runs.
-- MAGIC current_date = datetime.now().strftime("%Y-%m-%d")
-- MAGIC dbutils.widgets.text("date", current_date, "Process Date (YYYY-MM-DD)")
-- MAGIC
-- MAGIC date_val = dbutils.widgets.get("date").strip()
-- MAGIC if date_val == "":
-- MAGIC     # user cleared it → fallback to widget default
-- MAGIC     date_val = current_date
-- MAGIC elif not re.match(r"^\d{4}-\d{2}-\d{2}$", date_val):
-- MAGIC     raise ValueError(f"Invalid date '{date_val}'. Expected YYYY-MM-DD format.")
-- MAGIC
-- MAGIC # --- JOB CONTEXT --------------------------------------------------------------
-- MAGIC IS_JOB = "DATABRICKS_JOB_ID" in os.environ
-- MAGIC task_key = ""
-- MAGIC try:
-- MAGIC     ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
-- MAGIC     task_key = (ctx.get("tags") or {}).get("taskKey") or ""
-- MAGIC except Exception:
-- MAGIC     pass
-- MAGIC
-- MAGIC m = re.search(r"(?:^|[_-])(?:stage)?(\d+)(?:$|[_-])", task_key, re.IGNORECASE)
-- MAGIC stage = m.group(1) if m else ""
-- MAGIC
-- MAGIC # --- PUBLISH TO SQL -----------------------------------------------------------
-- MAGIC
-- MAGIC is_job = "True" if IS_JOB else "False"
-- MAGIC stage = 1
-- MAGIC
-- MAGIC
-- MAGIC spark.sql("DECLARE OR REPLACE VARIABLE date string")
-- MAGIC spark.sql("DECLARE OR REPLACE VARIABLE is_job string")
-- MAGIC spark.sql("DECLARE OR REPLACE VARIABLE stage string")
-- MAGIC spark.sql(f"SET VAR date = '{date_val}'")
-- MAGIC spark.sql(f"SET VAR is_job = '{is_job}'")
-- MAGIC spark.sql(f"SET VAR stage = '{stage}'")
-- MAGIC
-- MAGIC print("─────────────────────────────────────────────")
-- MAGIC print(f"RUN MODE : {'JOB' if IS_JOB else 'MANUAL'}")
-- MAGIC print(f"DATE     : {date_val}")
-- MAGIC print(f"TASK KEY : {task_key or '(manual run)'}")
-- MAGIC print(f"STAGE    : {stage}")
-- MAGIC print(f"IS_JOB    : {IS_JOB}")
-- MAGIC print("─────────────────────────────────────────────")
-- MAGIC

-- COMMAND ----------

select date, is_job, stage

-- COMMAND ----------

-- 1 companies
BEGIN
  IF ((stage= '1') OR (is_job = 'False' AND stage = '')) THEN
    insert into dbtest.default.test values(1, "inserted");
  END IF;
END;


-- COMMAND ----------

-- 1 companies
BEGIN
  IF ((stage= '2') OR (is_job = 'False' AND stage = '')) THEN
    insert into dbtest.default.test values(2, "inserted");
  END IF;
END;


-- COMMAND ----------

-- create table default.test(id int, message string);

-- COMMAND ----------

select * from 
--TRUNCATE TABLE
default.test;

-- COMMAND ----------

--insert into default.test values(1, "inserted");

-- COMMAND ----------

select 'test_cell' as test_cell;