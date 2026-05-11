# Databricks notebook source
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA default")

df_raw = spark.read.table("main.default.mortgage_raw")
print(f"Raw rows loaded: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan, isnull

# Check nulls in each column
print("=== NULL COUNTS PER COLUMN ===")
null_counts = df_raw.select([
    count(when(isnull(c), c)).alias(c) 
    for c in df_raw.columns
])
null_counts.display()

# Total rows
total = df_raw.count()
print(f"\nTotal rows: {total}")

# COMMAND ----------

from pyspark.sql.functions import col, when, median, lit, current_timestamp

# Fill nulls with median/default values
median_credit = df_raw.approxQuantile("credit_score", [0.5], 0.01)[0]
median_dti = df_raw.approxQuantile("debt_to_income", [0.5], 0.01)[0]
median_income = df_raw.approxQuantile("applicant_income", [0.5], 0.01)[0]

df_refined = df_raw \
    .fillna({"credit_score": median_credit,
             "debt_to_income": median_dti,
             "applicant_income": median_income}) \
    .filter(col("loan_amount") > 0) \
    .filter(col("loan_term").isin([180, 240, 360])) \
    .filter(col("action_taken").isin([1, 2, 3])) \
    .withColumn("action_label", 
        when(col("action_taken") == 1, "Approved")
        .when(col("action_taken") == 2, "Denied")
        .otherwise("Withdrawn")) \
    .withColumn("_layer", lit("refined")) \
    .withColumn("_refined_timestamp", current_timestamp())

print(f"Refined rows: {df_refined.count()}")

# COMMAND ----------

# Quality gate — fail if too many nulls remain
null_check = df_refined.select([
    count(when(isnull(c), c)).alias(c)
    for c in ["credit_score", "debt_to_income", "applicant_income"]
]).collect()[0]

failed = False
for col_name in ["credit_score", "debt_to_income", "applicant_income"]:
    if null_check[col_name] > 0:
        print(f"QUALITY FAIL: {col_name} still has nulls")
        failed = True

if not failed:
    print("Quality gate PASSED — no nulls in critical columns")

# COMMAND ----------

df_refined.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.default.mortgage_refined")

print("Refined layer written successfully")
spark.sql("SELECT COUNT(*) as total_rows FROM main.default.mortgage_refined").display()