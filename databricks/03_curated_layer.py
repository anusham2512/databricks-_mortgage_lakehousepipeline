# Databricks notebook source
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA default")

df_refined = spark.read.table("main.default.mortgage_refined")
print(f"Refined rows loaded: {df_refined.count()}")

# COMMAND ----------

from pyspark.sql.functions import count, round, avg, when, col

df_by_state = df_refined.groupBy("state") \
    .agg(
        count("loan_id").alias("total_applications"),
        count(when(col("action_label") == "Approved", 1)).alias("approved"),
        count(when(col("action_label") == "Denied", 1)).alias("denied"),
        round(avg("loan_amount"), 2).alias("avg_loan_amount"),
        round(avg("credit_score"), 2).alias("avg_credit_score"),
        round(avg("applicant_income"), 2).alias("avg_income")
    ) \
    .withColumn("approval_rate_%", 
        round((col("approved") / col("total_applications")) * 100, 2))

df_by_state.display()

# COMMAND ----------

from pyspark.sql.functions import when, col, count, round, avg

df_credit_band = df_refined \
    .withColumn("credit_band",
        when(col("credit_score") < 620, "Poor (<620)")
        .when(col("credit_score") < 680, "Fair (620-679)")
        .when(col("credit_score") < 740, "Good (680-739)")
        .otherwise("Excellent (740+)")) \
    .groupBy("credit_band") \
    .agg(
        count("loan_id").alias("total_applications"),
        count(when(col("action_label") == "Approved", 1)).alias("approved"),
        round(avg("loan_amount"), 2).alias("avg_loan_amount"),
        round(avg("debt_to_income"), 2).alias("avg_dti")
    ) \
    .withColumn("approval_rate_%",
        round((col("approved") / col("total_applications")) * 100, 2)) \
    .orderBy("credit_band")

df_credit_band.display()

# COMMAND ----------

df_purpose = df_refined.groupBy("loan_purpose", "property_type") \
    .agg(
        count("loan_id").alias("total_applications"),
        count(when(col("action_label") == "Approved", 1)).alias("approved"),
        round(avg("loan_amount"), 2).alias("avg_loan_amount"),
        round(avg("interest_rate"), 2).alias("avg_interest_rate")
    ) \
    .withColumn("approval_rate_%",
        round((col("approved") / col("total_applications")) * 100, 2)) \
    .orderBy("loan_purpose", "property_type")

df_purpose.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Final curated dataset — all features for loan approval analysis
df_curated = df_refined \
    .withColumn("credit_band",
        when(col("credit_score") < 620, "Poor")
        .when(col("credit_score") < 680, "Fair")
        .when(col("credit_score") < 740, "Good")
        .otherwise("Excellent")) \
    .withColumn("income_band",
        when(col("applicant_income") < 50000, "Low")
        .when(col("applicant_income") < 100000, "Medium")
        .when(col("applicant_income") < 150000, "High")
        .otherwise("Very High")) \
    .withColumn("loan_to_income_ratio",
        round(col("loan_amount") / col("applicant_income"), 2)) \
    .withColumn("_layer", lit("curated")) \
    .withColumn("_curated_timestamp", current_timestamp())

df_curated.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.default.mortgage_curated")

print("Curated layer written successfully")
spark.sql("SELECT COUNT(*) as total_rows FROM main.default.mortgage_curated").display()

# COMMAND ----------

print("=== PIPELINE SUMMARY ===")
raw = spark.sql("SELECT COUNT(*) as cnt FROM main.default.mortgage_raw").collect()[0]['cnt']
refined = spark.sql("SELECT COUNT(*) as cnt FROM main.default.mortgage_refined").collect()[0]['cnt']
curated = spark.sql("SELECT COUNT(*) as cnt FROM main.default.mortgage_curated").collect()[0]['cnt']

print(f"Raw layer:      {raw} rows")
print(f"Refined layer:  {refined} rows")
print(f"Curated layer:  {curated} rows")
print(f"\nAll 3 Delta layers built successfully!")