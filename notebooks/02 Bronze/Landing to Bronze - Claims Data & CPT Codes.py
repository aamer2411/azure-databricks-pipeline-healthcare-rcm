# Databricks notebook source
# Landing to Bronze - Claims Data & CPT Codes
# Note: ADLS storage key is configured at cluster level (rcm cluster)
# Always run on rcm cluster, NOT SQL Warehouse

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# Configuration
LANDING_PATH = "abfss://landing@rcmproject.dfs.core.windows.net"
BRONZE_PATH = "abfss://bronze@rcmproject.dfs.core.windows.net"
current_date = datetime.now().date()

# Source paths
CLAIMS_SOURCE = f"{LANDING_PATH}/claims/*.csv"
CPTCODES_SOURCE = f"{LANDING_PATH}/cptcodes/*.csv"

# Destination paths
CLAIMS_DEST = f"{BRONZE_PATH}/claims/"
CPTCODES_DEST = f"{BRONZE_PATH}/cptcodes/"

# COMMAND ----------

# ==========================================
# CLAIMS DATA
# ==========================================

# Read claims CSV files from landing
claims_df = spark.read.csv(CLAIMS_SOURCE, header=True, inferSchema=True)

# Add datasource column based on file name
claims_df = claims_df.withColumn(
    "datasource",
    F.when(F.input_file_name().contains("hospital1"), "hosa")
     .when(F.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

# Add audit columns
claims_df = claims_df.withColumn("inserted_date", F.lit(current_date))
claims_df = claims_df.withColumn("updated_date", F.lit(current_date))

print(f"Claims records: {claims_df.count()}")
display(claims_df)

# COMMAND ----------

# Write claims to bronze
claims_df.write.format("parquet").mode("overwrite").save(CLAIMS_DEST)
print(f"Claims data written to {CLAIMS_DEST}")

# COMMAND ----------

# ==========================================
# CPT CODES
# ==========================================

# Read CPT codes CSV files from landing
cptcodes_df = spark.read.csv(CPTCODES_SOURCE, header=True, inferSchema=True)

# Clean column names: replace spaces with underscores and lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# Add audit columns
cptcodes_df = cptcodes_df.withColumn("inserted_date", F.lit(current_date))
cptcodes_df = cptcodes_df.withColumn("updated_date", F.lit(current_date))

print(f"CPT codes records: {cptcodes_df.count()}")
display(cptcodes_df)

# COMMAND ----------

# Write CPT codes to bronze
cptcodes_df.write.format("parquet").mode("overwrite").save(CPTCODES_DEST)
print(f"CPT codes data written to {CPTCODES_DEST}")