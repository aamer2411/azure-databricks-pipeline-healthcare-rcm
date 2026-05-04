# Databricks notebook source
# NPI API Extract
# Note: ADLS storage key is configured at cluster level (rcm cluster)
# Always run on rcm cluster, NOT SQL Warehouse

import requests
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType

# COMMAND ----------

# Configuration
BASE_URL = "https://npiregistry.cms.hhs.gov/api/"
ADLS_PATH = "abfss://bronze@rcmproject.dfs.core.windows.net/npi_extract/"
current_date = datetime.now().date()

# API Parameters
params = {
    "version": "2.1",
    "state": "CA",
    "city": "Los Angeles",
    "limit": 20,
}

# COMMAND ----------

# Fetch NPI data from API
response = requests.get(BASE_URL, params=params)

if response.status_code != 200:
    raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

npi_data = response.json()
npi_list = [result["number"] for result in npi_data.get("results", [])]

# COMMAND ----------

# Get detailed NPI information
detailed_results = []

for npi in npi_list:
    detail_params = {"version": "2.1", "number": npi}
    detail_response = requests.get(BASE_URL, params=detail_params)

    if detail_response.status_code == 200:
        detail_data = detail_response.json()
        if "results" in detail_data and detail_data["results"]:
            for result in detail_data["results"]:
                npi_number = result.get("number")
                basic_info = result.get("basic", {})

                if result["enumeration_type"] == "NPI-1":
                    fname = basic_info.get("first_name", "")
                    lname = basic_info.get("last_name", "")
                else:
                    fname = basic_info.get("authorized_official_first_name", "")
                    lname = basic_info.get("authorized_official_last_name", "")

                position = basic_info.get("authorized_official_title_or_position", "")
                organisation = basic_info.get("organization_name", "")
                last_updated = basic_info.get("last_updated", "")

                detailed_results.append({
                    "npi_id": npi_number,
                    "first_name": fname,
                    "last_name": lname,
                    "position": position,
                    "organisation_name": organisation,
                    "last_updated": last_updated,
                    "inserted_date": current_date,
                    "updated_date": current_date,
                })

print(f"Fetched {len(detailed_results)} NPI records")

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("npi_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("organisation_name", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
])

# Create DataFrame and write to ADLS
if detailed_results:
    df = spark.createDataFrame(detailed_results, schema=schema)
    display(df)
    df.write.format("parquet").mode("overwrite").save(ADLS_PATH)
    print(f"Data written to {ADLS_PATH}")
else:
    print("No results found.")