# Databricks notebook source
# ICD Code API Extract
# Note: ADLS storage key is configured at cluster level (rcm cluster)
# Always run on rcm cluster, NOT SQL Warehouse

import requests
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

# COMMAND ----------

# Configuration
BASE_URL = "https://id.who.int/icd/"
AUTH_URL = "https://icdaccessmanagement.who.int/connect/token"
ADLS_PATH = "abfss://bronze@rcmproject.dfs.core.windows.net/icd_codes/"
current_date = datetime.now().date()

# API Credentials
CLIENT_ID = "c1b31e75-de89-4e9e-9738-87a4779e98e9_b7b0ac50-33a3-4ba5-abc2-4f4b1050ab15"
CLIENT_SECRET = "UtTb8EIodKWT2g3BoIkgGsNbMcv0ClZKJbEij4ySzTs="

# COMMAND ----------

# Authenticate with ICD API
auth_response = requests.post(AUTH_URL, data={
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
})

if auth_response.status_code != 200:
    raise Exception(f"Failed to obtain access token: {auth_response.status_code} - {auth_response.text}")

access_token = auth_response.json().get("access_token")

headers = {
    "Authorization": f"Bearer {access_token}",
    "API-Version": "v2",
    "Accept-Language": "en",
}

print("Authentication successful")

# COMMAND ----------

# Helper functions
def fetch_icd_codes(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

def extract_codes(url):
    data = fetch_icd_codes(url)
    codes = []
    if "child" in data:
        for child_url in data["child"]:
            codes.extend(extract_codes(child_url))
    else:
        if "code" in data and "title" in data:
            codes.append({
                "icd_code": data["code"],
                "icd_code_type": "ICD-10",
                "code_description": data["title"]["@value"],
                "inserted_date": current_date,
                "updated_date": current_date,
                "is_current_flag": True
            })
    return codes

# COMMAND ----------

# Fetch ICD codes
root_url = "https://id.who.int/icd/release/10/2019/A00-A09"
icd_codes = extract_codes(root_url)

print(f"Fetched {len(icd_codes)} ICD codes")

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

# Create DataFrame and write to ADLS
if icd_codes:
    df = spark.createDataFrame(icd_codes, schema=schema)
    display(df)
    df.write.format("parquet").mode("append").save(ADLS_PATH)
    print(f"Data written to {ADLS_PATH}")
else:
    print("No results found.")