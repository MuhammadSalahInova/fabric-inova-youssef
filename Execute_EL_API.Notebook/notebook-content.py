# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "57a47c02-b37e-402c-bc7f-2bab9f16b709",
# META       "default_lakehouse_name": "Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "c31b15d8-8c12-4378-9b7b-7d9ef27fbfa2",
# META       "known_lakehouses": [
# META         {
# META           "id": "57a47c02-b37e-402c-bc7f-2bab9f16b709"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

SrcTable = "Products"
TrgtTable = "Products"
Base_URL = "https://services.odata.org/V4/Northwind/Northwind.svc/"
Time = "2026-09-03T00:40:15Z"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from pyspark.sql.functions import to_timestamp,sha2, concat_ws,col,lit,lower,regexp_replace

# Initialization
spark.sql("""CREATE SCHEMA IF NOT EXISTS NorthWind""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Initial Table URL
current_table = Base_URL + SrcTable
response = requests.get(current_table)

#First Page
records = response.json().get("value", [])


#Pages loop
while response.json().get("@odata.nextLink",[]):
    #Next page URL
    next_link = Base_URL + response.json().get("@odata.nextLink",[])
    response = requests.get(next_link)

    records.extend(response.json().get("value", []))


#Transform to json first
json_rdd = spark.sparkContext.parallelize(
    [json.dumps(record) for record in records]
)

#Read API with dataframe
df = spark.read.json(json_rdd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Drop etag column if exists
if "@odata.etag" in df.columns:
    df = df.drop("@odata.etag")
#Drop picture columns if exists
if "Photo" in df.columns:
    df = df.drop("Photo")
if "PhotoPath" in df.columns:
    df = df.drop("PhotoPath")
if "Picture" in df.columns:
    df = df.drop("Picture")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#New Hashed Column
#Create String
ConcatenatedString = concat_ws(
    "||",
    *[col(f"`{c}`") for c in df.columns]
)
#Normalize String
ConcatenatedString = lower(ConcatenatedString)
ConcatenatedString = regexp_replace(ConcatenatedString, " ", "")
ConcatenatedString = regexp_replace(ConcatenatedString, r"\.00", "")

NewHashed = sha2(ConcatenatedString,256)

df = df.withColumn("RowHash", NewHashed)

#New source identifier column
df = df.withColumn(
    "IngestionSource",
    lit("API")
)

#New Ingestion time column
df = df.withColumn(
    "IngestedAt",
    to_timestamp(lit(Time))
)

#Write to bronze
df.write.format("delta").mode("append").saveAsTable(f"NorthWind.{TrgtTable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
