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

SrcTable = ""
TrgtTable = ""
FilePath = ""
FileName = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import builtins
from pyspark.sql.functions import max,min,col

#Specify Current Table ID
if SrcTable == "OrderDetails":
    IDchk = "OrderID"
else:
    IDchk = SrcTable[:-1] + "ID"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Return Checks from xlsx
xlsx = pd.read_excel(FilePath,sheet_name=SrcTable)
minx = xlsx[IDchk].min()
maxx = xlsx[IDchk].max()
count = len(xlsx)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get latest ingested rows
df = spark.read.table(f"Northwind.{TrgtTable}")
Latest_Ingestion = df.select(max("IngestedAt")).collect()[0][0]

#Filter latest ingested rows from api
filtered_df = df.filter((df["IngestedAt"] == Latest_Ingestion) & (df["IngestionSource"] == "XLSX"))

#Compare rows count
if filtered_df.count() == count:
    print("Records Match")
else:
    raise Exception(f"Ingested Records = {filtered_df.count()} While Actual = {count}")

#Compare min/max IDs
maxing = filtered_df.agg(max(col(IDchk))).collect()[0][0]
mining = filtered_df.agg(min(col(IDchk))).collect()[0][0]

if maxing == maxx:
    print("Max ID Matches")
else:
    raise Exception(f"Ingested Max ID = {maxing} While Actual = {maxID}")

if mining == minx:
    print("Min ID Matches")
else:
    raise Exception(f"Ingested Max ID = {mining} While Actual = {minID}")


spark.sql(f"""
UPDATE Configuration.fileconfig
SET IngestedSheets = IngestedSheets + 1
WHERE FileName = '{FileName}'""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
