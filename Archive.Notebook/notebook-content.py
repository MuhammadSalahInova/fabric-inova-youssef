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

# CELL ********************

from pyspark.sql.functions import col, current_timestamp, when,regexp_replace
#Initial Condition
condition = (
    (col("FileStatus") == "New") &
    (col("IngestedSheets") == 4)
)
df = spark.read.table("Configuration.fileconfig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read totally ingested files
files_to_archive = (
    df.filter(
        condition
    )
    .select("FileName")
    .collect()
)

#Check if none exists
if not files_to_archive:
    mssparkutils.notebook.exit("No files to archive")

#Archive files
for row in files_to_archive:
    file_name = row["FileName"]

    source = f"Files/New/Excel/{file_name}"
    destination = f"Files/Archived/Excel/{file_name}"

    mssparkutils.fs.mv(source, destination)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Update the config table
df = df.withColumn(
    "FilePath",
    when(condition,regexp_replace(col("FilePath"), "New", "Archived")).otherwise(col("FilePath"))
).withColumn(
    "ArchivedAt",
    when(condition, current_timestamp()).otherwise(col("ArchivedAt"))
).withColumn(
    "FileStatus",
    when(condition, "Archived").otherwise(col("FileStatus"))
)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Configuration.fileConfig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
