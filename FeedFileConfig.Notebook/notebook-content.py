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

#Read Object Storage and config table
excel = mssparkutils.fs.ls("Files/New/Excel")
file_config = spark.table("Configuration.fileconfig")

#Stop if no new files exist
if len(excel) == 0:
    mssparkutils.notebook.exit("No files to process")

#collect file names from config
existing_files = {
    row.FileName
    for row in file_config.select("FileName").collect()
}

#Insert to config if file new
for file in excel:
    if file.name not in existing_files:
        spark.sql(f"""
        INSERT INTO Configuration.fileconfig
        (SourceID, FileName, FilePath, FileStatus, ReadAt, ArchivedAt,IngestedSheets)
        VALUES
        ('XLSX', '{file.name}', '{file.path}', 'New', CURRENT_TIMESTAMP(), NULL,0)""")
    else:
        mssparkutils.fs.rm(file.path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
