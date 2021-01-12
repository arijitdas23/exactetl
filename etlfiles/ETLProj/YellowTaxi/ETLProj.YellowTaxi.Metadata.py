# Databricks notebook source
#Define Metadata location
metadatafilelocation = 'dbfs:/FileStore/tables/'
metadatafilename = 'yellowtaximetadata.json'

#Read metadata
spdf_yellowtaximetadata = spark.read.json("{}{}".format(metadatafilelocation,metadatafilename),multiLine=True)
yellowtaximetadata_lst = spdf_yellowtaximetadata.collect()
yellowtaximetadata = yellowtaximetadata_lst[0]

# COMMAND ----------

# The base URL of NYC Yello Taxi data set
base_url = yellowtaximetadata.base_url
# FileName format
base_file_name = yellowtaximetadata.base_file_name
# Available data for the months in the URL
month_identifier_lst = yellowtaximetadata.month_identifier_lst
# Local DBFS file path
local_dbfs_path = yellowtaximetadata.local_dbfs_path
# Mount path of ADLS to DBFS
dbfs_mnt_base_path = yellowtaximetadata.dbfs_mnt_base_path
# FOrmat to write data in Databrcks table (example, delta)
db_table_format = yellowtaximetadata.db_table_format
# Mode to write data in Databrcks table (example, overrite)
db_table_mode = yellowtaximetadata.db_table_mode
# Year List (example, 2020,2019)
var_trip_year_lst = yellowtaximetadata.var_trip_year_lst
# Databricks table name where TipPctg insight will be saved
dbtable_yellowtaxi_tippctg = yellowtaximetadata.dbtable_yellowtaxi_tippctg
# SQL DB Staging table name where TipPctg insight will be saved
sqldb_stg_tippctg_tblname = yellowtaximetadata.sqldb_stg_tippctg_tblname
# Databricks table name where TripSpeed insight will be saved
dbtable_yellowtaxi_tripspeed = yellowtaximetadata.dbtable_yellowtaxi_tripspeed
# SQL DB Staging table name where TripSpeed insight will be saved
sqldb_stg_tripspeed_tblname = yellowtaximetadata.sqldb_stg_tripspeed_tblname
