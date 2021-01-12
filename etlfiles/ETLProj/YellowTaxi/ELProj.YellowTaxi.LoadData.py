# Databricks notebook source
# MAGIC %md <h4>Import Required Libraries</h4>

# COMMAND ----------

from pyspark import SparkContext
from pyspark import SparkFiles

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.Metadata

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.Utility

# COMMAND ----------

# MAGIC %md <h4>Read Data from URL and store it to DBFS</h4>

# COMMAND ----------

# Read data from URL if it is not exists in local DBFS path
if file_exists(local_dbfs_path):
  print("ELProj.YellowTaxi.LoadData:Input Files Already exists")
else:
  for month_identifier in month_identifier_lst:
    data_url = base_url+base_file_name+month_identifier+".csv"
    spark.sparkContext.addFile(data_url)
    local_file_path = "file://"+SparkFiles.get(base_file_name+month_identifier+".csv")
    dbutils.fs.mv(local_file_path, local_dbfs_path+base_file_name+month_identifier+".csv")

# COMMAND ----------

# MAGIC %md <h4>Save data to ADLS as parquet file for future reference</h4>

# COMMAND ----------

# Read downloaded data from dbfs path
spdf_yellowtaxi_data = spark.read.csv(local_dbfs_path+'/*.csv',header=True, inferSchema=True)
# Re-partition into 6 partitions
spdf_yellowtaxi_data = spdf_yellowtaxi_data.coalesce(6)
# Save the data to ADLS
if file_exists(dbfs_mnt_base_path+'/inputdata'):
  dbutils.fs.rm(dbfs_mnt_base_path+'/inputdata',True)
spdf_yellowtaxi_data.write.format('parquet').save(dbfs_mnt_base_path+'/inputdata')
