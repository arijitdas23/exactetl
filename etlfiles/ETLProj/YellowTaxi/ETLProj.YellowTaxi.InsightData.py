# Databricks notebook source
from pyspark import SparkContext
import pyspark.sql.functions as sf
from pyspark.sql.functions import to_timestamp
import datetime
from pyspark.sql.types import *
from pyspark.sql import Window
spark.sparkContext.setCheckpointDir('/tmp/checkpoints')

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.Metadata

# COMMAND ----------

def savedataframetodbtable(spdf_input,table_name):
  """function to save data in Databriks table.
  
  Args:
    spdf_input : input spark dataframe
    table_name : Databricks table name
    
  Returns:
    None 
    
  Raises:
    Exception if table name not mentioned or no data to write
  """
  if table_name == None or table_name == '':
    raise Exception("ETLProj.YellowTaxi.InsightData:savedataframetodbtable: No table name mentioned to store insight data")
  if len(spdf_input.head(1))>0:
    spdf_input.write.format(db_table_format).mode(db_table_mode).saveAsTable(table_name)
  else:
    raise Exception("ETLProj.YellowTaxi.InsightData:savedataframetodbtable: Blank Dataframe.Nothing to save")

# COMMAND ----------

# MAGIC %md <h4>Read clean data from ADLS</h4>

# COMMAND ----------

spdf_yellowtaxi_cleandata = spark.read.parquet('dbfs:/mnt/root/yellowtaxidata/cleandata/*.parquet')

# COMMAND ----------

# MAGIC %md <h4>Generate Insight for Tip</h4>
# MAGIC This will generate insight of where tip is highest, in proportion to the cost of the ride, per quarter

# COMMAND ----------

# Define a window function based on per year per quarter
windowhighesttip = Window.partitionBy('trip_year','trip_quarter')
# Find max tip percentage on per year per quarter basis
spdf_yellowtaxi_tippctg = spdf_yellowtaxi_cleandata.\
withColumn('max_tip_pctg',sf.max(sf.col('tip_percentage')).over(windowhighesttip))
# Select those rows having tip percentage is highest
spdf_yellowtaxi_tippctg = spdf_yellowtaxi_tippctg.filter(sf.col('tip_percentage')==sf.col('max_tip_pctg'))
# Save the data to Databriks table
savedataframetodbtable(spdf_yellowtaxi_tippctg,'tbl_yellowtaxitippctg')

# COMMAND ----------

# MAGIC %md <h4>Generate Insight for Trip Speed</h4>
# MAGIC This section will generate insight of hour of the day the speed of the taxis is highest (speed = distance/trip duration)

# COMMAND ----------

# Define a window function based on per year per month per day of month and per hour of day
windowmaxspeed = Window.partitionBy('trip_year','trip_month','trip_day_of_month','trip_hour_of_day')
# Find max tip percentage on per year per month per day of month and per hour of day basis
spdf_yellowtaxi_tripspeed = spdf_yellowtaxi_cleandata.\
withColumn('max_trip_speed',sf.max(sf.col('trip_speed')).over(windowmaxspeed))
# Select those rows having trip speed is highest
spdf_yellowtaxi_tripspeed = spdf_yellowtaxi_tripspeed.filter(sf.col('trip_speed')==sf.col('max_trip_speed'))
#spdf_yellowtaxi_tripspeed = spdf_yellowtaxi_tripspeed.dropDuplicates(['trip_year','trip_quarter','trip_month','trip_day_of_month','trip_hour_of_day'])
# Save the data to Databriks table
savedataframetodbtable(spdf_yellowtaxi_tripspeed,'tbl_yellowtaxitripspeed')
