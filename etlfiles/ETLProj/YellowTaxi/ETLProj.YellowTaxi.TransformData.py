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

# MAGIC %run ./ETLProj.YellowTaxi.Utility

# COMMAND ----------

spdf_yellowtaxi_inputdata = spark.read.parquet(dbfs_mnt_base_path+'/inputdata/*.parquet')

# COMMAND ----------

# Clean up, add additional column, column transformation etc.
spdf_yellowtaxi_data = spdf_yellowtaxi_inputdata.selectExpr(
                       'concat(VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,DOLocationID) as trip_identifier',
                       'VendorID',
                       'cast(tpep_pickup_datetime as timestamp) tpep_pickup_datetime',
                       'cast(tpep_dropoff_datetime as timestamp) tpep_dropoff_datetime',
                       'to_date(tpep_pickup_datetime) as trip_date',
                       'year(tpep_pickup_datetime) as trip_year',
                       'quarter(tpep_pickup_datetime) as trip_quarter',
                       'month(tpep_pickup_datetime) as trip_month',
                       'dayofmonth(tpep_pickup_datetime) as trip_day_of_month',
                       'hour(tpep_pickup_datetime) as trip_hour_of_day',
                       'passenger_count',
                       'trip_distance',
                       'RatecodeID',
                       'store_and_fwd_flag',
                       'PULocationID',
                       'DOLocationID',
                       'payment_type',
                       'fare_amount',
                       'extra',
                       'mta_tax',
                       'tip_amount',
                       'tolls_amount',
                       'improvement_surcharge',
                       'total_amount',
                       'congestion_surcharge'
)
spdf_yellowtaxi_data_chkd = spdf_yellowtaxi_data.checkpoint()

# COMMAND ----------

# Select only those records whose year of trip matching in the list
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_chkd.filter(sf.col('trip_year').isin(var_trip_year_lst))
# Remove all duplicates based on VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,DOLocationID
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_2020.dropDuplicates(['trip_identifier'])
# Add tip percentage
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_2020.withColumn('tip_percentage',\
                                                                 sf.expr('bround((tip_amount/total_amount)*100,2)'))
# Add trip_duration_in_seconds
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_2020.withColumn('trip_duration_in_seconds',\
                                                       sf.col('tpep_dropoff_datetime').cast('long')-\
                                                                 sf.col('tpep_pickup_datetime').\
                                                       cast('long'))
# Add trip_duration_in_hour
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_2020.withColumn('trip_duration_in_hour',\
                                                       sf.bround((sf.col('trip_duration_in_seconds')/3600),3)).\
                                                       drop('trip_duration_in_seconds')
# Calculate and Add Trip Spped
spdf_yellowtaxi_data_2020 = spdf_yellowtaxi_data_2020.withColumn('trip_speed',sf.bround\
                                                                 ((sf.expr('trip_distance/trip_duration_in_hour')),2))
# Now filter valid trips based on below assumptions:
#   - Consider those trips which is atleast 1 min of duration
#   - Consider only those records where fare amount is not 0 and more than tip amount
#   - Consider only when trip distance is not 0 and PickUp Location is not equal DropOff Location
#   - Valid payment types are Cash and Credit Card
spdf_yellowtaxi_validdata = spdf_yellowtaxi_data_2020.filter(
                                         (sf.col('trip_duration_in_hour')>=0.0167) & \
                                         (sf.col('fare_amount')>=0) &\
                                        (sf.col('fare_amount')>sf.col('tip_amount')) &\
                                        (sf.col('trip_distance')>0) &\
                                        ~(sf.col('PULocationID')==sf.col('DOLocationID')) &\
                                         (sf.col('payment_type').isin('1','2'))
                                        )

# COMMAND ----------

# Drop calculated trip_identifier column as it is only required to remove duplicates
spdf_yellowtaxi_validdata = spdf_yellowtaxi_validdata.drop('trip_identifier')
# Repartion to 6 partitions
spdf_yellowtaxi_validdata = spdf_yellowtaxi_validdata.coalesce(6)
# Save the data to ADLS
if file_exists(dbfs_mnt_base_path+'/cleandata'):
  dbutils.fs.rm(dbfs_mnt_base_path+'/cleandata',True)
spdf_yellowtaxi_validdata.write.format('parquet').save(dbfs_mnt_base_path+'/cleandata')
