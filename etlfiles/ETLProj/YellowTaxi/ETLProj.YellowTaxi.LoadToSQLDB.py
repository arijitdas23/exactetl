# Databricks notebook source
import pyspark.sql.functions as sf
import datetime as datetime
spark.conf.set("spark.sql.execution.arrow.enabled", 'true')

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.SQLConfig

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.SQLFunctions

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.Metadata

# COMMAND ----------

# MAGIC %run ./ETLProj.YellowTaxi.Utility

# COMMAND ----------

# MAGIC %md <h4>Load the insight of Yellow Taxi Tip percentage by Quarter to SQLDB</h4>

# COMMAND ----------

# The SQL Query to read TipPctg insight data from Databricks table
select_sql_query_tippctg = "select VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_date, "\
                   "trip_year,trip_quarter,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag, "\
                   "PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount, "\
                   "tolls_amount,improvement_surcharge,total_amount,congestion_surcharge, "\
                   "tip_percentage,max_tip_pctg from {}".format(dbtable_yellowtaxi_tippctg)
# Read the data as spark dataframe
spdf_yellowtaxitippctg = spark.sql(select_sql_query_tippctg)
# Add metadata columns, such as processed_status, LastUpdatedBy, LastUpdatedTime
spdf_yellowtaxitippctg = stgtablemetadatacolumns(spdf_yellowtaxitippctg, 'Notebook:ETLProj.YellowTaxi.LoadToSQLDB')
# SQL DB Staging table target columns
sqldb_stg_tblcollist_tippctg = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_date',
                        'trip_year','trip_quarter','passenger_count','trip_distance','RatecodeID',
                        'store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra',
                        'mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount',
                        'congestion_surcharge','tip_percentage','max_tip_pctg','processed_status','LastUpdatedBy','LastUpdatedTime']
# Convert it to pandas dataframe
ppdf_yellowtaxitippctg = spdf_yellowtaxitippctg.toPandas()
ppdf_yellowtaxitippctg = ppdf_yellowtaxitippctg.fillna(0)
# Save it to SQL DB Staging table
loadtosqlserver(ppdf_yellowtaxitippctg, sqldb_stg_tippctg_tblname, sqldb_stg_tblcollist_tippctg)

# COMMAND ----------

# MAGIC %md <h4>Load the insight of Yellow Taxi Trip Speed by daily hour to SQLDB</h4>

# COMMAND ----------

# The SQL Query to read TripSpeed insight data from Databricks table
select_sql_query_tripspeed = "select VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,trip_date,trip_year, "\
                   "trip_quarter,trip_month,trip_day_of_month,trip_hour_of_day,passenger_count, "\
                   "trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID, "\
                   "payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge, "\
                   "total_amount,congestion_surcharge,trip_duration_in_hour,trip_speed,max_trip_speed from {}".format(dbtable_yellowtaxi_tripspeed)
# Read the data as spark dataframe
spdf_yellowtaxitripspeed = spark.sql(select_sql_query_tripspeed)
# Add metadata columns, such as processed_status, LastUpdatedBy, LastUpdatedTime
spdf_yellowtaxitripspeed = stgtablemetadatacolumns(spdf_yellowtaxitripspeed, 'Notebook:ETLProj.YellowTaxi.LoadToSQLDB')
# SQL DB Staging table target columns
sqldb_stg_tblcollist_tripspeed = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_date','trip_year','trip_quarter',
                                  'trip_month','trip_day_of_month','trip_hour_of_day','passenger_count','trip_distance','RatecodeID',
                                  'store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount',
                                  'tolls_amount','improvement_surcharge','total_amount','congestion_surcharge','trip_duration_in_hour',
                                  'trip_speed','max_trip_speed','processed_status','LastUpdatedBy','LastUpdatedTime']
# Convert it to pandas dataframe
ppdf_yellowtaxitripspeed = spdf_yellowtaxitripspeed.toPandas()
ppdf_yellowtaxitripspeed = ppdf_yellowtaxitripspeed.fillna(0)
# Save it to SQL DB Staging table
loadtosqlserver(ppdf_yellowtaxitripspeed, sqldb_stg_tripspeed_tblname, sqldb_stg_tblcollist_tripspeed)
