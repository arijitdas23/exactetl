# Databricks notebook source
def prepareinsertstatement(sqltablename, columnlist):
  """Generic function to prepare insert statements to load data in
  SQL Server table.
  
  Args:
    sqltablename : SQL Server target table where data will be loaded
    columnlist : List of columns for target table
    
  Returns:
    SQL insert prepared statement 
    
  Raises:
    None
  """
  colname = ''
  val = ''
  for each in columnlist:
    if colname == '':
      colname = each
      val='?'
    else:
      colname = colname+','+each
      val = val+','+'?'
  insertsql = "insert into {}({}) values ({})".format(sqltablename,colname,val)
  return insertsql

# COMMAND ----------

def loadtosqlserver(pddf, sqltablename, columnlist):
  """Generic function to load data in SQL Server table.
  
  Args:
    ppdf : the pandas dataframe containing data to load
    sqltablename : SQL Server target table where data will be loaded
    columnlist : List of columns for target table
    
  Returns:
    None 
    
  Raises:
    None
  """
  pyodbcconn = openSQLDBConnection()
  cursor = pyodbcconn.cursor()
  insertstatement = prepareinsertstatement(sqltablename, columnlist)
  try:
    for row_count in range(0,pddf.shape[0]):
      try:
        chunk = pddf.iloc[row_count:row_count+1,:].values.tolist()
        tuple_of_tuples = tuple(tuple(x) for x in chunk)
        cursor.executemany(insertstatement,tuple_of_tuples)
      except Exception as e:
        log_trace_to_db_console("ETLProj.YellowTaxi.SQLFunctions:loadtosqlserver: Problem in executing prepared statement")
        pass
  except Exception as e:
    log_trace_to_db_console("ETLProj.YellowTaxi.SQLFunctions:loadtosqlserver: Problem in loading data in sqldb ")
  finally:
    cursor.commit()
    closeconnection(pyodbcconn)

# COMMAND ----------

def stgtablemetadatacolumns(spdf, source):
  """Generic function to add extra metadata column for SQL DB Staging table.
  
  Args:
    ppdf : the spark dataframe
    source : source notebook name
    
  Returns:
    spdf : spark dataframe with added columns 
    
  Raises:
    None
  """
  spdf = spdf.withColumn('processed_status', sf.lit('N')).\
  withColumn('LastUpdatedBy', sf.lit(source)).\
  withColumn('LastUpdatedTime', sf.date_format('current_timestamp','yyyy-MM-dd HH:mm:ss'))
  return spdf
