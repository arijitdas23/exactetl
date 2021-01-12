# Databricks notebook source
def file_exists(path):
  """Generic function to check file path exists or not
  
  Args:
    path : dbfs or mount location
    
  Returns:
    Boolean True or False 
    
  Raises:
    None
  """
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def log_trace_to_db_console(msg):
  """Generic function log info in Databricks console
  
  Args:
    msg : message to print
    
  Returns:
    Na 
    
  Raises:
    None
  """
  print(msg)
