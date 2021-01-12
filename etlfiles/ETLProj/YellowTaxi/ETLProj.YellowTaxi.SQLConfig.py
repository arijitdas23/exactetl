# Databricks notebook source
import pyodbc

# COMMAND ----------

def openSQLDBConnection():
  """Generic function to prepare and open SQL DB connection
  
  Args:
    NA
    
  Returns:
    pyodbc connection 
    
  Raises:
    None
  """
  # Read SQL DB connection secrets from KeyVault
  sqldbserver = dbutils.secrets.get(scope="DBSecrets", key="SQLDB_SERVER_NAME")
  sqldbname = dbutils.secrets.get(scope="DBSecrets", key="SQLDB_DATABASE_NAME")
  sqldbusername = dbutils.secrets.get(scope="DBSecrets", key="SQLDB_USER_ID")
  sqldbpswd = dbutils.secrets.get(scope="DBSecrets", key="SQLDB_DB_PSWD")
  # Create the connection
  pyodbcconn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={};DATABASE={};UID={};PWD={}'.\
                              format(sqldbserver,sqldbname,sqldbusername,sqldbpswd))
  return pyodbcconn

def closeconnection(pyodbcconn):
  """Generic function to close SQL DB connection
  
  Args:
    NA
    
  Returns:
    NA 
    
  Raises:
    None
  """
  pyodbcconn.close()
