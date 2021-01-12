# Databricks notebook source
# MAGIC %md <h2>Reading the Dynamic Values</h2>

# COMMAND ----------

dbutils.widgets.text("username","")
username = dbutils.widgets.get("username")

dbutils.widgets.text("password","")
password = dbutils.widgets.get("password")

dbutils.widgets.text("directoryid","")
directoryid = dbutils.widgets.get("directoryid")

dbutils.widgets.text("storagename","")
storagename = dbutils.widgets.get("storagename")

dbutils.widgets.text("mountpoint","")
mountpoint = dbutils.widgets.get("mountpoint")

# COMMAND ----------

# MAGIC %md <h2>Mount Data Lake Gen1 to DBFS</h2>

# COMMAND ----------

configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
           "fs.adl.oauth2.client.id": username,
           "fs.adl.oauth2.credential": password,
           "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/"+directoryid+"/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://"+storagename+".azuredatalakestore.net/"+mountpoint,
  mount_point = "/mnt/"+mountpoint,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md <h2>Set up ODBC</h2>

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc
