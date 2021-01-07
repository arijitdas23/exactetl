import os
import sqlalchemy as sa
import urllib
import pandas as pd

def opendbconnection():
    """
    sql_server_name = os.environ.get('SQL_SERVER_NAME')
    sql_db_name = os.environ.get('SQL_DB_NAME')
    db_user_name = os.environ.get('SQL_USER_NAME')
    db_user_pswd = os.environ.get('SQL_PSWD')

    """
    sql_server_name = 'azsqldbserver1988.database.windows.net'
    sql_db_name = 'azsqldb1988'
    db_user_name = 'demouser'
    db_user_pswd = 'Iamsmart@2020'


    sql_connection_settings = f'SERVER={sql_server_name};PORT=1433;DATABASE={sql_db_name};UID={db_user_name};PWD={db_user_pswd}'
    sql_connection_string = f'DRIVER={{FreeTDS}};{sql_connection_settings};TDS_VERSION=8.0;'

    params = urllib.parse.quote_plus(sql_connection_string)
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    """
    conStrDct = {'user': 'demouser',
                 'password': 'Iamsmart@2020',
                 'server': 'azsqldbserver1988.database.windows.net',
                 'database': 'azsqldb1988',
                 'driver': 'SQL+Server'
                 # 'driver':'FreeTDS'
                 }

    engine = sa.create_engine("mssql+pyodbc://{}:{}@{}/{}?driver={}" \
                              .format(conStrDct.get('user'), \
                                      conStrDct.get("password"), \
                                      conStrDct.get('server'), \
                                      conStrDct.get("database"), \
                                      conStrDct.get("driver")), echo=False)
    """

    return engine

def closedbconnection(conn):
    conn.dispose()

def prepareproccall(procname,paramlist):
    val = "?,"*len(paramlist)
    val = val[:-1]
    callproc = "set nocount on;{call "+procname+"("+val+")}"
    return callproc

def readcustomer():
    conn = opendbconnection()
    df = pd.DataFrame([],columns=['FirstName'])
    procname = 'dbo.sp_readcustomer'
    paramlist = [2]
    callproc = prepareproccall(procname,paramlist)
    try:
        with conn.begin() as connection:
            results = connection.execute(callproc,paramlist).fetchall()
            if len(results)>0:
                df = pd.DataFrame(results,columns=['FirstName'])
            success = True
    except Exception as e:
        success = str(e)
    closedbconnection(conn)
    return df, success

def getcustomernameapi():
    customerdf, success = readcustomer()
    out = customerdf.to_json(orient='records')
    return out+success



