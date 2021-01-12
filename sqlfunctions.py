import os
import sqlalchemy as sa
import urllib
import pandas as pd


def opendbconnection():
    """Generic function to connect SQL DB using secrets.

      Args:
        NA

      Returns:
        pyodbc connection engine

      Raises:
        None
    """
    sql_server_name = os.environ.get('SQL_SERVER_NAME')
    sql_db_name = os.environ.get('SQL_DB_NAME')
    db_user_name = os.environ.get('SQL_USER_NAME')
    db_user_pswd = os.environ.get('SQL_PSWD')

    sql_connection_settings = f'SERVER={sql_server_name};PORT=1433;DATABASE={sql_db_name};UID={db_user_name};PWD={db_user_pswd}'
    sql_connection_string = f'DRIVER={{FreeTDS}};{sql_connection_settings};TDS_VERSION=8.0;'

    params = urllib.parse.quote_plus(sql_connection_string)
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    return engine


def closedbconnection(conn):
    """Generic function to close sqldb connection.

      Args:
        NA

      Returns:
        NA

      Raises:
        None
    """
    conn.dispose()


def prepareproccall(procname, paramlist):
    """Generic function to prepare the call for Store Procedure.

      Args:
        procname : the nme of the procedure
        paramlist : the parameter list

      Returns:
        callproc : String presenting StoreProcedure call prepared statement

      Raises:
        None
    """
    val = "?," * len(paramlist)
    val = val[:-1]
    callproc = "set nocount on;{call " + procname + "(" + val + ")}"
    return callproc


def readusingsqldbproc(procname, paramlist, columnlist):
    """Generic function to read data from SQL DB using
       store procedure.

      Args:
        procname : the nme of the procedure
        paramlist : the parameter list
        columnlist : return column list

      Returns:
        df : Dataframe containing the result
        status : status of the execution, "Correct" for happy flow
                 otherwise exception string

      Raises:
        None
    """
    conn = opendbconnection()
    df = pd.DataFrame([], columns=columnlist)
    callproc = prepareproccall(procname, paramlist)
    try:
        with conn.begin() as connection:
            results = connection.execute(callproc, paramlist).fetchall()
            if len(results) > 0:
                df = pd.DataFrame(results, columns=columnlist)
            status = "Correct"
    except Exception as e:
        status = str(e)
    finally:
        closedbconnection(conn)
    return df, status
