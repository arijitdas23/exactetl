import os
import sqlalchemy as sa
import urllib
import pandas as pd
import json


def opendbconnection():

    sql_server_name = os.environ.get('SQL_SERVER_NAME')
    sql_db_name = os.environ.get('SQL_DB_NAME')
    db_user_name = os.environ.get('SQL_USER_NAME')
    db_user_pswd = os.environ.get('SQL_PSWD')

    """
    sql_server_name = 'azsqldbserver1988.database.windows.net'
    sql_db_name = 'azsqldb1988'
    db_user_name = 'demouser'
    db_user_pswd = 'Iamsmart@2020'
    """

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


def prepareproccall(procname, paramlist):
    val = "?," * len(paramlist)
    val = val[:-1]
    callproc = "set nocount on;{call " + procname + "(" + val + ")}"
    return callproc


def readusingsqldbproc(procname, paramlist, columnlist):
    conn = opendbconnection()
    df = pd.DataFrame([], columns=columnlist)
    callproc = prepareproccall(procname, paramlist)
    try:
        with conn.begin() as connection:
            results = connection.execute(callproc, paramlist).fetchall()
            if len(results) > 0:
                df = pd.DataFrame(results, columns=columnlist)
            success = "Correct"
    except Exception as e:
        success = str(e)
    closedbconnection(conn)
    return df, success


'''
def getcustomernameapi():
    customerdf, success = readcustomer()
    out = customerdf.to_json(orient='records')
    return out+success
'''


def getMaxTipPercentage(trip_year, trip_quarter):
    paramlist = []
    if trip_year is not None:
        paramlist.append(trip_year)
    if trip_quarter is not None:
        paramlist.append(trip_quarter)
    procname = 'sp_YelloTaxiTipInsight'
    columnlist = ['trip_year', 'trip_quarter', 'max_tip_pctg']
    dftippercentage, success = readusingsqldbproc(procname, paramlist, columnlist)
    ret_json_str = ''
    if len(dftippercentage.head(1)) > 0:
        dftippercentage['max_tip_pctg'] = \
            dftippercentage['max_tip_pctg'].round().astype('int')
        dftippercentage = dftippercentage.rename(
            columns={'max_tip_pctg': 'maxTipPercentage', 'trip_quarter': 'quarter'})
        if dftippercentage.shape[0] == 1:
            dftippercentage = dftippercentage.drop(['trip_year', 'quarter'], axis=1)
            ret_json_str = dftippercentage.to_json(orient='records')
        else:
            j = dftippercentage.groupby(['trip_year'], as_index=False) \
                .apply(lambda x: x[['quarter', 'maxTipPercentage']].to_dict('r')) \
                .reset_index().drop('index', axis=1) \
                .rename(columns={0: 'maxTipPercentages'}) \
                .to_json(orient='records')
            ret_json_str = json.dumps(json.loads(j), indent=2, sort_keys=False)

    else:
        ret_json_str = "No data found for this criteria"

    return ret_json_str[1:-1]


def getMaxTripSpeed(trip_year, trip_month, trip_day):
    paramlist = []
    if trip_year != None:
        paramlist.append(trip_year)
    if trip_month != None:
        paramlist.append(trip_month)
    if trip_day != None:
        paramlist.append(trip_day)
    procname = 'sp_YelloTaxiTripSpeedInsight'
    columnlist = ['trip_year', 'trip_hour_of_day', 'max_trip_speed']
    dftripspeed, success = readusingsqldbproc(procname, paramlist, columnlist)
    ret_json_str = ''
    if len(dftripspeed.head(1)) > 0:
        dftripspeed['max_trip_speed'] = \
            dftripspeed['max_trip_speed'].round().astype('int')
        dftripspeed = dftripspeed.rename(
            columns={'max_trip_speed': 'maxSpeed', 'trip_hour_of_day': 'hour'})
        dftripspeed['hour'] = dftripspeed['hour']+1
        dftripspeed = dftripspeed.sort_values(by=['hour'])
        j = dftripspeed.groupby(['trip_year'], as_index=False) \
            .apply(lambda x: x[['hour', 'maxSpeed']].to_dict('r')) \
            .reset_index().drop('index', axis=1) \
            .rename(columns={0: 'tripSpeeds'}) \
            .to_json(orient='records')
        ret_json_str = json.dumps(json.loads(j), indent=2, sort_keys=False)
    else:
        ret_json_str = "No data found for this criteria"
    return ret_json_str[1:-1]
