import pandas as pd
import json
from sqlfunctions import *


def validateNumericInput(input_param):
    """Generic method to validate input parameters are numric or not

      Args:
        input_param : input parameter

      Returns:
        boolean:  True or False

      Raises:
        ValueError
    """
    if input_param.isnumeric():
        return input_param
    else:
        raise ValueError("One or more input parameter(s) are non numeric")


def getMaxTipPercentage(trip_year, trip_quarter):
    """API function to serve the request to get Max Tip percentage from SQLDB.
       Get the data as dataframe and covert it to JSON/Nested
       JSON and return back to caller

      Args:
        trip_year : trip year
        trip_quarter : Quarter of the year

      Returns:
        ret_json_str:  required response in JSON string

      Raises:
        None
    """
    paramlist = []
    status = 0
    try:
        # Dynamically add parameter for SP call
        if trip_year is not None:
            paramlist.append(validateNumericInput(trip_year))
        if trip_quarter is not None:
            paramlist.append(validateNumericInput(trip_quarter))
        procname = 'sp_YelloTaxiTipInsight'
        # Return column list
        columnlist = ['trip_year', 'trip_quarter', 'max_tip_pctg']
        # Read data from DB as per requested criteria
        dftippercentage, success = readusingsqldbproc(procname, paramlist, columnlist)
        ret_json_str = ''
        if len(dftippercentage.head(1)) > 0 and len(paramlist) > 0:
            # Rounding off to next digit
            dftippercentage['max_tip_pctg'] = \
                dftippercentage['max_tip_pctg'].round().astype('int')
            # Rename as per response contract
            dftippercentage = dftippercentage.rename(
                columns={'max_tip_pctg': 'maxTipPercentage', 'trip_quarter': 'quarter'})
            # Convert the Response Dataframe to JSON String
            if dftippercentage.shape[0] == 1:
                dftippercentage = dftippercentage.drop(['trip_year', 'quarter'], axis=1)
                ret_json_str = dftippercentage.to_json(orient='records')[1:-1]
            else:
                j = dftippercentage.groupby(['trip_year'], as_index=False) \
                    .apply(lambda x: x[['quarter', 'maxTipPercentage']].to_dict('r')) \
                    .reset_index().drop('index', axis=1) \
                    .rename(columns={0: 'maxTipPercentages'}) \
                    .to_json(orient='records')
                ret_json_str = json.dumps(json.loads(j), indent=2, sort_keys=False)[1:-1]
        else:
            raise ValueError("No data found for this criteria or minimal parameter is missing")
    except Exception as exp:
        expStr = repr(exp)
        res_dict = "{'error':"+ expStr+"}"
        ret_json_str = json.loads(json.dumps(res_dict,indent=2, sort_keys=False))
        status = 1

    return ret_json_str,status


def getMaxTripSpeed(trip_year, trip_month, trip_day):
    """API function to serve the request to get Max Trip Speed from SQLDB.
       Get the data as dataframe and covert it to JSON/Nested
       JSON and return back to caller

      Args:
        trip_year : trip year
        trip_month : month of the trip year
        trip_day : the day of the trip month


      Returns:
        ret_json_str:  required response in JSON string

      Raises:
        None
    """
    paramlist = []
    status = 0
    try:
        # Dynamically add parameter for SP call
        if trip_year is not None:
            paramlist.append(validateNumericInput(trip_year))
        if trip_month is not None:
            paramlist.append(validateNumericInput(trip_month))
        if trip_day is not None:
            paramlist.append(validateNumericInput(trip_day))
        procname = 'sp_YelloTaxiTripSpeedInsight'
        # Return column list
        columnlist = ['trip_year', 'trip_hour_of_day', 'max_trip_speed']
        # Read data from DB as per requested criteria
        dftripspeed, success = readusingsqldbproc(procname, paramlist, columnlist)
        ret_json_str = ''
        if len(dftripspeed.head(1)) > 0 and len(paramlist) > 0:
            # Rounding off to next digit
            dftripspeed['max_trip_speed'] = \
                dftripspeed['max_trip_speed'].round().astype('int')
            # Rename as per response contract
            dftripspeed = dftripspeed.rename(
                columns={'max_trip_speed': 'maxSpeed', 'trip_hour_of_day': 'hour'})
            dftripspeed['hour'] = dftripspeed['hour'] + 1
            dftripspeed = dftripspeed.sort_values(by=['hour'])
            # Convert the Response Dataframe to JSON String
            j = dftripspeed.groupby(['trip_year'], as_index=False) \
                .apply(lambda x: x[['hour', 'maxSpeed']].to_dict('r')) \
                .reset_index().drop('index', axis=1) \
                .rename(columns={0: 'tripSpeeds'}) \
                .to_json(orient='records')
            ret_json_str = json.dumps(json.loads(j), indent=2, sort_keys=False)[1:-1]
        else:
            raise ValueError("No data found for this criteria or minimal parameter is missing")
    except Exception as exp:
        expStr = repr(exp)
        res_dict = "{'error':"+ expStr+"}"
        ret_json_str = json.loads(json.dumps(res_dict,indent=2, sort_keys=False))
        status = 1
    return ret_json_str,status
