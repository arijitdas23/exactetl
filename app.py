import flask
import os
from flask import Flask,request,abort
from yellowtaxiapi import *

app = Flask(__name__)

@app.route('/')
@app.route('/hello')
def helloworld():
    return "HelloWorld"

@app.route('/api/tip/<trip_year>/<max>', defaults={'trip_quarter': None}, methods=['GET'])
@app.route('/api/tip/<trip_year>/<trip_quarter>/<max>', methods=['GET'])
def maxtippctgapi(trip_year,trip_quarter,max):
    # comment
    retstr = getMaxTipPercentage(trip_year, trip_quarter)
    return retstr

@app.route('/api/speed/<trip_year>/<max>', defaults={'trip_month': 1,'trip_day':1}, methods=['GET'])
@app.route('/api/speed/<trip_year>/<trip_month>/<max>', defaults={'trip_day':1}, methods=['GET'])
@app.route('/api/tip/<trip_year>/<trip_month>/<trip_day>/<max>', methods=['GET'])
def maxtripspeedapi(trip_year,trip_month,trip_day,max):
    # comment
    retstr = getMaxTripSpeed(trip_year, trip_month, trip_day)
    return retstr

if __name__=='__main__':
    app.run(debug=True,host='0.0.0.0')