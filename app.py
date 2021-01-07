import flask
import os
from flask import Flask,request,abort
from yellowtaxiapi import *

app = Flask(__name__)

@app.route('/api/', methods=['GET'])
def rootapi():
    # comment
    retstr = getcustomernameapi()
    return "Hello World "+retstr

if __name__=='__main__':
    app.run(debug=True,host='0.0.0.0')