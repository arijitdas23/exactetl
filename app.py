import flask
import os
from flask import Flask,request,abort

app = Flask(__name__)

@app.route('/api/', methods=['GET'])
def rootapi():
    db_user_nme = os.environ.get('SQL_USER_NAME')
    return "Hello World"+db_user_nme

if __name__=='__main__':
    app.run(debug=True,host='0.0.0.0')