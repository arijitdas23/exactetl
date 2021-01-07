import flask
from flask import Flask,request,abort

app = Flask(__name__)

@app.route('/api/', methods=['GET'])
def rootapi():
    return "Hello World"

if __name__=='__main__':
    app.run(debug=True,host='0.0.0.0')