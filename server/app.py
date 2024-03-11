from flask import Flask, jsonify, request, redirect
import os
import mysql.connector
from multiprocessing.dummy import Pool

from .helper import DataHandler, SQLHandler
app = Flask(__name__)

# Get server ID from environment variable
server_id = os.environ.get('SERVER_ID')
server_name = os.environ.get('SERVER_NAME')
db = []
mydb = mysql.connector.connect(host='localhost',user='root',password='abc')
# Home endpoint
def query(sql):
    try:
            cursor = mydb.cursor()
            cursor.execute(sql)
    except Exception:
            mydb = mysql.connector.connect(host='localhost',user='root',password='abc')

            cursor = mydb.cursor()
            cursor.execute(sql)
    res=cursor.fetchall()
    cursor.close()
    mydb.commit()
    return res


@app.route('/config', methods=['POST'])
def config():
    data = request.get_json()
    schema = data['schema']
    shards = data['shards']
    columns = schema['columns']
    dtypes = schema['dtypes']
    try:
        message = ""
        dmap={'Number':'INT','String':'VARCHAR(512)'}
        col_config=''
        for c,d in zip(columns,dtypes):
            col_config+=f", {c} {dmap[d]}"
        for shard in shards:
            # Test this line
            query(f"CREATE DATABASE {shard}")
            query(f"USE {shard}")
            query(f"CREATE TABLE StudT ({col_config})")
            message = message + server_name + ":"+ shard + ","
        message = message + " configured"
        response_data = {
            "message": message,
            "status": "successful"
        }
        return jsonify(response_data), 200
    except Exception as e:
        response_data = {
            "message": str(e),
            "status": "failed"
        }
        return jsonify(response_data), 500

# Heartbeat endpoint
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # no message 
    response_data = {
        "message": "",
        "status": "successful"
    }
    return jsonify(response_data), 200

@app.route('/copy', methods=['GET'])
def copy():
    data = request.get_json()
    shards = data['shards']
    response_message = {}
    for shard in shards:
        response = []
        query(f"USE {shard}")
        query(f"CREATE TABLE StudT LIKE StudT")
        result = query(f"SELECT * FROM StudT")
        for row in result:
            response.append(row)
        response_message[shard] = response
    response_message["status"] = "successful"    
    return jsonify(response_message), 200



@app.route('/read', methods=['POST'])
def read():
    data = request.get_json()
    shard = data['shard']
    Stud_id = data['Stud_id']
    low = Stud_id['low']
    high = Stud_id['high']
    query(f"USE {shard}")
    response = []
    for id in range(low, high + 1):
        result = query(f"SELECT * FROM StudT WHERE id = {id}")
        response.append(result)

    response_data = {
        "data": response,
        "status": "success"
    
    }
    return jsonify(response_data), 200


@app.route('/write', methods=['POST'])
def write():
    data = request.get_json()
    shard = data['shard']
    curr_idx = data['curr_idx']
    stud_data = data['data']
    query(f"USE {shard}")
    #TODO: Complete this function

@app.route('/update', methods=['PUT'])
def update():
    pass


@app.route('/del', methods=['DELETE'])
def delete():
    pass


# main function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
