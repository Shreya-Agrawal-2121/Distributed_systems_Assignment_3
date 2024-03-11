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
    
    #TODO: Add error handling like same column name or same shard name ,etc
    # because marks will be deducted for that
    # take it seriously

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
    #TODO: what if shard don't exist
    data = request.get_json()
    shards = data['shards']
    response_message = {}
    for shard in shards:
        response = []
        query(f"USE {shard}")
        query(f"CREATE TABLE StudT LIKE StudT")  #@sherya: What are u doing here,please put light on it?
        result = query(f"SELECT * FROM StudT")
        for row in result:
            response.append(row)
        response_message[shard] = response
    response_message["status"] = "successful"    
    return jsonify(response_message), 200



@app.route('/read', methods=['POST'])
def read():
    #TODO: what if shard don't exist
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

    # check if size == curr_idx
    result = query(f"SELECT COUNT(*) FROM StudT")
    if result[0][0] != curr_idx:
        response_data = {
            "message": "Size does not match",
            "status": "failed"
        }
        return jsonify(response_data), 500

    cnt = 0

    for row in stud_data:
        # check if student id exists
        result = query(f"SELECT * FROM StudT WHERE id = {row[0]}")
        if len(result) == 0:
            query(f"INSERT INTO StudT VALUES {tuple(row)}")
            cnt += 1
    
    response_data = {
        "message": "Data entries added",
        "current_idx": curr_idx + cnt,
        "status": "success"
    }
    if cnt==0:
        response_data = {
            "message": "All Data entries already exists",
            "current_idx": curr_idx,
            "status": "failed"
        }
        return jsonify(response_data), 500
    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
def update():
    data = request.get_json()
    shard = data['shard']
    stud_id = data['stud_id']
    new_data = data['new_data']

    # check if student id matches with new data
    if new_data[0] != stud_id:
        response_data = {
            "message": "Student id does not match with new data",
            "status": "failed"
        }
        return jsonify(response_data), 500

    # if shard does not exist
    result = query(f"SHOW DATABASES LIKE '{shard}'")
    if len(result) == 0:
        response_data = {
            "message": "Shard does not exist",
            "status": "failed"
        }
        return jsonify(response_data), 500

    query(f"USE {shard}")
    result = query(f"SELECT * FROM StudT WHERE id = {stud_id}")
    if len(result) == 0:
        response_data = {
            "message": "Student id does not exist",
            "status": "failed"
        }
        return jsonify(response_data), 500
    
    query(f"UPDATE StudT SET {new_data} WHERE id = {stud_id}")

    response_data = {
        "message": f"Data entry for Stud_id:{stud_id} updated",
        "status": "success"
    }
    return jsonify(response_data), 200


@app.route('/del', methods=['DELETE'])
def delete():
    data = request.get_json()
    shard = data['shard']
    stud_id = data['stud_id']

    # if shard does not exist
    result = query(f"SHOW DATABASES LIKE '{shard}'")
    if len(result) == 0:
        response_data = {
            "message": "Shard does not exist",
            "status": "failed"
        }
        return jsonify(response_data), 500

    query(f"USE {shard}")
    result = query(f"SELECT * FROM StudT WHERE id = {stud_id}")
    if len(result) == 0:
        response_data = {
            "message": "Student id does not exist",
            "status": "failed"
        }
        return jsonify(response_data), 500
    
    query(f"DELETE FROM StudT WHERE id = {stud_id}")
    response_data = {
        "message": f"Data entry with Stud_id:{stud_id} removed",
        "status": "success"
    }
    return jsonify(response_data), 200


#TODO: Please add mutex lock in write,del,update,read,etc

# main function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
