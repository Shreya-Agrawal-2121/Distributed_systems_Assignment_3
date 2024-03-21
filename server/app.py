from flask import Flask, jsonify, request, redirect
import os
import mysql.connector
from multiprocessing.dummy import Pool
import sqlite3
app = Flask(__name__)

# Get server ID from environment variable
server_id = os.environ.get('SERVER_ID')
server_name = os.environ.get('SERVER_NAME')
db = []
column_list = ""
columns = []
dtypes = []

# Home endpoint
def query(sql, database):
    global mydb
    try:
            cursor = mydb.cursor()
            cursor.execute(sql)
    except Exception:
            mydb = sqlite3.connect(database)

            cursor = mydb.cursor()
            cursor.execute(sql)
    res=cursor.fetchall()
    cursor.close()
    mydb.commit()
    return res


@app.route('/config', methods=['POST'])
def config():
    
    global columns, dtypes, column_list, mydb
    data = {
"schema":{"columns":["Stud_id","Stud_name","Stud_marks"],

"dtypes":["Number","String","String"]},

"shards":["sh1","sh2"]
 }
    schema = data['schema']
    shards = data['shards']
    columns = schema['columns']
    dtypes = schema['dtypes']
    if len(columns) != len(dtypes):
        response_data = {
            "message": "Columns and datatypes do not match",
            "status": "failed"
        }
        return (response_data), 500
    if len(shards) != len(set(shards)):
        response_data = {
            "message": "Shard names are not unique",
            "status": "failed"
        }
        return (response_data), 500
    if len(columns) != len(set(columns)):
        response_data = {
            "message": "Column names are not unique",
            "status": "failed"
        }
        return (response_data), 500
    column_list = ",".join(columns)
    try:
        message = ""
        dmap={'Number':'INT','String':'VARCHAR(512)'}
        col_config=''
        for c,d in zip(columns,dtypes):
            col_config+=f", {c} {dmap[d]}"
        for shard in shards:
            # Test this line
            shard_db = f"{shard}.db"
            print(shard_db)
            mydb = sqlite3.connect(shard_db)
            query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
            query(f"CREATE TABLE StudT (id INT AUTO_INCREMENT PRIMARY KEY{col_config})", shard_db)
            query(f"DETACH DATABASE '{shard}'", shard_db)
            message = message + str(server_name) + ":"+ str(shard) + ","
            mydb.close()
        message = message[:-1]
        message = message + " configured"
        response_data = {
            "message": message,
            "status": "successful"
        }
        return (response_data), 200
    except Exception as e:
        response_data = {
            "message": str(e),
            "status": "failed"
        }
        return (response_data), 500

# Heartbeat endpoint
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # no message 
    response_data = {
        "message": "",
        "status": "successful"
    }
    return (response_data), 200

@app.route('/copy', methods=['GET'])
def copy():
    global columns, dtypes, column_list

    data = {
"shards":["sh1","sh2"]
    }
    shards = data['shards']
    global column_list
    response_message = {}
    for shard in shards:
        response = []
        shard_db = f"{shard}.db"    
        # result = query(f"SHOW DATABASES LIKE '{shard}'", shard_db)
        # if(len(result) == 0):
        #     continue
        query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
        result = query(f"SELECT {column_list} FROM StudT", shard_db)
        query(f"DETACH DATABASE '{shard}'", shard_db)
        mydb.close()
        for row in result:
            res = {}
            for i, column in enumerate(columns):
                res[column] = row[i]
            response.append(res)
        response_message[shard] = response
    response_message["status"] = "successful"    
    return (response_message), 200



@app.route('/read', methods=['POST'])
def read():
    global columns, dtypes, column_list
    data = {
    "shard":"sh1",
    "Stud_id":{"low":1,"high":3}
    }
    shard = data['shard']
    Stud_id = data['Stud_id']
    low = Stud_id['low']
    high = Stud_id['high']
    shard_db = f"{shard}.db"
    # result = query(f"SHOW DATABASES LIKE '{shard}'")
    # if(len(result) == 0):
    #     response_data = {
    #         "message": "Shard does not exist",
    #         "status": "failed"
    #     }
    #     return (response_data), 500
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    response = []
    for id in range(low, high + 1):
        result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {id}", shard_db)
        if len(result) == 0:
            continue
        for row in result:
            res = {}
            for i, column in enumerate(columns):
                res[column] = row[i]
            response.append(res)
    query(f"DETACH DATABASE '{shard}'", shard_db)
    response_data = {
        "data": response,
        "status": "success"
    
    }
    mydb.close()
    return (response_data), 200


@app.route('/write', methods=['POST'])
def write():
    global columns, dtypes, column_list
    data = {
    "shard":"sh1",
    "curr_idx":0,
    "data":[{"Stud_id":1,"Stud_name":"John","Stud_marks":"A"},
            {"Stud_id":2,"Stud_name":"Doe","Stud_marks":"B"},
            {"Stud_id":3,"Stud_name":"Jane","Stud_marks":"C"}]
    }
    shard = data['shard']
    curr_idx = data['curr_idx']
    stud_data = data['data']
    shard_db = f"{shard}.db"
    
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)

    # check if size == curr_idx
    result = query(f"SELECT COUNT(*) FROM StudT", shard_db)
    if result[0][0] != curr_idx:
        response_data = {
            "message": "Size does not match",
            "status": "failed"
        }
        return (response_data), 500

    cnt = 0

    for row in stud_data:
        # check if student id exists
        values = list(row.values())
        result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {values[0]}", shard_db)
        if len(result) == 0:
            query(f"INSERT INTO StudT {tuple(columns)} VALUES {tuple(values)}", shard_db)
            cnt += 1
    query(f"DETACH DATABASE '{shard}'", shard_db)
    mydb.close()
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
        return (response_data), 500
    return (response_data), 200


@app.route('/update', methods=['PUT'])
def update():
    global columns, dtypes, column_list

    data = {
    "shard":"sh1",
    "Stud_id":1,
    "new_data":{"Stud_id":1,"Stud_name":"John","Stud_marks":"A+"}
    
    }
    shard = data['shard']
    stud_id = data['Stud_id']
    new_data = data['new_data']

    # check if student id matches with new data
    if list(new_data.values())[0] != stud_id:
        response_data = {
            "message": "Student id does not match with new data",
            "status": "failed"
        }
        return (response_data), 500

    # if shard does not exist
    # result = query(f"SHOW DATABASES LIKE '{shard}'")
    # if len(result) == 0:
    #     response_data = {
    #         "message": "Shard does not exist",
    #         "status": "failed"
    #     }
    #     return (response_data), 500

    shard_db = f"{shard}.db"
    
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {stud_id}", shard_db)
    if len(result) == 0:
        response_data = {
            "message": "Student id does not exist",
            "status": "failed"
        }
        return (response_data), 500
    updated_data = ""
    for key, value in new_data.items():
        updated_data += f"{key} = '{value}', "
    updated_data = updated_data[:-2]
    query(f"UPDATE StudT SET {updated_data} WHERE Stud_id = {stud_id}", shard_db)
    query(f"DETACH DATABASE '{shard}'", shard_db)
    mydb.close()
    response_data = {
        "message": f"Data entry for Stud_id:{stud_id} updated",
        "status": "success"
    }
    return (response_data), 200


@app.route('/del', methods=['DELETE'])
def delete():
    global columns, dtypes, column_list
    data = {
    "shard":"sh1",
    "stud_id":1
    }
    shard = data['shard']
    stud_id = data['stud_id']

    # if shard does not exist
    # result = query(f"SHOW DATABASES LIKE '{shard}'")
    # if len(result) == 0:
    #     response_data = {
    #         "message": "Shard does not exist",
    #         "status": "failed"
    #     }
    #     return (response_data), 500

    shard_db = f"{shard}.db"
    
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {stud_id}", shard_db)
    if len(result) == 0:
        response_data = {
            "message": "Student id does not exist",
            "status": "failed"
        }
        return (response_data), 500
    
    query(f"DELETE FROM StudT WHERE Stud_id = {stud_id}", shard_db)
    query(f"DETACH DATABASE '{shard}'", shard_db)

    response_data = {
        "message": f"Data entry with Stud_id:{stud_id} removed",
        "status": "success"
    }
    return (response_data), 200


# main function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
