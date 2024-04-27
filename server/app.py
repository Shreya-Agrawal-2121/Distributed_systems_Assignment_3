from flask import Flask, jsonify, request, redirect
import os
import docker
import requests
import sqlite3
import mysql.connector
import asyncio
import aiohttp
client = docker.from_env()
network = "n1"
image = "server"
mysql_container = client.containers.get("mysql_db")
mysql_ip = mysql_container.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
cnx = mysql.connector.connect(user='root', password='test',
                              host=mysql_ip,
                              )
cursor = cnx.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS meta_data")
cursor.execute("USE meta_data")
app = Flask(__name__)
log = 0
# Get server ID from environment variable
server_id = os.environ.get('SERVER_ID')
server_name = os.environ.get('SERVER_NAME')
is_primary_server = 0
db = []
column_list = ""
columns = []
dtypes = []
WALs = {}
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
# Home endpoint
async def write_to_primary(shard, data):
    sm = client.containers.get("sm")
    sm_ip = sm.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
    response = requests.get(f"http://{sm_ip}:5001/secondary?shard={shard}")
    secondary_servers = response.json()
    WAL_p = WALs[shard]
    send_data = {
        "shard":shard,
        "is_primary_server":0,
        "WAL_p":WAL_p
    }
    async with aiohttp.ClientSession() as session:
        tasks = []

        for server in secondary_servers:
            # send data to secondary servers
            serv = client.containers.get(server)
            server_ip = serv.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            tasks.append(asyncio.create_task(session.post(
                f'http://{server_ip}:5000/write', json=send_data)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        cnt = 0
        for result in results:
            if not isinstance(result, Exception) and result.status == 200:
                cnt += 1
            else:
                app.logger.error(
                    f"Error while writing to secondary server: {result}")
        if cnt < len(secondary_servers)/2:
            response_data = {
                "message": "Write failed to secondary servers",
                "status": "failed"
            }
            return jsonify(response_data), 500
    shard_db = f"{shard}.db"
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    cnt = 0
    # open the log and make changes to be made in the log_file serve_id.log
    for row in data:
        # check if student id exists
        values = list(row.values())
        result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {values[0]}", shard_db)
        if len(result) == 0:
            # also mention shard_id in the log file
            query(f"INSERT INTO StudT {tuple(columns)} VALUES {tuple(values)}", shard_db)
            cnt += 1
    query(f"DETACH DATABASE '{shard}'", shard_db)
    mydb.close()
    response_data = {
        "message": "Data entries added",
        "status": "success"
    }
    if cnt==0:
        response_data = {
            "message": "All Data entries already exists",
            "status": "failed"
        }
        return jsonify(response_data), 500
    return jsonify(response_data), 200
    
def execute_tranx(entry, shard):
    shard_db = f"{shard}.db"
    if entry['type'] == 'write':
        data = entry['data']
        cnt = 0
        for row in data:
        # check if student id exists
            values = list(row.values())
            result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {values[0]}", shard_db)
            if len(result) == 0:
                # also mention shard_id in the log file
                query(f"INSERT INTO StudT {tuple(columns)} VALUES {tuple(values)}", shard_db)
                cnt += 1

async def write_to_secondary(shard, WAL_p):
    WAL = WALs[shard]
    shard_db = f"{shard}.db"
    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    for idx,entry in enumerate(WAL):
        if WAL['log'] >= WAL_p[0]['log']:
            WAL = WAL[idx:]
            break
    if len(WAL) > 0 and WAL[-1]['log'] < WAL_p[0]['log']:
        WAL = []
    WALs[shard] = WAL
    greater_idx = -1
    for idx, entry in enumerate(WAL_p):
        if len(WAL) == 0 or WAL[-1]['log'] < entry['log']:
            greater_idx = idx
            break
    if greater_idx != -1:
        new_logs = WAL_p[greater_idx:]
        for entry in new_logs:
            WAL.append(entry)
            execute_tranx(entry=entry)
    WALs[shard] = WAL
    query(f"DETACH DATABASE '{shard}'", shard_db)
    return jsonify({"message":"All operations executed successfully", "status":"successful"}), 200

@app.route('/config', methods=['POST'])
def config():
    
    global columns, dtypes, column_list, mydb
    data = request.get_json()
    schema = data['schema']
    shards = data['shards']
    columns = schema['columns']
    dtypes = schema['dtypes']
    if len(columns) != len(dtypes):
        response_data = {
            "message": "Columns and datatypes do not match",
            "status": "failed"
        }
        return jsonify(response_data), 500
    if len(shards) != len(set(shards)):
        response_data = {
            "message": "Shard names are not unique",
            "status": "failed"
        }
        return jsonify(response_data), 500
    if len(columns) != len(set(columns)):
        response_data = {
            "message": "Column names are not unique",
            "status": "failed"
        }
        return jsonify(response_data), 500
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
    global columns, dtypes, column_list

    data = request.get_json()
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
    return jsonify(response_message), 200
    


@app.route('/read', methods=['POST'])
def read():
    global columns, dtypes, column_list
    data = request.get_json()
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
    return jsonify(response_data), 200


@app.route('/write', methods=['POST'])
async def write():
    global columns, dtypes, column_list, is_primary_server
    data = request.get_json()
    shard = data['shard']
    stud_data = None
    if 'data' in data.keys():
        stud_data = data['data']
    shard_db = f"{shard}.db"
    is_primary_server = None
    WAL_p = None
    if 'is_primary_server' in data.keys():
        is_primary_server = data['is_primary_server']
    if 'WAL_p' in data.keys():
        WAL_p = data['WAL_p']
    if is_primary_server is None:
        if shard in WALs.keys():
            WALs[shard].append({'log':log,'type':'write','data':data})
        else:
            WALs[shard] = []
            WALs[shard].append({'log':log,'type':'write','data':data})
        return await write_to_primary(shard, stud_data)
    
    return await write_to_secondary(shard, WAL_p)


@app.route('/update', methods=['PUT'])
def update():
    global columns, dtypes, column_list, is_primary_server

    data = request.get_json()
    shard = data['shard']
    stud_id = data['Stud_id']
    data = data['data']
    is_primary_server = data['is_primary_server']
    # check if student id matches with new data
    if list(data.values())[0] != stud_id:
        response_data = {
            "message": "Student id does not match with new data",
            "status": "failed"
        }
        return jsonify(response_data), 500


    # if shard does not exist
    # result = query(f"SHOW DATABASES LIKE '{shard}'")
    # if len(result) == 0:
    #     response_data = {
    #         "message": "Shard does not exist",
    #         "status": "failed"
    #     }
    #     return (response_data), 500

    shard_db = f"{shard}.db"
    

    if is_primary_server is None:
        # send request with shard_id to shard manager for secondary server list
        sm = client.containers.get("sm")
        sm_ip = sm.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
        response = requests.get(f"http://{sm_ip}:5001/secondary?shard={shard}")
        secondary_servers = response.json()
        maj_cnt=0
        for server in secondary_servers:
            # send data to secondary servers
            serv = client.containers.get(server)
            server_ip = serv.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            requests.post(f"http://{server_ip}:5000/write", json=data)
            # check if successful
            if response.status_code == 200:
                maj_cnt += 1
        if maj_cnt < len(secondary_servers)/2:
            # how to remove the successful writes #TODO
            response_data = {
                "message": "Update failed",
                "status": "failed"
            }
            return jsonify(response_data), 500

    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)

    # open the log and make changes to be made in the log_file serve_id.log
    with open(f"{server_id}.log", "a") as log_file:
        
        result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {stud_id}", shard_db)
        if len(result) == 0:
            response_data = {
                "message": "Student id does not exist",
                "status": "failed"
            }
            return jsonify(response_data), 500
        updated_data = ""
        for key, value in data.items():
            updated_data += f"{key} = '{value}', "
        updated_data = updated_data[:-2]
        log_file.write(f"{shard}: UPDATE StudT SET {data} WHERE Stud_id = {stud_id}\n")
        query(f"UPDATE StudT SET {updated_data} WHERE Stud_id = {stud_id}", shard_db)    
        log_file.close()

    query(f"DETACH DATABASE '{shard}'", shard_db)
    mydb.close()
    response_data = {
        "message": f"Data entry for Stud_id:{stud_id} updated",
        "status": "success"
    }
    return jsonify(response_data), 200


@app.route('/del', methods=['DELETE'])
def delete():
    global columns, dtypes, column_list, is_primary_server
    data = request.get_json()
    shard = data['shard']
    stud_id = data['Stud_id']

    # if shard does not exist
    # result = query(f"SHOW DATABASES LIKE '{shard}'")
    # if len(result) == 0:
    #     response_data = {
    #         "message": "Shard does not exist",
    #         "status": "failed"
    #     }
    #     return (response_data), 500

    shard_db = f"{shard}.db"
    is_primary_server = 0
    sm = client.containers.get("sm")
    sm_ip = sm.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
    cursor.execute(f"SELECT Primary_server FROM MapT WHERE Shard_id = '{shard}' AND Server_id = '{server_id}'")
    result = cursor.fetchall()
    is_primary_server = result[0][0]
    if is_primary_server == 1:
        # send request with shard_id to shard manager for secondary server list
        response = requests.get(f"http://{sm_ip}:5001/secondary?shard={shard}")
        secondary_servers = response.json()
        maj_cnt=0
        for server in secondary_servers:
            # send data to secondary servers
            serv = client.containers.get(server)
            server_ip = serv.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            requests.post(f"http://{server_ip}:5000/write", json=data)
            # check if successful
            if response.status_code == 200:
                maj_cnt += 1
        if maj_cnt < len(secondary_servers)/2:
            # how to remove the successful writes #TODO
            response_data = {
                "message": "Delete failed",
                "status": "failed"
            }
            return jsonify(response_data), 500
        


    query(f"ATTACH DATABASE '{shard_db}' as '{shard}'", shard_db)
    
    # open the log and make changes to be made in the log_file serve_id.log
    with open(f"{server_id}.log", "a") as log_file:
        
    
        result = query(f"SELECT {column_list} FROM StudT WHERE Stud_id = {stud_id}", shard_db)
        if len(result) == 0:
            response_data = {
                "message": "Student id does not exist",
                "status": "failed"
            }
            return jsonify(response_data), 500
        log_file.write(f"{shard}: DELETE FROM StudT WHERE Stud_id = {stud_id}\n")
        query(f"DELETE FROM StudT WHERE Stud_id = {stud_id}", shard_db)
        
    query(f"DETACH DATABASE '{shard}'", shard_db)
    mydb.close()
    response_data = {
        "message": f"Data entry with Stud_id:{stud_id} removed",
        "status": "success"
    }
    return jsonify(response_data), 200


# # Home endpoint
# @app.route('/home', methods=['GET'])
# def home():
#     # hello message
#     response_data = {
#         "message": f"Hello from Server: {server_id}",
#         "status": "successful"
#     }
#     return jsonify(response_data), 200


# main function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
