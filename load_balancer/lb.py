from flask import Flask, jsonify, request, redirect
import docker
import os
import random
import requests
import subprocess
from consistentHashing import ConsistentHashing
import time
from threading import Thread
from threading import Lock
import sqlite3
import mysql.connector
import socket

# Initialize the Flask application
app = Flask(__name__)
mydb = None
# Initialize the ConsistentHashing class
heartbeat_ptr=0
hashmaps = {}
# Initialize the Docker client
client = docker.from_env()
network = "n1"
image = "server"
mysql_container = client.containers.get("mysql_db")
mysql_ip = mysql_container.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
cnx = mysql.connector.connect(user='root', password='test',
                              host=mysql_ip)
cursor = cnx.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS meta_data")
cursor.execute("USE meta_data")
cnx.commit()
# Initialize the server_id to hostname and hostname to server_id mapping
server_id_to_host = {}
server_host_to_id = {}
shardT = {}
#TODO: update mapT to store server name along with primary server info
mapT = {}
N = 0
schema = {}
locks = {}
def write_to_servers(*args):
    shard = args[0]
    data = args[1]
    lock = locks[shard]
    lock.acquire()
    cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}'  AND Primary_server = 1")
    result = cursor.fetchall()
    if len(result) == 0:
        lock.release()
        return
    server = result[0][0]
    try:
        container = client.containers.get(server)
        ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
        url_redirect = f'http://{ip_addr}:5000/write'
        requests.post(url_redirect, json={"data":data,"shard":shard})
    except Exception as e:
        print(e)
        response_data = {'message': '<Error> Failed to write to server', 
                        'status': 'failure'}
        lock.release()
        return jsonify(response_data), 400
    lock.release()

@app.route('/init', methods=['POST'])
def init_server():
    global N, schema, mydb
    data = request.get_json()
    N = data['N']
    schema = data['schema']
    shards = data['shards']
    servers = data['servers']
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ShardT (Stud_id_low INT PRIMARY KEY, Shard_id VARCHAR(512), Shard_size INT)")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS MapT (id INT AUTO_INCREMENT PRIMARY KEY, Shard_id VARCHAR(512), Server_id VARCHAR(512), Primary_server INT)")
    
    cnx.commit()
    keys = list(servers.keys())
    i = 0
    while i < N:
        if i < len(keys):
            server = keys[i]
        else:
            server = "Server#"
        server_id = random.randint(100000, 999999)
        if server_id in server_id_to_host.keys():
            continue
        if i >= len(keys):
            server = server + str(server_id)
            if server in server_host_to_id.keys():
                continue
            keys.append(server)
        try:
                client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id, 'SERVER_NAME': server,
                }, ports={5000:None},
                privileged=True,
                volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}})
        except Exception as e:
                print(e)
                response = {'message': '<Error> Failed to spawn new container', 
                        'status': 'failure'}
                return jsonify(response), 400
        server_id_to_host[server_id] = server
        server_host_to_id[server] = server_id
        
        shards_data = servers[server]
        for shard in shards_data:
            cursor.execute(f"INSERT INTO MapT (Shard_id, Server_id, Primary_server) VALUES ('{shard}', '{server}', 0)")

        i += 1
        time.sleep(1)
    cnx.commit()
    sm = client.containers.get("sm")
    sm_ip = sm.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
    for server in keys:
        server_id = server_host_to_id[server]
        
        post_data = {
            "schema": schema,
            "shards": servers[server]
        }
        try :
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            print(ip_addr)
            headers = {'content-type' : 'application/json'}
            url_redirect = f'http://{ip_addr}:5000/config'
            requests.post(url_redirect, json=post_data, headers=headers)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            
            return jsonify(response_data), 400
        try:
            send_data = {
                "server_id": server_id,
                "server_name": server
            }
            url_redirect = f'http://{sm_ip}:5001/add_server'
            requests.post(url_redirect, json=send_data)
            time.sleep(1)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            return jsonify(response_data), 400
        time.sleep(1)
    for shard in shards:
        values = list(shard.values())
        shard_id = shard['Shard_id']
        cursor.execute(f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size) VALUES {tuple(values)}")
        cmap = ConsistentHashing(3, 512, 9)
        mutex_lock = Lock()
        locks[shard_id] = mutex_lock
        cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard_id}'")
        result = cursor.fetchall()
        print(result)
        if len(result) == 0:
            continue
        server_first = result[0][0]
        cursor.execute(f"UPDATE MapT SET Primary_server = 1 WHERE Shard_id = '{shard_id}' AND Server_id = '{server_first}'")
        for row in result:
            server_id = server_host_to_id[row[0]]
            cmap.add_server(server_id)
        hashmaps[shard_id] = cmap
    response_data = {
        'message' : "Configured database",
        'status' : "successful" 
    }
    cnx.commit()
    return jsonify(response_data), 200

@app.route('/status', methods=['GET'])
def status():
    shards = []
    cursor.execute("SELECT * FROM ShardT")
    result = cursor.fetchall()
    for row in result:
        shard = {
            "Stud_id_low": row[0],
            "Shard_id": row[1],
            "Shard_size": row[2]
        }
        shards.append(shard)
    cursor.execute("SELECT Shard_id, Server_id FROM MapT")
    result = cursor.fetchall()
    servers = {}
    for row in result:
        if row[1] not in servers:
            servers[row[1]] = [row[0]]
        else:
            servers[row[1]].append(row[0])
    response = {
        "N": N,
        "schema": schema,
        "shards": shards,
        "servers": servers
    }
    cnx.commit()
    return jsonify(response), 200

# route /add
@app.route('/add', methods=['POST'])
def add_servers():
    global N, shardT, mapT, schema
    data = request.get_json()
    n = data['n']
    new_shards = data['new_shards']
    servers_new = data['servers']
    if n > len(servers_new):
        response_data = {
            "message" : "<Error> Number of new servers (n) is greater than newly added instances",
            "status" : "failure"
        }
        return jsonify(response_data), 400
    i = 0
    keys = list(servers_new.keys())
    while i < n:
        server = keys[i]
        server_id = random.randint(100000, 999999)
        if server_id in server_id_to_host.keys():
            continue
        try:
                client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id, 'SERVER_NAME': server})
        except Exception as e:
                print(e)
                response = {'message': '<Error> Failed to spawn new docker container', 
                        'status': 'failure'}
                return jsonify(response), 400
        server_id_to_host[server_id] = server
        server_host_to_id[server] = server_id
        post_data = {
            "schema": schema,
            "shards": servers_new[server]
        }
        time.sleep(1)
        try :
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/config'
            requests.post(url_redirect, json=post_data)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            
            return jsonify(response_data), 400
        shard_data = servers_new[server]
        for shard in shard_data:
            cursor.execute(f"INSERT INTO MapT (Shard_id, Server_id, Primary_server) VALUES ('{shard}', '{server}', 0)")
        time.sleep(1)
        N += 1
        i += 1
    for new_shard in new_shards:
        values = list(new_shard.values())
        cursor.execute(f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size) VALUES {tuple(values)}")
    cursor.execute("SELECT Shard_id FROM ShardT")
    result = cursor.fetchall()
    shards = [row[0] for row in result]
    for shard in shards:
        cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}'")
        result = cursor.fetchall()
        servers = [row[0] for row in result]
        num = len(servers)
        cmap = ConsistentHashing(num, 512, 9)
        for server in servers:
            mutex_lock = Lock()
            locks[shard] = mutex_lock
            server_id = server_host_to_id[server]
            cmap.add_server(server_id)
        hashmaps[shard] = cmap
    message = "Add "
    for server in servers_new:
        id = server_host_to_id[server]
        message = message + "Server:" + str(id) + " "
    response = {
        "N": N,
        "message": message,
        "status": "successful"
    }
    cnx.commit()
    return jsonify(response), 200


@app.route('/rm', methods=['DELETE'])
def remove_servers():
    # Get the number of servers to be removed and the hostnames of the servers
    global N
    data = request.get_json()
    n = data['n']
    server_names = data['servers']

    # if n is less than length of hostnames supplied return error
    if(len(server_names) > n):
        response_data = {
            "message" : "<Error> Length of server list is more than removable instances",
            "status" : "failure"
        }
        return jsonify(response_data), 400


    # Get the list of existing server hostnames
    containers = client.containers.list(filters={'network':network})
    container_names = [container.name for container in containers if container.name != "lb"]

    # If number of servers is less than number of removable instances requested return error
    if(len(container_names) < n):
        response_data = {
        "message" : "<Error> Number of removable instances is more than number of replicas",
        "status" : "failure"
    }
        return jsonify(response_data), 400
    
    # Get the number of extra servers to be removed
    random_remove = n - len(server_names)
    extra_servers = list(set(container_names) - set(server_names))
    servers_rm = server_names

    # Randomly sample from extra servers
    servers_rm += random.sample(extra_servers, random_remove)

    # Check if servers requested for removal exist or not
    for server in servers_rm:
        if server not in container_names:
            response_data = {
        "message" : "<Error> At least one of the servers was not found",
        "status" : "failure"
    }
            return jsonify(response_data), 400
        
    # Delete the hash map entry of server id and host names and stop and remove the correpsonding server conatiner
    for server in servers_rm:
        # remove virtual server entries of server from consistent hash map
        

        # remove server from server_id_to_host and server_host_to_id
        server_id = server_host_to_id[server]

        # check if server is primary server for any shard by sending request to shard manager
        # response = requests.get('http://localhost:5000/find_primary', json={'server_id': server_id})
        # response_data = response.json()
        # if response_data['status'] == 'failure':
        #     return jsonify(response_data), 400
        # primary_shards = response_data['data']


        # try to stop and remove server container
        try:
            container = client.containers.get(server)
            container.stop()
            container.remove()
            # if successfully removed, remove server from server_id_to_host and server_host_to_id
            server_host_to_id.pop(server)
            server_id_to_host.pop(server_id)
            cursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = '{server}'")
            result = cursor.execute()
            shards = [row[0] for row in result]
            for shard in shards:
                
                hashmaps[shard].remove_server(server_id)
            cursor.execute(f"DELETE FROM MapT WHERE Server_id = '{server}'")            

            # # send request to shard manager to update primary server for shards
            # for shard in primary_shards:
            #     response = requests.post('http://localhost:5000/update_primary', json={'shard_id': shard})
            #     response_data = response.json()
            #     if response_data['status'] == 'failure':
            #         return jsonify(response_data), 400
            


            time.sleep(1)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to remove docker container', 
                        'status': 'failure'}
            return jsonify(response_data), 400
        
    # Get server containers hostnames and return the response
    containers = client.containers.list(filters={'network':network})
    N = len(containers) - 1
    response_data = {
            "N": len(containers) - 1,
            "replicas": [container.name for container in containers if container.name != "lb"]
        }
    response = {
            "message": response_data,
            "status": "successful"
        }
    return jsonify(response), 200

@app.route('/read', methods=['POST'])
def read():
    data = request.get_json()
    stud_id  = data['Stud_id']
    low = stud_id['low']
    high = stud_id['high']
    shards_range = {}
    result = []
    cursor.execute("SELECT * FROM ShardT")
    result = cursor.fetchall()
    shards = [
        {
            "Stud_id_low": row[0],
            "Shard_id": row[1],
            "Shard_size": row[2]
        } for row in result
    ]
    for shard in shards:
        range_val = (shard['Stud_id_low'], shard['Stud_id_low'] + shard['Shard_size'])
        if max((range_val[0], low)) <= min((range_val[1], high)):
            shards_range[shard['Shard_id']] = (max((range_val[0], low)), min((range_val[1], high)))
    result = []
    for shard in shards_range.keys():
        request_id = random.randint(100000, 999999)
        range_val = shards_range[shard]
        server = hashmaps[shard].get_server_for_request(request_id)
        container = client.containers.get(server_id_to_host[server])
        ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
        url_redirect = f'http://{ip_addr}:5000/read'
        data = {}
        data['shard'] = shard
        data['Stud_id'] = {
            "low": range_val[0],
            "high": range_val[1]
        }
        response = requests.post(url_redirect, json=data)
        if response.status_code != 200:
            return response
        data = response.json()
        result.extend(data['data'])
    response_data = {
        "shards_queried" : list(shards_range.keys()),
        "data": result,
        "status": "successful"
    }
    return jsonify(response_data), 200

#  {
#  "data": [{"Stud_id":2255,"Stud_name":"GHI","Stud_marks":27},
#  {"Stud_id":3524,"Stud_name":"JKBFSFS","Stud_marks":56},
#  {"Stud_id":1005,"Stud_name":"YUBAAD","Stud_marks":100}] /* 100 entries */
#  }

# This endpoint writes data entries in the distributed database. The endpoint expects
# multiple entries that are to be written in the server containers. 
# The load balancer schedules each write to its corresponding
# shard replicas and ensures data consistency using mutex locks for a particular 
# shard and its replicas. The general write
# work flow will be like: (1) Get Shard ids from Stud ids and group writes for 
# each shard → For each shard Do:
# (2a)Take mutex lock for Shard id (m) → (2b) Get all servers (set S) having 
# replicas of Shard id (m) → (2c) Write
# entries in all servers (set S) in Shard id (m) → (2d) Update the valid idx of Shard id (m) in the metadata if
# writes are successful → (2e) Release the mutex lock for Shard id (m). An example request-response pair is shown.
# TODO: havent use mutex anywhere
@app.route('/write', methods=['POST'])
def write():
    data = request.get_json()
    data = data['data']
    cnt=0
    writes = {}

    #TODO : only need to write to primary server


    cursor.execute("SELECT * FROM ShardT")
    result = cursor.fetchall()
    shards = [
        {
            "Stud_id_low": row[0],
            "Shard_id": row[1],
            "Shard_size": row[2]
        } for row in result
    ]
    for entry in data:
        shard_id = ''
        for shard in shards:
            shard_id = shard['Shard_id']
            if entry['Stud_id'] >= int(shard['Stud_id_low']) and entry['Stud_id'] < int(shard['Stud_id_low']) + int(shard['Shard_size']):
                if shard_id not in writes.keys():
                    writes[shard_id] = []
                    writes[shard_id].append(entry)
                else:
                    writes[shard_id].append(entry)
                break
        cnt += 1
    threads = []
    for shard in writes.keys():
        thread = Thread(target=write_to_servers, args=(shard, writes[shard]))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    response_data = {
        "message" : "{} Data entries added".format(cnt),
        "status" : "success"
    }
    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
def update():
    data = request.get_json()
    Stud_id = data['Stud_id']
    req_shard = ''
    new_data = data['data']
    cursor.execute("SELECT * FROM ShardT")
    result = cursor.fetchall()


    #TODO : only need to update to primary server

    
    shards = [
        {
            "Stud_id_low": row[0],
            "Shard_id": row[1],
            "Shard_size": row[2]
        } for row in result
    ]
    for shard in shards:
        shard_id = shard['Shard_id']
        if Stud_id >= shard['Stud_id_low'] and Stud_id < shard['Stud_id_low'] + shard['Shard_size']:
            req_shard = shard_id
            break
    if req_shard == '':
        response_data = {
            "message" : "Data entry does not exist",
            "status" : "failed"
        }
        return jsonify(response_data), 400
    cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{req_shard}' and Primary_server = 1")
    result = cursor.fetchall()
    servers = [row[0] for row in result]
    print(servers)
    for server in servers:
        try:
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/update'
            data = {
                "shard":req_shard,
                "Stud_id":Stud_id,
                "data":new_data
            }
            requests.put(url_redirect, json=data)
            time.sleep(1)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to update server', 
                        'status': 'failure'}
            return jsonify(response_data), 400
    response_data = {
        "message":f"Data entry with Stud_id: {Stud_id} updated",
        "status":"success"
    }
    return jsonify(response_data), 200

@app.route('/del', methods=['DELETE'])
def delete():
    data = request.get_json()
    Stud_id = data['Stud_id']
    req_shard = ''
    cursor.execute("SELECT * FROM ShardT")
    result = cursor.fetchall()

    #TODO : only need to del to primary server

    
    shards = [
        {
            "Stud_id_low": row[0],
            "Shard_id": row[1],
            "Shard_size": row[2]
        } for row in result
    ]
    for shard in shards:
        shard_id = shard['Shard_id']
        if Stud_id >= shard['Stud_id_low'] and Stud_id < shard['Stud_id_low'] + shard['Shard_size']:
            req_shard = shard_id
            break
    if req_shard == '':
        response_data = {
            "message" : "Data entry does not exist",
            "status" : "failed"
        }
        return jsonify(response_data), 400
    cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{req_shard}' and Primary_server = 1")
    result = cursor.fetchall()
    servers = [row[0] for row in result]
    for server in servers:
        try:
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/del'
            data = {
                "shard":req_shard,
                "Stud_id":Stud_id
            }
            requests.delete(url_redirect, json=data)
            time.sleep(1)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to delete server', 
                        'status': 'failure'}
            return jsonify(response_data), 400
    response_data = {
        "message":f"Data entry with Stud_id: {Stud_id} removed from all replicas",
        "status":"success"
    }
    return jsonify(response_data), 200
@app.route('/get_schema', methods=['GET'])
def get_schema():
    response = {"schema":schema}
    return jsonify(response), 200
# # route /<path:path>
# @app.route('/<path:path>', methods=['GET'])
# def redirect_request(path='home'):
#     global heartbeat_ptr
#     # If path is not heartbeat or home return error
#     if not (path == 'home' or path == 'heartbeat'):
#         response_data = {
#             "message" : "<Error> {path} endpoint does not exist in server replicas",
#             "status" : "failure"
#         }
#         return jsonify(response_data), 400
    
#     # If no server replicas are working return error
#     if len(server_host_to_id) == 0:
#         response_data = {
#             "message" : "<Error> No server replica working",
#             "status" : "failure"
#         }
#         return jsonify(response_data), 400
    
#     # If path is heartbeat, check if the server is working or not
#     if path == 'heartbeat':
#         # Get the next server to send heartbeat request to
#         num_servers = len(server_host_to_id)
#         heartbeat_ptr = (heartbeat_ptr + 1) % num_servers

#         # Get the server id and server name
#         server = list(server_host_to_id.keys())[heartbeat_ptr]
#         server_id = server_host_to_id[server]

#         # try Send heartbeat request to the server
#         try:
#             # if successful, return the response
#             container = client.containers.get(server)
#             ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
#             url_redirect = f'http://{ip_addr}:5000/{path}'
#             return requests.get(url_redirect).json(), 200
#         except docker.errors.NotFound:
#             # if server container is not found, 
#             # run a new server container and return error
#             client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id})
#             print('Restarted server container ' + server + ' with id ' + str(server_id))
#             response_data = {'message': '<Error> Failed to redirect request', 
#                         'status': 'failure'}
#             return jsonify(response_data), 400
#         except Exception as e:
#             # if server container is found but not working,
#             #  restart the server container and return error
#             container = client.containers.get(server)
#             container.restart()
#             print('Restarted server container ' + server + ' with id ' + str(server_id))
#             response_data = {'message': '<Error> Failed to redirect request', 
#                         'status': 'failure'}
#             return jsonify(response_data), 400

#     # 
#     # try:
#     #     data = request.get_json()
#     #     if not data  or 'request_id' not in data.keys():
#     #         request_id = random.randint(100000, 999999)
#     #     else:
#     #         request_id = data['request_id']
#     # except KeyError as err:
#     request_id = random.randint(100000, 999999) # generate a random request id

#     # Using the request id select the server and replace server_id and server name with corresponding values
#     try:
#         # send the request to the server by finding the server_id using consistent hashing
#         server_id = ConsistentHashing.get_server_for_request(request_id)
#         server = server_id_to_host[server_id]
#         container = client.containers.get(server)
#         ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
#         url_redirect = f'http://{ip_addr}:5000/{path}'
#         return requests.get(url_redirect).json(), 200
#     except Exception as e:
#             print(e)
#             response_data = {'message': '<Error> Failed to redirect request', 
#                         'status': 'failure'}
#             return jsonify(response_data), 400

# main function
if __name__ == "__main__":
     # run a new process for heartbeat.py file
    # absolute_path = os.path.dirname(__file__)
    # relative_path = "./heartbeat.py"
    # full_path = os.path.join(absolute_path, relative_path)
    # process = Popen(['python3', full_path], close_fds=True)
    
    # run the flask app
    app.run(host='0.0.0.0', port=5000)

    
