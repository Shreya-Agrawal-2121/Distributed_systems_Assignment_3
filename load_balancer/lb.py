from flask import Flask, jsonify, request, redirect
import docker
import os
import random
import requests
from subprocess import Popen
from consistentHashing import ConsistentHashing
import time
from threading import Thread
from threading import Lock
# Initialize the Flask application
app = Flask(__name__)

# Initialize the ConsistentHashing class
heartbeat_ptr=0
hashmaps = {}
# Initialize the Docker client
client = docker.from_env()
network = "n1"
image = "server"

# Initialize the server_id to hostname and hostname to server_id mapping
server_id_to_host = {}
server_host_to_id = {}
shardT = {}
mapT = {}
N = 0
schema = {}
shards = []
servers = {}
locks = {}
def write_to_servers(*args):
    shard = args[0]
    data = args[1]
    lock = locks[shard]
    lock.acquire()
    valid_id = 0
    shd = None
    for sh in shards:
        if shard == sh['Shard_id']:
            valid_id = sh['valid_idx']
            shd = sh
            break
    for server in mapT[shard]:
        try:
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/write'
            request_data = {
                "shard": shard,
                "curr_idx": valid_id,
                "data": data
            }
            requests.post(url_redirect, json=request_data)
            time.sleep(1)
        except Exception as e:
            print(e)
            lock.release()
            response_data = {'message': '<Error> Failed to write to server', 
                        'status': 'failure'}
            return jsonify(response_data), 400
    for sh in shards:
        if shard == sh['Shard_id']:
            sh['valid_idx'] += len(data)
            break
    lock.release()
@app.route('/init', methods=['POST'])
def init_server():
    global N, schema, shards, servers
    data = request.get_json()
    N = data['N']
    schema = data['schema']
    shards = data['shards']
    servers = data['servers']
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
            keys.append(server)
        try:
                client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id, 'SERVER_NAME': server,
                }, ports={5000:None})
        except Exception as e:
                print(e)
                response = {'message': '<Error> Failed to spawn new container', 
                        'status': 'failure'}
                return jsonify(response), 400
        server_id_to_host[server_id] = server
        server_host_to_id[server] = server_id
        
        shards_data = servers[server]
        for shard in shards_data:
            if shard not in mapT:
                mapT[shard] = [server]
            else:
                mapT[shard].append(server)
        i += 1
        time.sleep(1)
    for server in keys:
        post_data = {
            "schema": schema,
            "shards": servers[server]
        }
        try :
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            print(ip_addr)
            print(post_data)
            headers = {'content-type' : 'application/json'}
            url_redirect = f'http://{ip_addr}:5000/config'
            requests.post(url_redirect, json=post_data, headers=headers)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            
            return jsonify(response_data), 400
        time.sleep(1)
    for shard in shards:
        shard_id = shard['Shard_id']
        low_id = shard['Stud_id_low']
        shard['valid_idx'] = low_id
        if shard_id in shardT:
            continue
        shardT[shard_id] = shard
    for shard in mapT.keys():
        cmap = ConsistentHashing(3, 512, 9)
        mutex_lock = Lock()
        locks[shard] = mutex_lock
        for server in mapT[shard]:
            server_id = server_host_to_id[server]
            cmap.add_server(server_id)
        hashmaps[shard] = cmap
    response_data = {
        'message' : "Configured database",
        'status' : "successful" 
    }
    return jsonify(response_data), 200

@app.route('/status', methods=['GET'])
def status():
    response = {
        "N": N,
        "schema": schema,
        "shards": shards,
        "servers": servers
    }
    
    return jsonify(response), 200

# route /add
@app.route('/add', methods=['POST'])
def add_servers():
    global N, shards, shardT, servers, mapT, schema
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
        servers[server] = servers_new[server]
        shard_data = servers_new[server]
        for shard in shard_data:
            if shard not in mapT:
                mapT[shard] = [server]
            else:
                mapT[shard].append(server)
        time.sleep(1)
        N += 1
        i += 1
    for new_shard in new_shards:
        shard_id = new_shard['Shard_id']
        low_id = new_shard['Stud_id_low']
        new_shard['valid_idx'] = low_id
        if shard_id in shardT:
            continue
        shardT[shard_id] = new_shard
        shards.append(new_shard)
    for shard in mapT.keys():
        num = len(mapT[shard])
        cmap = ConsistentHashing(num, 512, 9)
        for server in mapT[shard]:
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

        # try to stop and remove server container
        try:
            container = client.containers.get(server)
            container.stop()
            container.remove()
            # if successfully removed, remove server from server_id_to_host and server_host_to_id
            server_host_to_id.pop(server)
            server_id_to_host.pop(server_id)
            for shard in servers[server]:
                mapT[shard].remove(server)
                hashmaps[shard].remove_server(server_id)
            servers.pop(server)
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
    for shard in shards:
        range_val = (shard['Stud_id_low'], shard['Stud_id_low'] + shard['Shard_size'])
        if max((range_val[0], low)) <= min((range_val[1], high)):
            shards_range[shard['Shard_id']] = (max((range_val[0], low)), min((range_val[1], high)))
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
    for entry in data:
        shard_id = ''
        for shard in shardT.keys():
            if entry['Stud_id'] >= int(shardT[shard]['Stud_id_low']) and entry['Stud_id'] < int(shardT[shard]['Stud_id_low']) + int(shardT[shard]['Shard_size']):
                if shard not in writes.keys():
                    writes[shard] = []
                    writes[shard].append(entry)
                else:
                    writes[shard].append(entry)
                break
        cnt += 1
    for shard in writes.keys():
        thread = Thread(target=write_to_servers, args=(shard, writes[shard]))
        thread.start()


    response_data = {
        "message" : "{} Data entries added".format(cnt),
        "status" : "success"
    }
    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
def update():
    data = request.get_json()
    Stud_id = data['Stud_id']
    shard = ''
    new_data = data['data']
    for shard_id in shardT.keys():
        if Stud_id >= shardT[shard_id]['Stud_id_low'] and Stud_id < shardT[shard_id]['Stud_id_low'] + shardT[shard_id]['Shard_size']:
            shard = shard_id
            break
    if shard_id == '':
        response_data = {
            "message" : "Data entry does not exist",
            "status" : "failed"
        }
        return jsonify(response_data), 400
    for server in mapT[shard]:
        try:
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/update'
            data = {
                "shard":shard,
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
    shard = ''
    for shard_id in shardT.keys():
        if Stud_id >= shardT[shard_id]['Stud_id_low'] and Stud_id < shardT[shard_id]['Stud_id_low'] + shardT[shard_id]['Shard_size']:
            shard = shard_id
            break
    if shard_id == '':
        response_data = {
            "message" : "Data entry does not exist",
            "status" : "failed"
        }
        return jsonify(response_data), 400
    for server in mapT[shard]:
        try:
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/del'
            data = {
                "shard":shard,
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

    
