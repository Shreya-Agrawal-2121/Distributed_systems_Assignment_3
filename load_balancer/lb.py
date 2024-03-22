from flask import Flask, jsonify, request, redirect
import docker
import os
import random
import requests
from subprocess import Popen
from consistentHashing import ConsistentHashing


# Initialize the Flask application
app = Flask(__name__)

# Initialize the ConsistentHashing class
heartbeat_ptr=0
ConsistentHashing = ConsistentHashing(3, 512, 9)
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
        server = keys[i]
        server_id = random.randint(100000, 999999)
        if server_id in server_id_to_host.keys():
            continue
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
        post_data = {
            "schema": schema,
            "shards": servers[server]
        }
        try :
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            print(ip_addr)
            print(post_data)
            url_redirect = f'http://{ip_addr}:5000/config'
            requests.post(url_redirect, json=post_data)
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            
            return jsonify(response_data), 400
        shards_data = servers[server]
        for shard in shards_data:
            if shard not in mapT:
                mapT[shard] = [server]
            else:
                mapT[shard].append(server)
        i += 1
    for shard in shards:
        shard_id = shard['Shard_id']
        low_id = shard['Stud_id_low']
        shard['valid_idx'] = low_id
        if shard_id in shardT:
            continue
        shardT[shard_id] = shard
    for shard in mapT.keys():
        cmap = ConsistentHashing(3, 512, 9)
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
            server_id = server_host_to_id[server]
            cmap.add_server(server_id)
        hashmaps[shard] = cmap
    message = "Add "
    for server in servers_new:
        id = server_host_to_id[server]
        message = message + "Server:" + id + " "
    response = {
        "N": N,
        "message": message,
        "status": "successful"
    }
    return jsonify(response), 200


@app.route('/rm', methods=['DELETE'])
def remove_servers():
    # Get the number of servers to be removed and the hostnames of the servers
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
        ConsistentHashing.remove_server(server_host_to_id[server])

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
        except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to remove docker container', 
                        'status': 'failure'}
            return jsonify(response_data), 400
        
    # Get server containers hostnames and return the response
    containers = client.containers.list(filters={'network':network})
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



# TODO: havent use mutex anywhere
@app.route('/write', methods=['POST'])
def write():
    data = request.get_json()
    data = data['data']
    cnt=0
    for entry in data:
        shard = shardT[entry['Stud_id'] % N]        # TODO:needs to be modified, ho to do this
        request_id = random.randint(100000, 999999)
        server = hashmaps[shard['Shard_id']].get_server_for_request(request_id)
        container = client.containers.get(server_id_to_host[server])
        ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
        
        url_redirect = f'http://{ip_addr}:5000/write'
        
        new_mp={}
        new_mp['shard'] = shard['Shard_id']
        new_mp['curr_idx'] = shard['valid_idx']
        new_mp['data'] = entry
        response = requests.post(url_redirect, json=new_mp)

        if response.status_code == 200:
            cnt += response.json()['current_idx']-shard['valid_idx']

    response_data = {
        "message" : "{} Data entries added".format(cnt),
        "status" : "success"
    }
    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
def update():
    data = request.get_json()
    shard = shardT[data['Stud_id'] % N]
    request_id = random.randint(100000, 999999)
    server = hashmaps[shard['Shard_id']].get_server_for_request(request_id)
    container = client.containers.get(server_id_to_host[server])
    ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]

    url_redirect = f'http://{ip_addr}:5000/update'
    new_data = {}
    new_data['shard'] = shard['Shard_id']
    new_data['Stud_id'] = data['Stud_id']
    new_data['new_data'] = data['data']
    response = requests.put(url_redirect, json=new_data)
    if response.status_code == 200:
        response_data = {
            "message" : "Data entry for Stud_id: {} updated".format(data['Stud_id']),
            "status" : "success"
        }
        return jsonify(response_data), 200
    else:
        response_data = {
            "message" : response.json()['message'],
            "status" : "failure"
        }
        return jsonify(response_data), 500
    


@app.route('/del', methods=['DELETE'])
def delete():
    data = request.get_json()
    shard = shardT[data['Stud_id'] % N]
    request_id = random.randint(100000, 999999)
    server = hashmaps[shard['Shard_id']].get_server_for_request(request_id)
    container = client.containers.get(server_id_to_host[server])
    ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]

    url_redirect = f'http://{ip_addr}:5000/del'
    new_data = {}
    new_data['shard'] = shard['Shard_id']
    new_data['Stud_id'] = data['Stud_id']
    response = requests.delete(url_redirect, json=new_data)
    if response.status_code == 200:
        response_data = {
            "message" : "Data entry with Stud_id: {} removed from all replicas".format(data['Stud_id']),
            "status" : "success"
        }
        return jsonify(response_data), 200
    else:
        response_data = {
            "message" : response.json()['message'],
            "status" : "failure"
        }
        return jsonify(response_data), 500


# main function
if __name__ == "__main__":
    # run a new process for heartbeat.py file
    absolute_path = os.path.dirname(__file__)
    relative_path = "./heartbeat.py"
    full_path = os.path.join(absolute_path, relative_path)
    process = Popen(['python3', full_path], close_fds=True)
    
    # run the flask app
    app.run(host='0.0.0.0', port=5000)

    
