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

# Initialize the Docker client
client = docker.from_env()
network = "n1"
image = "server"

# Initialize the server_id to hostname and hostname to server_id mapping
server_id_to_host = {}
server_host_to_id = {}

# route /rep 
@app.route('/rep', methods=['GET'])
def get_replicas():
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


# route /add
@app.route('/add', methods=['POST'])
def add_servers():
    # Get the number of servers to be added and the hostnames of the servers
    data = request.get_json()
    n = data['n']
    hostnames = data['hostnames']

    # If n is less than length of hostnames supplied return error
    if(len(hostnames) > n):
        response_data = {
        "message" : "<Error> Length of hostname list is more than newly added instances",
        "status" : "failure"
    }
        return jsonify(response_data), 400
    else:
        i = 0
        while(i < n):
            # Get server IDs for server
            server_id = random.randint(100000, 999999)

            # If server ID already exists , just continue
            if server_id in server_id_to_host.keys():
                continue
            server_name = ''
            
            # If n > len(hostnames) generate hostnames based on server IDs
            if i < len(hostnames) and hostnames[i] not in server_host_to_id.keys():
                server_name = hostnames[i]
            else:
                server_name = "Server" + str(server_id)
            
            # spawn a server container
            try:
                client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERVER_ID': server_id})
            except Exception as e:
                print(e)
                response = {'message': '<Error> Failed to spawn new docker container', 
                        'status': 'failure'}
                return jsonify(response), 400
            
            # store id->hostname and hostname->id mapping
            server_id_to_host[server_id] = server_name
            server_host_to_id[server_name] = server_id

            # add the server to consistent hash table 
            ConsistentHashing.add_server(server_id)

            i = i + 1 # increment i

        # Get server containers     
        containers = client.containers.list(filters={'network':network})
         # Return the response
        response_data = {
            "N": len(containers) - 1,
            "replicas": [container.name for container in containers if container.name != "lb"]
        }
        response = {
            "message": response_data,
            "status": "successful"
        }
        return jsonify(response), 200
    
# route /rm
@app.route('/rm', methods=['DELETE'])
def remove_servers():
    # Get the number of servers to be removed and the hostnames of the servers
    data = request.get_json()
    n = data['n']
    hostnames = data['hostnames']

    # If n is less than length of hostnames supplied return error
    if(len(hostnames) > n):
        response_data = {
        "message" : "<Error> Length of hostname list is more than removable instances",
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
    random_remove = n - len(hostnames)
    extra_servers = list(set(container_names) - set(hostnames))
    servers_rm = hostnames

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

# route /<path:path>
@app.route('/<path:path>', methods=['GET'])
def redirect_request(path='home'):
    global heartbeat_ptr
    # If path is not heartbeat or home return error
    if not (path == 'home' or path == 'heartbeat'):
        response_data = {
            "message" : "<Error> {path} endpoint does not exist in server replicas",
            "status" : "failure"
        }
        return jsonify(response_data), 400
    
    # If no server replicas are working return error
    if len(server_host_to_id) == 0:
        response_data = {
            "message" : "<Error> No server replica working",
            "status" : "failure"
        }
        return jsonify(response_data), 400
    
    # If path is heartbeat, check if the server is working or not
    if path == 'heartbeat':
        # Get the next server to send heartbeat request to
        num_servers = len(server_host_to_id)
        heartbeat_ptr = (heartbeat_ptr + 1) % num_servers

        # Get the server id and server name
        server = list(server_host_to_id.keys())[heartbeat_ptr]
        server_id = server_host_to_id[server]

        # try Send heartbeat request to the server
        try:
            # if successful, return the response
            container = client.containers.get(server)
            ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
            url_redirect = f'http://{ip_addr}:5000/{path}'
            return requests.get(url_redirect).json(), 200
        except docker.errors.NotFound:
            # if server container is not found, 
            # run a new server container and return error
            client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id})
            print('Restarted server container ' + server + ' with id ' + str(server_id))
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            return jsonify(response_data), 400
        except Exception as e:
            # if server container is found but not working,
            #  restart the server container and return error
            container = client.containers.get(server)
            container.restart()
            print('Restarted server container ' + server + ' with id ' + str(server_id))
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            return jsonify(response_data), 400

    # 
    # try:
    #     data = request.get_json()
    #     if not data  or 'request_id' not in data.keys():
    #         request_id = random.randint(100000, 999999)
    #     else:
    #         request_id = data['request_id']
    # except KeyError as err:
    request_id = random.randint(100000, 999999) # generate a random request id

    # Using the request id select the server and replace server_id and server name with corresponding values
    try:
        # send the request to the server by finding the server_id using consistent hashing
        server_id = ConsistentHashing.get_server_for_request(request_id)
        server = server_id_to_host[server_id]
        container = client.containers.get(server)
        ip_addr = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
        url_redirect = f'http://{ip_addr}:5000/{path}'
        return requests.get(url_redirect).json(), 200
    except Exception as e:
            print(e)
            response_data = {'message': '<Error> Failed to redirect request', 
                        'status': 'failure'}
            return jsonify(response_data), 400
    
# main function
if __name__ == "__main__":
    # run a new process for heartbeat.py file
    absolute_path = os.path.dirname(__file__)
    relative_path = "./heartbeat.py"
    full_path = os.path.join(absolute_path, relative_path)
    process = Popen(['python3', full_path], close_fds=True)
    
    # run the flask app
    app.run(host='0.0.0.0', port=5000)

    
