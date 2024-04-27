from flask import Flask, jsonify, request
import docker
import random
import requests
import time
from threading import Thread
import mysql.connector

app = Flask(__name__)
client = docker.from_env()
network = "n1"
image = "server"
mysql_container = client.containers.get("mysql_db")
mysql_ip = mysql_container.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
cnx = mysql.connector.connect(user='root', password='test',
                              host=mysql_ip,
                                )
lb_container = client.containers.get("lb")
lb_ip = lb_container.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
cursor = cnx.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS meta_data")
cursor.execute("USE meta_data")
all_threads = {}
def send_heartbeats(*args):
    name = args[0]
    server_id = args[1]
    while True:
        cursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = '{name}'")
        result = cursor.fetchall()
        if len(result) == 0:
            break
        time.sleep(0.5)
        server = client.containers.get(name)
        ip_addr = server.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
        try:
            requests.get(f"http://{ip_addr}:5000/heartbeat")
        except docker.errors.NotFound:
            cursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = '{name}'")
            result = cursor.fetchall()
            if len(result) == 0:
                break
            client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id,  'SERVER_NAME': name},
                                  privileged=True,
                volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}})
            server = client.containers.get(name)
            ip_addr = server.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            response = request.get(f'http://{lb_ip}:5000/get_server')
            response = response.json()
            schema = response['schema']
            cursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = '{name}'")
            result = cursor.fetchall()
            servers = {}
            shards = []
            for row in result:
                shard = row[0]
                shards.append(shard)
                cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}' AND Server_id != '{name}'")
                res = cursor.fetchone()
                if len(res) > 0:
                    serv = res[0]
                    if serv not in servers:
                        servers[serv] = [shard]
                    else:
                        servers[serv].append(shard)
            post_data = {
                "schema":schema,
                "shards":shards
            }
            requests.post(f"http://{ip_addr}:5000/config",json=post_data)
            for serv in servers.keys():
                cont = client.containers.get(serv)
                ip = cont.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
                data = {
                    "shards": servers[serv]
                }
                response = request.get(f"http://{ip}:5000/copy", json=data)
                time.sleep(0.25)
                if response.status_code == 200:
                    response_data = response.json()
                    for k in response_data.keys():
                        post_data = {
                          "shard":k,
                          "data":response_data[k]
                        }
                        requests.post(f"http://{ip_addr}:5000/write", json=post_data)
                        time.sleep(0.25)  
                    
        except Exception as e:
            cursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = '{name}'")
            result = cursor.fetchall()
            if len(result) == 0:
                break
            #TODO: update server contents
            container = client.containers.get(server)
            container.restart()
            # ip_addr = server.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            # requests.post(f"http://{ip_addr}:5000/replay",json={})
            print(f"Server {name} is down, restarting")

@app.route('/primary_elect', methods=['GET'])
def primary_elect():
    data = request.get_json()
    shard_id = data["shard_id"]
    cursor.execute(f"SELECT Shard_id, Server_id, Primary_server FROM MapT WHERE Shard_id = '{shard_id}'")
    result = cursor.fetchall()
    if len(result) == 0:
        return jsonify({"status": "failed", "message": "No server contains the shard replica"}), 400
    server_id = random.sample(result, 1)
    server_id = server_id[0][1]
    cursor.execute(f"UPDATE MapT SET Primary_server = 1 WHERE Shard_id = '{shard_id}' AND Server_id = '{server_id}'")
    cnx.commit()
    return jsonify({"status": "successful", "message": "Primary server elected", "server_id":server_id}), 200

@app.route('/add_server', methods=['POST'])
def add_server():
    data = request.get_json()
    name = data["server_name"]
    server_id = data["server_id"]
    thread = Thread(target=send_heartbeats, args=(name, server_id))
    all_threads[name] = thread
    thread.start()
    return jsonify({"status": "successful", "message": "Server added"}), 200


@app.route('/secondary', methods=['GET'])
def get_secondary_servers():
    shard = request.args.get("shard")
    try:
        cursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}' AND Primary_server = 0")
        result = cursor.fetchall()
        servers = [row[0] for row in result]
        return jsonify({"status": "successful", "message": "Secondary servers retrieved", "servers": servers}), 200
    except Exception as e:
        return jsonify({"status": "failed", "message": "Error retrieving secondary servers"}), 400

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)