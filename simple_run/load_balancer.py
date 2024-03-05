import subprocess
from flask import Flask, jsonify
import requests
import os

app = Flask(__name__)

# Server process
server_process = None

def start_server():
    global server_process
    server_process = subprocess.Popen(['python', 'app.py'])


# Uncomment the line below to start the server when the load balancer is launched
start_server()

# List of server URLs to balance requests
server_urls = [
    'http://localhost:5000',
    'http://localhost:5000',
    # Add more server URLs as needed
]

current_server_index = 0

@app.route('/home', methods=['GET'])
def home():
    # Check if the server is running
    if server_process and server_process.poll() is not None:
        # Server process has terminated, restart it
        start_server()

    global current_server_index
    server_url = server_urls[current_server_index]
    current_server_index = (current_server_index + 1) % len(server_urls)

    try:
        response = requests.get(f'{server_url}/home')
        response_data = response.json()
        return jsonify(response_data), 200
    except requests.exceptions.RequestException as e:
        return jsonify({"message": f"Error connecting to server: {server_url}", "status": "error"}), 500

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
