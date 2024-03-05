from flask import Flask, jsonify
import os

app = Flask(__name__)

# Get server ID from environment variable
server_id = os.environ.get('SERVER_ID')

# Home endpoint
@app.route('/home', methods=['GET'])
def home():
    # hello message
    response_data = {
        "message": f"Hello from Server: {server_id}",
        "status": "successful"
    }
    return jsonify(response_data), 200

# Heartbeat endpoint
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # no meassage 
    response_data = {
        "message": "",
        "status": "successful"
    }
    return jsonify(response_data), 200

# main function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
