from flask import Flask, jsonify
import os
import mysql.connector

cnx = mysql.connector.connect(user='root', password='shr',
                              host='127.0.0.1',
                              database='meta_data')
app = Flask(__name__)

# Get server ID from environment variable
server_id = os.environ.get('SERVER_ID', 'Unknown')

@app.route('/home', methods=['GET'])
def home():
    response_data = {
        "message": f"Hello from Server: {server_id}",
        "status": "successful"
    }
    return jsonify(response_data), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

