import requests

def send_request():
    try:
        response = requests.get('http://localhost:8000/home')
        response_data = response.json()
        print(response_data)
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to load balancer: {e}")

if __name__ == '__main__':
    send_request()
