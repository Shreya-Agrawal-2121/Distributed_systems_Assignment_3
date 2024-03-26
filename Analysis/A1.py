import requests
import random
import time

# Function to send POST requests
def send_post_request(data, endpoint):
    url = 'http://localhost:5000/' + endpoint
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    return response

# Function to generate random student IDs
def generate_random_student_ids(max_id, num_ids):
    return random.sample(range(max_id), num_ids)

# Function to send multiple POST requests
def send_multiple_write_requests(data_list):
    start_time = time.time()
    for data in data_list:
        send_post_request(data, '/write')
    end_time = time.time()
    return end_time - start_time

def send_multiple_read_requests(data_list):
    start_time = time.time()
    for data in data_list:
        payload_json = {"low": data["Stud_id"], "high": data["Stud_id"]}
        response = send_post_request(payload_json, '/read')
    end_time = time.time()
    return end_time - start_time

# Generate 10,000 random student IDs
student_ids = generate_random_student_ids(12201, 10000)

# generate 10000 random 4 letter names and marks
student_marks = [random.randint(0, 100) for _ in range(10000)]

student_names = []
for i in range(10000):
    student_names.append(''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=4)))

# print first 10 student details
# print(student_ids[:10])
# print(student_names[:10])
# print(student_marks[:10])


# Prepare sample data and send POST requests
sample_data_list = []
for i in range(10000):
    sample_data_list.append({"Stud_id": student_ids[i], "Stud_name": student_names[i], "Stud_marks": student_marks[i]})

# Measure time taken to send POST requests
write_time = send_multiple_write_requests(sample_data_list)
print(f"Write speed for 10,000 writes: {write_time} seconds")

# Measure time taken to send POST requests
read_time = send_multiple_read_requests(sample_data_list)
print(f"Read speed for 10,000 reads: {read_time} seconds")