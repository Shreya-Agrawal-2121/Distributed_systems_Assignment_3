import requests
import random
import time
import numpy as np
import matplotlib.pyplot as plt

# Function to send POST requests
def send_post_request(data, endpoint):
    url = 'http://localhost:5000/' + endpoint
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    return response

def plot_line_chart(x_values, y_values, x_label, y_label, title, path):
    plt.close()
    if x_values is None:
        plt.plot(y_values)
    else:
        plt.plot(x_values, y_values)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.savefig(path)
    print(f"Saved plot: {title} to {path}")

# Function to generate random student IDs
def generate_random_student_ids(max_id, num_ids):
    return random.sample(range(max_id), num_ids)

# Function to send multiple POST requests
def send_multiple_write_requests(data_list):
    write_time = []
    for data in data_list:
        start_time = time.time()
        write_data = {"data":[data]}
        send_post_request(write_data, '/write')
        end_time = time.time()
        write_time.append(end_time - start_time)
    
    return write_time

def send_multiple_read_requests(data_list):
    read_time = []
    for data in data_list:
        payload_json = {"low": data["Stud_id"], "high": data["Stud_id"]}
        payload_json = {"Stud_id": payload_json}
        start_time = time.time()
        response = send_post_request(payload_json, '/read')
        end_time = time.time()
        read_time.append(end_time - start_time)

    return read_time

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
read_time = send_multiple_read_requests(sample_data_list)

print("A-1: Default Configuration")
print("Total read time:", np.sum(read_time))
print("Total write time:", np.sum(write_time))
print("Average read time:", np.mean(read_time))
print("Average write time:", np.mean(write_time))

plot_line_chart(x_values=None, y_values=read_time, x_label="Request", y_label="Time (s)",
                title="Read Time", path="A1_read_time.png")
plot_line_chart(x_values=None, y_values=write_time, x_label="Request", y_label="Time (s)",
                title="Write Time", path="A1_write_time.png")

# print(f"Write speed for 10,000 writes: {write_time} seconds")

# Measure time taken to send POST requests
# print(f"Read speed for 10,000 reads: {read_time} seconds")



## SUBTASK 1
# payload = {
#     "N": 3,
#     "schema": {
#         "columns": [
#             "Stud_id",
#             "Stud_name",
#             "Stud_marks"
#         ],
#         "dtypes": [
#             "Number",
#             "String",
#             "String"
#         ]
#     },
#     "shards": [
#         {
#             "Stud_id_low": 0,
#             "Shard_id": "sh1",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 4096,
#             "Shard_id": "sh2",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 8192,
#             "Shard_id": "sh3",
#             "Shard_size": 4096
#         }
#     ],
#     "servers": {
#         "Server0": [
#             "sh1",
#             "sh2"
#         ],
#         "Server1": [
#             "sh2",
#             "sh3"
#         ],
#         "Server2": [
#             "sh1",
#             "sh3"
#         ]
#     }
# }

# ## SUBTASK 2
# payload = {
#     "N": 3,
#     "schema": {
#         "columns": [
#             "Stud_id",
#             "Stud_name",
#             "Stud_marks"
#         ],
#         "dtypes": [
#             "Number",
#             "String",
#             "String"
#         ]
#     },
#     "shards": [
#         {
#             "Stud_id_low": 0,
#             "Shard_id": "sh1",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 4096,
#             "Shard_id": "sh2",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 8192,
#             "Shard_id": "sh3",
#             "Shard_size": 4096
#         }
#     ],
#     "servers": {
#         "Server0": [
#             "sh1",
#             "sh2"
#         ],
#         "Server1": [
#             "sh2",
#             "sh3"
#         ],
#         "Server2": [
#             "sh1",
#             "sh3"
#         ]
#     }
# }

## SUBTASK 3
# payload = {
#     "N": 3,
#     "schema": {
#         "columns": [
#             "Stud_id",
#             "Stud_name",
#             "Stud_marks"
#         ],
#         "dtypes": [
#             "Number",
#             "String",
#             "String"
#         ]
#     },
#     "shards": [
#         {
#             "Stud_id_low": 0,
#             "Shard_id": "sh1",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 4096,
#             "Shard_id": "sh2",
#             "Shard_size": 4096
#         },
#         {
#             "Stud_id_low": 8192,
#             "Shard_id": "sh3",
#             "Shard_size": 4096
#         }
#     ],
#     "servers": {
#         "Server0": [
#             "sh1",
#             "sh2"
#         ],
#         "Server1": [
#             "sh2",
#             "sh3"
#         ],
#         "Server2": [
#             "sh1",
#             "sh3"
#         ]
#     }
# }

# for n in range(3, 11):
#     rtime, wtime = launch_rw_requests()
#     write_times[n] = {"total": np.sum(wtime), "mean": np.mean(
#         wtime), "error": np.std(wtime)}
#     read_times[n] = {"total": np.sum(rtime), "mean": np.mean(
#         rtime), "error": np.std(rtime)}
#     payload = {}
#     response = requests.post(base_url + "/add", json=payload)
#     if response.status_code != 200:
#         print("Error:", response.text)
