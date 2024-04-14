import random
import string
import requests
import time
import random
import numpy as np
import matplotlib.pyplot as plt

def get_random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

class PayloadGenerator:
    def __init__(self, limit):
        self.available_ids = set(range(0, limit))
        self.allocated_ids = set()

    def generate_random_payload(self, endpoint):
        if endpoint == "/read":
            return self._read_payload()
        elif endpoint == "/write":
            return self._write_payload(1)
        elif endpoint == "/update":
            return self._update_payload()
        elif endpoint == "/delete":
            return self._delete_payload()
        else:
            raise ValueError("Invalid endpoint.")

    def _read_payload(self):
        low = random.randint(0, 899999) + 100000
        high = random.randint(low, 999999)
        return {"Stud_id": {"low": low, "high": high}}

    def _write_payload(self, num_students):
        data = []
        for _ in range(num_students):
            if len(self.available_ids) == 0:
                break
            Stud_id = random.choice(tuple(self.available_ids))
            self.available_ids.remove(Stud_id)
            self.allocated_ids.add(Stud_id)
            Stud_name = get_random_string(random.randint(5, 20))
            Stud_marks = random.randint(0, 90) + 10
            data.append(
                {'Stud_id': Stud_id, 'Stud_name': Stud_name, 'Stud_marks': Stud_marks})
        return {'data': data}

    def _updated_payload(self):
        if len(self.allocated_ids) == 0:
            raise ValueError("No allocated student IDs.")
        Stud_id = random.choice(tuple(self.allocated_ids))
        Stud_name = get_random_string(random.randint(5, 20))
        Stud_marks = random.randint(0, 100)
        return {'Stud_id': Stud_id, 'data': {'Stud_id': Stud_id, 'Stud_name': Stud_name, 'Stud_marks': Stud_marks}}

    def _delete_payload(self):
        if len(self.allocated_ids) == 0:
            raise ValueError("No allocated student IDs.")
        Stud_id = random.choice(tuple(self.allocated_ids))
        self.allocated_ids.remove(Stud_id)
        self.available_ids.add(Stud_id)
        return {'Stud_id': Stud_id}

base_url = "http://localhost:5000"
generator = PayloadGenerator(12288)


def plot_line_chart(x_values, y_values, x_label, y_label, title, path):
    """
    Generate and save a line chart.

    Parameters:
        x_values (list or None): X-axis values. If None, indexes of y_values are used.
        y_values (list): Y-axis values.
        x_label (str): Label for the X-axis.
        y_label (str): Label for the Y-axis.
        title (str): Title of the plot.
        path (str): Path where the plot will be saved.
    """
    plt.close()  # Close any existing plots to start fresh

    # Plot the data
    if x_values is None:
        plt.plot(y_values)
    else:
        plt.plot(x_values, y_values)

    # Add labels and title
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    # Save the plot
    plt.savefig(path)
    print(f"Saved plot '{title}' to '{path}'")

def launch_read_requests():
    read_time = []
    for _ in range(1000):
        start = time.time()
        response = requests.post(
            base_url + "/read", json=generator.generate_random_payload(endpoint="/read"))
        if response.status_code != 200:
            print("Error:", response.text)
        end = time.time()
        read_time.append(end - start)

    return read_time

def launch_write_requests():
    write_time = []
    for _ in range(1000):
        start = time.time()
        response = requests.post(
            base_url + "/write", json=generator.generate_random_payload(endpoint="/write"))
        if response.status_code != 200:
            print("Error:", response.text)
        end = time.time()
        write_time.append(end - start)

    return write_time
