# write a python script to launch 1000 async requests to (url):

import asyncio
import aiohttp
import matplotlib.pyplot as plt
import requests
import json

# write a function to launch a get request to the url and return the response
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

def extract_server_id(response):

    # Extract server ID from the message
    message = response["message"]
    server_id = message.split(":")
    if len(server_id) != 2:
        return -1
    server_id = server_id[1].strip()
    
    return server_id
    
async def main():

    # launch a GET request to the url
    url = 'http://localhost:5000/'

    # run curl -X DELETE  -H "Content-Type: application/json" -d '{"n": 1, "hostnames": ["s1"]}' 
    # http://localhost:5000/hosts

    data = {"n": 1, "hostnames": ["s3"]}
    headers = {"Content-Type": "application/json"}

    # we deleted one instance of a server (which was 3 initially)

    response = requests.delete(url + 'rm', data=json.dumps(data), headers=headers)

    if response.status_code == 200:

        main_data = {}

        for i in range (2, 7):

            freq = {}
            
            # calculate the frequency of each server, send 10000 requests
            for j in range(10000):
                async with aiohttp.ClientSession() as session:
                    response = await fetch(session, url + 'home')
                    serverid = extract_server_id(response)
                    if serverid == -1:
                        continue
                    freq[serverid] = freq.get(serverid, 0) + 1
            
            server_count = len(freq.values())

            mean = sum(freq.values()) / server_count

            plt.plot(freq.keys(), freq.values())
            plt.xlabel('Server ID')
            plt.ylabel('Frequency')

            # draw a horizontal line at the mean
            plt.axhline(y=mean, color='r', linestyle='-')
            plt.savefig('a2_freq' + str(i) + '.png')
            plt.clf()

            # plot the graph of differences from the mean
            plt.plot(freq.keys(), [(x - mean) for x in freq.values()])
            plt.xlabel('Server ID')
            plt.ylabel('Difference from Mean')
            plt.savefig('a2_diff' + str(i) + '.png')
            plt.clf()

            variance = sum([((x - mean) ** 2) for x in freq.values()]) / server_count

            std_dev = variance ** 0.5

            main_data[i] = std_dev

            data = {"n": 1, "hostnames": ["s" + str(i + 1)]}

            response = requests.post(url + 'add', data=json.dumps(data), headers=headers)

        # plot the graph using line plot
        
        plt.plot(main_data.keys(), main_data.values())
        plt.xlabel('Number of Servers')
        plt.ylabel('Standard Deviation')
        plt.savefig('a2_sd.png')
        plt.clf()

        # plt.bar(main_data.keys(), main_data.values())
        # plt.xlabel('Number of Servers')
        # plt.ylabel('Standard Deviation')




    else:
        print("Error: " + str(response.status_code))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
