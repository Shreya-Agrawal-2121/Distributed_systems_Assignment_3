# write a python script to launch 1000 async requests to (url):

import asyncio
import aiohttp
import matplotlib.pyplot as plt

# write a function to launch a get request to the url and return the response
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

def extract_server_id(response):

    # Extract server ID from the message
    message = response["message"]
    server_id = message.split(":")[1].strip()
    
    return server_id
    
async def main():

    # launch a GET request to the url
    url = 'http://localhost:5000/home'

    # store the count of serverid responses in a dictionary

    freq = {}

    for i in range(10000):
        async with aiohttp.ClientSession() as session:
            response = await fetch(session, url)
            
            serverid = extract_server_id(response)
            freq[serverid] = freq.get(serverid, 0) + 1

    # draw a bar graph of the frequency of each serverid
    # and label the x-axis with the serverid and the y-axis with the frequency
    plt.bar(freq.keys(), freq.values())
    # Add the values on top of the bars
    for server_id, frequency in freq.items():
        plt.text(server_id, frequency + 0.1, str(frequency), ha='center', va='bottom')

    plt.xlabel('Server ID')
    plt.ylabel('Frequency')
    
    # print the graph
    # plt.show()
    plt.savefig('A1.png')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
