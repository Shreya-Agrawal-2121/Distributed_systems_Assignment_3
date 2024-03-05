import asyncio
import aiohttp
from time import sleep

# fetch function from aiohttp documentation
# what it does: fetches the url and returns the response
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

# main function
# what it does: sends a heartbeat request to the server every 2 seconds
async def main():
    while True:
        sleep(2)  #TODO: change to 1 second or 0.5 second and see what happens
        url = 'http://localhost:5000/heartbeat'
        # create a session and send a request to the server
        async with aiohttp.ClientSession() as session:
            response = await fetch(session, url)
            

if __name__ == '__main__':
    sleep(10) # wait for the servers to start
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
