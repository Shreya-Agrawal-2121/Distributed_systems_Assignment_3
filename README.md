## Instructions for running the load balancer

    make build 
    make run_lb

Open another terminal and run the following command to initiate 3 servers
    
    make run_servers

For running the Analysis code
    
    python3 Analysis/A1.py
    python3 Analysis/A2.py

For stopping the containers
    
    make stop

For removing the containers
    
    make rm

# A-1

The graph indicates uneven load distribution among servers, leading to several issues:

    1. Uneven Resource Utilization:
    Resources are unevenly distributed, causing some servers to be underutilized while others are overloaded.

    2. Latency Concerns:
    Overloaded servers result in higher latency for users, leading to slower response times during access.

    3. Risk of Failures:
    Overloaded servers are more prone to failures, potentially causing downtime or disruptions in service.

    4. Resource Planning Challenges:
    Uneven load distribution complicates resource planning, making it difficult to allocate resources effectively.

<!-- TODO: Explaination of the uneven distribution -->

## A-2

The data analysis of standard deviation versus server counts reveals that linear probing is effective for a large number of server instances, whereas quadratic probing is more helpful for smaller server instances. However, both techniques exhibit uneven distribution, potentially leading to scalability issues. This uneven distribution poses challenges for capacity planning as the load balancer struggles to evenly distribute traffic among servers. Consequently, ensuring efficient resource utilization and maintaining balance becomes more complex, emphasizing the need for careful consideration of the chosen probing technique based on the scale of the server infrastructure.


<!-- TODO: Explaination of the uneven distribution -->

## A-3

We're implementing a new process that periodically sends a heartbeat every 2 seconds to enable the load balancer to verify server statuses. We maintain a circular array of servers and send heartbeats in sequence. If the status code is 200, indicating the server is online, no action is taken. However, if the status code is different, we check whether the server container has been stopped or removed. If stopped, we resume the server container; otherwise, we initiate a new server container. This approach ensures a fixed number of servers. In the worst-case scenario, the time required to bring a server back to life is estimated at 2*n seconds, assuming there are currently n live servers.

After starting the load balancer and the servers to simulate the server failure do the following: 
    
    docker ps

Copy the id of any server container then
    
    docker stop <container_id>

For removing the stopped server container
    
    docker rm <container_id>

After performing the above steps if you run `docker ps` you can see that the server container has started again.

## A-4

The chosen hash functions for the demonstration, \
H(i)      = i * i + 6 \
phi(i, j) = i * i + j 

along with the application of the linear probing technique, exhibited poorer performance compared to the previous ones. Server 1 experienced a higher load due to the inability of these hash functions to effectively distribute the loads across the servers.


## Explaination of uneven distribution:
We have carried out the experiment multiple times with the same parameter and every time we observed different distributions hence we cannot conclude anything based upon its randomized nature.
