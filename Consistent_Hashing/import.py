import sys
sys.path.append('../Consistent_Hashing')
from load_balancer.main import ConsistentHashing

ConsistentHashing = ConsistentHashing(3, 512, 9)
ConsistentHashing.add_server(0)
ConsistentHashing.add_server(1)
ConsistentHashing.add_server(2)

ConsistentHashing.print_servers()

ConsistentHashing.remove_server(0)
ConsistentHashing.remove_server(1)

ConsistentHashing.print_servers()

ConsistentHashing.add_server(0)

ConsistentHashing.print_servers()