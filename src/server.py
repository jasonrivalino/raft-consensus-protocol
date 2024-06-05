import sys
import json
import asyncio
import time
from xmlrpc.server import SimpleXMLRPCServer
from threading import Thread
from lib.raft import RaftNode
from lib.app import Application
from lib.struct.address import Address

def start_serving(addr: Address, contact_node_addr: Address = None):
    print(f"Starting Raft Server at {addr.ip}:{addr.port}")
    with SimpleXMLRPCServer((addr.ip, addr.port), allow_none=True) as server:
        server.register_introspection_functions()
        server.register_instance(RaftNode(Application(), addr, contact_node_addr))
        server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: server.py ip port [contact_ip] [contact_port]")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_serving(server_addr, contact_addr)
