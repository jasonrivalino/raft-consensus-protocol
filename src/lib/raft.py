import asyncio
import json
import socket
import time
import random
from threading import Thread
from xmlrpc.client import ServerProxy
from typing import Any, List, Dict
from enum import Enum
from lib.app import Application
from lib.struct.address import Address

class RaftNode:
    HEARTBEAT_INTERVAL = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT = 0.5

    class NodeType(Enum):
        LEADER = 1
        CANDIDATE = 2
        FOLLOWER = 3

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        self.log:                 List[str, str]    = []
        self.app:                 Any               = application
        self.election_term:       int               = 0
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("[Follower] Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type = RaftNode.NodeType.LEADER
        request = {
            "cluster_leader_addr": self.address
        }
        for addr in self.cluster_addr_list:
            if addr == self.address:
                continue
            self.__send_request(request, "initialize_as_leader", addr)

        self.heartbeat_thread = Thread(target=asyncio.run, args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()

    def initialize_as_follower(self, leader_addr: Address):
        self.__print_log("Initialize as follower node...")
        self.cluster_leader_addr = leader_addr
        self.type = RaftNode.NodeType.FOLLOWER
        self.election_timeout = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        self.election_thread = Thread(target=asyncio.run, args=[self.__follower_election()])
        self.election_thread.start()

    async def __leader_heartbeat(self):
        while True:
            self.__print_log("[Leader] Sending heartbeat...")
            for addr in self.cluster_addr_list:
                if addr == self.address:
                    continue
                request = {
                    "term": self.election_term,
                    "leader_id": self.address
                }
                self.__send_request(request, "heartbeat", addr)
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    async def __follower_election(self):
        while True:
            if time.time() > self.election_timeout:
                self.__print_log("[Follower] Election timeout, starting new election...")
                self.__start_election()
            await asyncio.sleep(0.1)

    def __start_election(self):
        self.election_term += 1
        self.type = RaftNode.NodeType.CANDIDATE
        self.vote_count = 1  # vote for self
        self.voted_for = self.address
        request = {
            "term": self.election_term,
            "candidate_id": self.address
        }
        for addr in self.cluster_addr_list:
            if addr == self.address:
                continue
            self.__send_request(request, "request_vote", addr)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        response = {
            "status": "redirected",
            "address": {
                "ip": contact_addr.ip,
                "port": contact_addr.port,
            }
        }
        while response["status"] != "success":
            self.__print_log(f"Applying membership for {self.address.ip}:{self.address.port}")
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response = self.__send_request(self.address, "apply_membership", redirected_addr)
        self.log = response["log"]
        self.cluster_addr_list = response["cluster_addr_list"]
        self.cluster_leader_addr = redirected_addr
        request = {
            "cluster_leader_addr": redirected_addr
        }
        for addr in self.cluster_addr_list:
            if addr == self.address:
                continue
            self.__send_request(request, "initialize_as_leader", addr)

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        node = ServerProxy(f"http://{addr.ip}:{addr.port}")
        json_request = json.dumps(request)
        rpc_function = getattr(node, rpc_name)
        response = json.loads(rpc_function(json_request))
        self.__print_log(f"Response from {addr}: {response}")
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> "json":
        request = json.loads(json_request)
        self.election_timeout = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        response = {
            "heartbeat_response": "ack",
            "address": self.address,
        }
        return json.dumps(response)

    def request_vote(self, json_request: str) -> "json":
        request = json.loads(json_request)
        if request["term"] > self.election_term:
            self.election_term = request["term"]
            self.voted_for = request["candidate_id"]
            self.type = RaftNode.NodeType.FOLLOWER
            response = {"vote_granted": True, "term": self.election_term}
        else:
            response = {"vote_granted": False, "term": self.election_term}
        return json.dumps(response)

    # Client RPCs
    def execute(self, json_request: str) -> "json":
        request = json.loads(json_request)
        if self.type == RaftNode.NodeType.LEADER:
            self.log.append(request)
            response = {"status": "success", "log": self.log}
        else:
            response = {"status": "redirect", "leader": self.cluster_leader_addr}
        return json.dumps(response)