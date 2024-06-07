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
        self.uncommitted_log:     List[str, str]    = []
        self.cluster_leader_addr: Address           = None
        self.pending_command                        = None
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
                if self.pending_command is not None:
                    self.__execute_pending_command()
            print("LOG: " + str(self.log))
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
        response = self.__send_request(self.address, "apply_membership", contact_addr)
        
        while response["status"] == "redirected":
            redirected_addr = Address(response["leader"]["ip"], response["leader"]["port"])
            self.__print_log(f"Redirected to {redirected_addr}...")
            response = self.__send_request(self.address, "apply_membership", redirected_addr)
        
        if response["status"] == "success":
            print("RESPOEPSNES")
            print(response)
            self.__print_log(f"Successfully joined the cluster. Current cluster configuration: {response['cluster_addr_list']}")
            self.log = response["log"]
            self.cluster_addr_list = {Address(addr['ip'], addr['port']) for addr in response["cluster_addr_list"]}
            self.cluster_leader_addr = Address(response["leader"]['ip'], response["leader"]['port'])
            for addr in self.cluster_addr_list:
                if addr == self.address:
                    continue
                try:
                    self.__print_log(f"Sending cluster address list to {addr}")
                    response = self.__send_request([a.__dict__ for a in self.cluster_addr_list], "set_cluster_addr_list", addr)
                    # self.__print_log(f"Response from {addr}: {response}")
                except Exception as e:
                    self.__print_log(f"Error sending cluster address list to {addr}: {e}")
            
        else:
            self.__print_log(f"Failed to join the cluster: {response['message']}")


    def apply_membership(self, json_request: str) -> str:
        if (self.type == RaftNode.NodeType.LEADER):
            request = json.loads(json_request)
            addr = Address(request["ip"], request["port"])
            self.cluster_addr_list.append(addr)
            # self.cluster_addr_list.extend(request)
            self.cluster_addr_list = list(set(self.cluster_addr_list))
            print("Membership applied: ", self.cluster_addr_list)
                    
            response = {
                "status": "success",
                "log": self.log,
                "cluster_addr_list": self.cluster_addr_list,
                "leader": self.cluster_leader_addr
            }
            return json.dumps(response)
        else:
            response = {
                "status": "redirected",
                "leader": self.cluster_leader_addr
            }
            print("REDIRECTING TO LEADER: ", response["leader"])
            return json.dumps(response)
    
    def set_cluster_addr_list(self, json_request: str) -> str:
        try:
            self.__print_log("Setting cluster addr list")
            request = json.loads(json_request)
            self.cluster_addr_list = [Address(**addr) for addr in request]
            response = {
                "status": "success",
                "cluster_addr_list": request
            }
            print("NODE ", self.address, " SET CLUSTER ADDR LIST: ", self.cluster_addr_list)
            return json.dumps(response)
        except Exception as e:
            self.__print_log(f"Error in set_cluster_addr_list: {e}")
            response = {
                "status": "error",
                "message": str(e)
            }
            return json.dumps(response)

    
    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> Dict[str, Any]:
        try:
            node = ServerProxy(f"http://{addr.ip}:{addr.port}")
            json_request = json.dumps(request)
            # self.__print_log(f"Sending request to {addr}: {json_request}")
            rpc_function = getattr(node, rpc_name)
            response = json.loads(rpc_function(json_request))
            # self.__print_log(f"Response from {addr}: {response}")
            return response
        except Exception as e:
            self.__print_log(f"Error in __send_request: {e}")
            return {"status": "error", "message": str(e)}

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> "json":
        request = json.loads(json_request)
        self.election_timeout = time.time() + RaftNode.ELECTION_TIMEOUT_MIN + (RaftNode.ELECTION_TIMEOUT_MAX - RaftNode.ELECTION_TIMEOUT_MIN) * random.random()
        # self.election_timeout = time.time() + self.election_timeout # Seharusnya tidak perlu random lagi (?)
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

    def append_log(self, json_request: str) -> "json":
        request = json.loads(json_request)
        self.uncommitted_log.append(request)
        response = {"status": "success", "log": self.uncommitted_log}
        return json.dumps(response)
    
    def commit_log(self, buffer = None) -> "json":
        self.log.extend(self.uncommitted_log)
        self.uncommitted_log = []
        response = {"status": "success", "log": self.log}
        print("LOG", self.log)
        return json.dumps(response)
    
    def rollback_log(self, buffer = None) -> "json":
        self.uncommitted_log = []
        response = {"status": "error", "message": "Failed to replicate log to majority of nodes. commits are rolled back."}
        return json.dumps(response)
        
    # Client RPCs
    def __execute_pending_command(self, buffer = None):
        request = self.pending_command
        self.pending_command = None  # Clear the pending command after executing it
        command = request.get("command")
        args = request.get("args", "")

        self.uncommitted_log.append(request)
        counter = 0
        for addr in self.cluster_addr_list:
            if addr == self.address:
                continue
            self.__send_request(request, "append_log", addr)
            print(addr)
            counter += 1
        if counter > len(self.cluster_addr_list) // 2 or len(self.cluster_addr_list) <= 2:
            
            self.log.extend(self.uncommitted_log)
            self.uncommitted_log = []
            
            for addr in self.cluster_addr_list:
                if addr == self.address:
                    continue
                self.__send_request(None, "commit_log", addr)
                        
            if command == "ping":
                response = self.app.ping()
            elif command == "get":
                response = self.app.get(args)
            elif command == "set":
                key, value = args.split(" ", 1)
                response = self.app.set(key, value)
            elif command == "strln":
                response = self.app.strln(args)
            elif command == "delete":
                response = self.app.delete(args)
            elif command == "append":
                key, value = args.split(" ", 1)
                response = self.app.append(key, value)
            else:
                response = "Unknown command"
            print("COMMAND", command)
            return json.dumps({"status": "success", "response": response, "log": self.log})
        else:
            response = self.rollback_log()
            for addr in self.cluster_addr_list:
                if addr == self.address:
                    continue
                self.__send_request(None, "rollback_log", addr)
            return json.dumps(response)
        
    def execute(self, json_request: str) -> str:
        request = json.loads(json_request)

        if self.type == RaftNode.NodeType.LEADER:
            self.pending_command = request  # Store the command to be executed after the next heartbeat
            return json.dumps({"status": "pending", "message": "Command will be executed after the next heartbeat"})
        else:
            return json.dumps({"status": "redirect", "leader": self.cluster_leader_addr})