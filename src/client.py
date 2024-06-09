import sys
import json
import traceback
from xmlrpc.client import ServerProxy


from lib.struct.address import Address
from lib.struct.client_rpc import ClientRPC

def send_request(request: dict, method: str, addr: Address) -> dict:
    node = ServerProxy(f"http://{addr.ip}:{addr.port}")
    json_request = json.dumps(request)
    rpc_function = getattr(node, method)
    response = ClientRPC.Response(ClientRPC.FAILED).to_dict()

    while response["status"] == ClientRPC.FAILED:
        print("[REQUEST] Sending to server")
        try:
            response = json.loads(rpc_function(json_request))
        except KeyboardInterrupt:
            break
        except:
            traceback.print_exc()
            print("[RESPONSE] Can't connect to server. retrying...")
            continue

    return response

def menu():
    print("Pilih salah satu menu:")
    print("1. ping")
    print("2. get <key>")
    print("3. set <key> <value>")
    print("4. strlen <key>")
    print("5. delete <key>")
    print("6. append <key> <value>")
    print("7. changeAddress <ip> <port>")
    print("8. exit")
    return input("Pilihan Command: ")

def validate_input(value: str) -> bool:
    return value != ""

def start_serving():
    if len(sys.argv) < 3:
        print("Usage: client.py ip port")
        exit()

    addr = Address(sys.argv[1], int(sys.argv[2]))
    while True:
        choice = menu().split(" ")
        if choice[0] == "ping":
            request = {"command": "ping"}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "get":
            key = choice[1]
            if len(choice) < 2:
                print("Usage: get <key>")
                continue
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "get", "args": key}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "set":
            if len(choice) < 3:
                print("Usage: set <key> <value>")
                continue
            key = choice[1]
            value = choice[2]
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "set", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "strlen":
            if len(choice) < 2:
                print("Usage: strlen <key>")
                continue
            key = choice[1]
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "strln", "args": key}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "delete":
            if len(choice) < 2:
                print("Usage: delete <key>")
                continue
            key = choice[1]
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "delete", "args": key}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "append":
            if len(choice) < 3:
                print("Usage: append <key> <value>")
                continue
            key = choice[1]
            value = choice[2]
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "append", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "changeAddress":
            if len(choice) < 3:
                print("Usage: changeAddress <ip> <port>")
                continue
            addr = Address(choice[1], int(choice[2]))
            print(f"Address changed to {addr.ip}:{addr.port}")
        elif choice[0] == "exit":
            break
        elif choice[0] == "leader_log_test":
            request = {"command": "leader_log_test"}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        elif choice[0] == "follower_log_test":
            request = {"command": "follower_log_test"}
            response = send_request(request, "execute", addr)
            print(json.loads(response["response"])["response"])
        else:
            print("Invalid choice")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("client.py <ip> <port>")
        exit()

    start_serving()