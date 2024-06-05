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
    print("2. get")
    print("3. set")
    print("4. strlen")
    print("5. delete")
    print("6. append")
    print("7. exit")
    return input("Pilihan Command: ")

def validate_input(value: str) -> bool:
    return value != ""

def start_serving():
    if len(sys.argv) < 3:
        print("Usage: client.py ip port")
        exit()

    addr = Address(sys.argv[1], int(sys.argv[2]))
    while True:
        choice = menu()
        if choice == "ping":
            request = {"command": "ping"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "get":
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "get", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "set":
            key = input("Key: ")
            value = input("Value: ")
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "set", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "strlen":
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "strln", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "delete":
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "delete", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "append":
            key = input("Key: ")
            value = input("Value: ")
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "append", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == "exit":
            break
        else:
            print("Invalid choice")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("client.py <ip> <port>")
        exit()

    start_serving()