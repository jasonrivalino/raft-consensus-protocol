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
    print("1. Ping")
    print("2. Get")
    print("3. Set")
    print("4. Strlen")
    print("5. Delete")
    print("6. Append")
    print("7. Exit")
    return int(input("Choose: "))

def validate_input(value: str) -> bool:
    return value != ""

def main():
    if len(sys.argv) < 3:
        print("Usage: client.py ip port")
        exit()

    addr = Address(sys.argv[1], int(sys.argv[2]))
    while True:
        choice = menu()
        if choice == 1:
            request = {"command": "ping"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 2:
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "get", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 3:
            key = input("Key: ")
            value = input("Value: ")
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "set", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 4:
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "strln", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 5:
            key = input("Key: ")
            if not validate_input(key):
                print("Key cannot be empty")
                continue
            request = {"command": "delete", "args": key}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 6:
            key = input("Key: ")
            value = input("Value: ")
            if not validate_input(key) or not validate_input(value):
                print("Key and value cannot be empty")
                continue
            request = {"command": "append", "args": f"{key} {value}"}
            response = send_request(request, "execute", addr)
            print(response)
        elif choice == 7:
            break
        else:
            print("Invalid choice")

if __name__ == "__main__":
    main()
