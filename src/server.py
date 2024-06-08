import sys
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xmlrpc.server import SimpleXMLRPCServer
from lib.raft import RaftNode
from lib.app import Application
from lib.struct.address import Address

# Custom server to suppress logs
class QuietXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    def log_message(self, format, *args):
        pass

class QuietSimpleXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        SimpleXMLRPCServer.__init__(self, *args, **kwargs, requestHandler=QuietXMLRPCRequestHandler)

    def log_message(self, format, *args):
        pass

def start_server(host, port, contact_host=None, contact_port=None):
    app = Application()
    addr = Address(host, port)
    contact_addr = Address(contact_host, contact_port) if contact_host and contact_port else None
    
    node = RaftNode(app, addr, contact_addr)
    
    # Use the quiet server
    server = QuietSimpleXMLRPCServer((host, port), allow_none=True)
    server.register_instance(node)
    print(f"Serving on {host}:{port}...")
    server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) not in (3, 5):
        print("Usage: python server.py <host> <port> [<contact_host> <contact_port>]")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    contact_host = sys.argv[3] if len(sys.argv) == 5 else None
    contact_port = int(sys.argv[4]) if len(sys.argv) == 5 else None

    start_server(host, port, contact_host, contact_port)