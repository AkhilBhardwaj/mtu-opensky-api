import socket
try:
    s = socket.create_connection(("akhilseventhub.servicebus.windows.net", 9093), timeout=10)
    print("Port 9093 reachable")
    s.close()
except Exception as e:
    print(f"Port 9093 BLOCKED: {e}")