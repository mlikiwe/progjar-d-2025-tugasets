import socket
import logging
from concurrent.futures import ThreadPoolExecutor
from file_protocol import FileProtocol

class ProcessTheClient:
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        self.fp = FileProtocol()

    def process(self):
        buffer = ""
        try:
            while True:
                data = self.connection.recv(8192)
                if not data:
                    break
                buffer += data.decode()
                while "\r\n\r\n" in buffer:
                    message, buffer = buffer.split("\r\n\r\n", 1)
                    result = self.fp.proses_string(message)
                    result += "\r\n\r\n"
                    self.connection.sendall(result.encode())
            return True
        except Exception as e:
            logging.warning(f"Error processing client {self.address}: {e}")
            return False
        finally:
            self.connection.close()

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=45000, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.success_count = 0
        self.fail_count = 0

    def run(self):
        logging.warning(f"Server running on {self.ipinfo} with {self.executor._max_workers} workers")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(50)
        while True:
            connection, client_address = self.my_socket.accept()
            logging.warning(f"Connection from {client_address}")
            client_handler = ProcessTheClient(connection, client_address)
            future = self.executor.submit(client_handler.process)
            future.add_done_callback(self._handle_result)

    def _handle_result(self, future):
        if future.result():
            self.success_count += 1
        else:
            self.fail_count += 1

    def get_stats(self):
        return {"success": self.success_count, "fail": self.fail_count}

def main(max_workers=5):
    svr = Server(max_workers=max_workers)
    svr.run()

if __name__ == "__main__":
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    main(max_workers=workers)