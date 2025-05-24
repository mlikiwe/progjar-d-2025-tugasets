import socket
import logging
from concurrent.futures import ProcessPoolExecutor
from file_protocol import FileProtocol
import multiprocessing
import json

def process_client_data(message):
    fp = FileProtocol()
    try:
        result = fp.proses_string(message)
        return result
    except Exception as e:
        logging.warning(f"Error processing message: {e}")
        return json.dumps(dict(status='ERROR', data=str(e)))

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=45000, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        self.success_count = multiprocessing.Value('i', 0)
        self.fail_count = multiprocessing.Value('i', 0)

    def run(self):
        logging.warning(f"Server running on {self.ipinfo} with {self.executor._max_workers} workers")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(50)  # Increased backlog
        while True:
            connection, client_address = self.my_socket.accept()
            logging.warning(f"Connection from {client_address}")
            try:
                buffer = ""
                while True:
                    data = connection.recv(1024*1024)
                    if not data:
                        break
                    buffer += data.decode('utf-8', errors='ignore')
                    # Process all complete messages in the buffer
                    while "\r\n\r\n" in buffer:
                        message, buffer = buffer.split("\r\n\r\n", 1)
                        # Validate that the message is non-empty and likely JSON
                        if message.strip():
                            try:
                                # Try to parse the message as JSON to ensure it's valid
                                json.loads(message)
                                future = self.executor.submit(process_client_data, message)
                                future.add_done_callback(lambda f: self._handle_result(f, connection))
                            except json.JSONDecodeError as e:
                                logging.warning(f"Invalid JSON from {client_address}: {e}")
                                error_response = json.dumps(dict(status='ERROR', data=f"Invalid JSON: {e}"))
                                connection.sendall((error_response + "\r\n\r\n").encode())
                                with self.fail_count.get_lock():
                                    self.fail_count.value += 1
            except Exception as e:
                logging.warning(f"Error handling client {client_address}: {e}")
                with self.fail_count.get_lock():
                    self.fail_count.value += 1
            finally:
                connection.close()

    def _handle_result(self, future, connection):
        try:
            result = future.result()
            result += "\r\n\r\n"
            connection.sendall(result.encode())
            with self.success_count.get_lock():
                self.success_count.value += 1
        except Exception as e:
            logging.warning(f"Error in future result: {e}")
            with self.fail_count.get_lock():
                self.fail_count.value += 1

    def get_stats(self):
        return {"success": self.success_count.value, "fail": self.fail_count.value}

def main(max_workers=5):
    svr = Server(max_workers=max_workers)
    svr.run()

if __name__ == "__main__":
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    main(max_workers=workers)