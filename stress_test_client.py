import os
import time
import base64
import socket
import json
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from file_client_cli import send_command
import random

server_address = ('172.16.16.101', 45000)

def generate_test_file(filename, size_bytes):
    with open(filename, 'wb') as f:
        f.write(os.urandom(size_bytes))
    return filename

def client_task(operation, filename, concurrency_type="thread", worker_id=0):
    start_time = time.time()
    downloaded_file = None
    if operation == "UPLOAD":
        try:
            with open(filename, "rb") as fp:
                file_content = fp.read()
            encoded_content = base64.b64encode(file_content).decode('utf-8')
            missing_padding = len(encoded_content) % 4
            if missing_padding != 0:
                encoded_content += '=' * (4 - missing_padding)
            command_str = {
                'command': 'UPLOAD',
                'params': [filename, encoded_content]
            }
        except Exception as e:
            logging.error(f"Worker {worker_id} failed preparing upload: {e}")
            return False, 0, 0
    elif operation == "DOWNLOAD":
        command_str = {
            'command': 'GET',
            'params': [filename]
        }
    else:
        logging.error(f"Unsupported operation: {operation}")
        return False, 0, 0

    try:
        result = send_command(command_str)
        if result['status'] == 'OK':
            if operation == "DOWNLOAD":
                namafile = result['data_namafile']
                isifile = base64.b64decode(result['data_file'])
                downloaded_file = f"downloaded_{worker_id}_{namafile}"
                with open(downloaded_file, 'wb') as fp:
                    fp.write(isifile)
            end_time = time.time()
            elapsed_time = end_time - start_time
            file_size = os.path.getsize(filename) if operation == "UPLOAD" else len(isifile)
            throughput = file_size / elapsed_time if elapsed_time > 0 else 0
            if operation == "DOWNLOAD" and downloaded_file and os.path.exists(downloaded_file):
                try:
                    os.remove(downloaded_file)
                    logging.info(f"Deleted downloaded file: {downloaded_file}")
                except Exception as e:
                    logging.warning(f"Failed to delete downloaded file {downloaded_file}: {e}")
            return True, elapsed_time, throughput
        else:
            logging.error(f"Worker {worker_id} failed: {result['data']}")
            return False, 0, 0
    except Exception as e:
        logging.error(f"Worker {worker_id} exception: {e}")
        if operation == "DOWNLOAD" and downloaded_file and os.path.exists(downloaded_file):
            try:
                os.remove(downloaded_file)
                logging.info(f"Deleted downloaded file on error: {downloaded_file}")
            except Exception as e:
                logging.warning(f"Failed to delete downloaded file {downloaded_file}: {e}")
        return False, 0, 0

def run_stress_test(operation, file_size_mb, client_workers, concurrency_type="thread"):
    file_size_bytes = file_size_mb * 1024 * 1024
    filename = f"test_file_{file_size_mb}MB.bin"
    generate_test_file(filename, file_size_bytes)
    
    results = []
    executor_class = ThreadPoolExecutor if concurrency_type == "thread" else ProcessPoolExecutor
    with executor_class(max_workers=client_workers) as executor:
        futures = [
            executor.submit(client_task, operation, filename, concurrency_type, i)
            for i in range(client_workers)
        ]
        for future in futures:
            results.append(future.result())
    
    success_count = sum(1 for r in results if r[0])
    fail_count = len(results) - success_count
    total_time = sum(r[1] for r in results) / max(success_count, 1)
    total_throughput = sum(r[2] for r in results) / max(success_count, 1)
    return {
        "success": success_count,
        "fail": fail_count,
        "avg_time": total_time,
        "avg_throughput": total_throughput
    }

def main():
    parser = argparse.ArgumentParser(description="Stress test client")
    parser.add_argument(
        "--operation",
        choices=["UPLOAD", "DOWNLOAD"],
        required=True,
        help="Operation to perform (UPLOAD or DOWNLOAD)"
    )
    parser.add_argument(
        "--method",
        choices=["thread", "process"],
        required=True,
        help="Concurrency method to use (thread or process)"
    )
    parser.add_argument(
        "--volume",
        type=int,
        choices=[10, 50, 100],
        required=True,
        help="File size in MB (10, 50, or 100)"
    )
    parser.add_argument(
        "--worker",
        type=int,
        choices=[1, 5, 50],
        required=True,
        help="Number of client workers (1, 5, or 50)"
    )
    args = parser.parse_args()

    print(f"Running test: {args.operation}, {args.volume}MB, {args.worker} workers, {args.method}")
    result = run_stress_test(args.operation, args.volume, args.worker, args.method)
    
    result_entry = {
        "number": 1,
        "operation": args.operation,
        "volume_mb": args.volume,
        "client_workers": args.worker,
        "concurrency_type": args.method,
        "avg_time": result["avg_time"],
        "avg_throughput": result["avg_throughput"],
        "client_success": result["success"],
        "client_fail": result["fail"]
    }

    print("\nStress Test Results:")
    print(f"{'No':<5} {'Op':<10} {'Size(MB)':<10} {'Client Workers':<15} {'Concurrency':<12} {'Avg Time(s)':<12} {'Throughput(B/s)':<15} {'Client Success':<15} {'Client Fail':<10}")
    print(f"{result_entry['number']:<5} {result_entry['operation']:<10} {result_entry['volume_mb']:<10} {result_entry['client_workers']:<15} {result_entry['concurrency_type']:<12} {result_entry['avg_time']:<12.2f} {result_entry['avg_throughput']:<15.2f} {result_entry['client_success']:<15} {result_entry['client_fail']:<10}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()