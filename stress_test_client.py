import os
import time
import base64
import socket
import json
import logging
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
                with open(f"downloaded_{worker_id}_{namafile}", 'wb') as fp:
                    fp.write(isifile)
            end_time = time.time()
            elapsed_time = end_time - start_time
            file_size = os.path.getsize(filename) if operation == "UPLOAD" else len(isifile)
            throughput = file_size / elapsed_time if elapsed_time > 0 else 0
            return True, elapsed_time, throughput
        else:
            logging.error(f"Worker {worker_id} failed: {result['data']}")
            return False, 0, 0
    except Exception as e:
        logging.error(f"Worker {worker_id} exception: {e}")
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
    operations = ["UPLOAD", "DOWNLOAD"]
    file_sizes_mb = [10, 50, 100]
    client_workers = [1, 5, 50]
    concurrency_types = ["thread", "process"]
    
    results = []
    test_number = 1
    
    for op in operations:
        for size in file_sizes_mb:
            for workers in client_workers:
                for concurrency in concurrency_types:
                    print(f"Running test {test_number}: {op}, {size}MB, {workers} workers, {concurrency}")
                    result = run_stress_test(op, size, workers, concurrency)
                    results.append({
                        "number": test_number,
                        "operation": op,
                        "volume_mb": size,
                        "client_workers": workers,
                        "concurrency_type": concurrency,
                        "avg_time": result["avg_time"],
                        "avg_throughput": result["avg_throughput"],
                        "client_success": result["success"],
                        "client_fail": result["fail"]
                    })
                    test_number += 1
    
    # Print results table
    print("\nStress Test Results:")
    print(f"{'No':<5} {'Op':<10} {'Size(MB)':<10} {'Client Workers':<15} {'Concurrency':<12} {'Avg Time(s)':<12} {'Throughput(B/s)':<15} {'Client Success':<15} {'Client Fail':<10}")
    for r in results:
        print(f"{r['number']:<5} {r['operation']:<10} {r['volume_mb']:<10} {r['client_workers']:<15} {r['concurrency_type']:<12} {r['avg_time']:<12.2f} {r['avg_throughput']:<15.2f} {r['client_success']:<15} {r['client_fail']:<10}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()