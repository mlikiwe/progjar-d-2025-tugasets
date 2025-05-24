import socket
import json
import base64
import logging
import os

server_address=('172.16.16.101', 45000)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    logging.warning(f"connecting to {server_address}")
    try:
        logging.warning(f"sending message ")
        command_str = json.dumps(command_str) + '\r\n\r\n'
        sock.sendall(command_str.encode())
        # Look for the response, waiting until socket is done (no more data)
        data_received="" #empty string
        while True:
            #socket does not receive all data at once, data comes in part, need to be concatenated at the end of process
            data = sock.recv(8192)
            if data:
                #data is not empty, concat with previous content
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                # no more data, stop the process by break
                break
        # at this point, data_received (string) will contain all data coming from the socket
        # to be able to use the data_received as a dict, need to load it using json.loads()
        hasil = json.loads(data_received)
        logging.warning("data received from server:")
        return hasil
    except:
        logging.warning("error during data receiving")
        return False


def remote_list():
    command_str={
        'command': 'LIST',
        'params': []
    }
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        print("daftar file : ")
        for nmfile in hasil['data']:
            print(f"- {nmfile}")
        return True
    else:
        print("Gagal")
        return False

def remote_get(filename=""):
    command_str={
        'command': 'GET',
        'params': [filename]
    }
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        #proses file dalam bentuk base64 ke bentuk bytes
        namafile= hasil['data_namafile']
        isifile = base64.b64decode(hasil['data_file'])
        fp = open(namafile,'wb+')
        fp.write(isifile)
        fp.close()
        return True
    else:
        print("Gagal")
        return False
    
def remote_upload(filename=""):
    if not os.path.exists(filename):
        print(f"File '{filename}' tidak ditemukan.")
        return False

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
        hasil = send_command(command_str)
        if hasil['status'] == 'OK':
            print(f"File '{filename}' berhasil diupload.")
            return True
        else:
            print("Gagal upload.")
            return False
    except Exception as e:
        print(f"Error saat upload: {e}")
        return False
    
def remote_delete(filename=""):
    command_str = {
        'command': 'DELETE',
        'params': [filename]
    }
    hasil = send_command(command_str)
    if hasil['status'] == 'OK':
        print(f"File '{filename}' berhasil dihapus.")
        return True
    else:
        print("Gagal hapus.")
        return False


import shlex

if __name__ == '__main__':
    print("Ketik perintah: LIST | GET <namafile> | UPLOAD <namafile> | DELETE <namafile> | EXIT")

    while True:
        try:
            perintah = input(">>> ").strip()
            if not perintah:
                continue

            tokens = shlex.split(perintah)
            cmd = tokens[0].upper()

            if cmd == 'EXIT':
                print("Keluar dari aplikasi.")
                break
            elif cmd == 'LIST':
                remote_list()
            elif cmd == 'GET':
                if len(tokens) >= 2:
                    remote_get(tokens[1])
                else:
                    print("Format: GET <namafile>")
            elif cmd == 'UPLOAD':
                if len(tokens) >= 2:
                    remote_upload(tokens[1])
                else:
                    print("Format: UPLOAD <namafile>")
            elif cmd == 'DELETE':
                if len(tokens) >= 2:
                    remote_delete(tokens[1])
                else:
                    print("Format: DELETE <namafile>")
            else:
                print("Perintah tidak dikenal.")
        except KeyboardInterrupt:
            print("\nKeluar dari aplikasi.")
            break