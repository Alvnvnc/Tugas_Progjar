import socket
import json
import base64
import logging
import os

server_address=('0.0.0.0',7777)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    logging.warning(f"connecting to {server_address}")
    try:
        logging.warning(f"sending message ")
        sock.sendall(command_str.encode())
        # Look for the response, waiting until socket is done (no more data)
        data_received="" #empty string
        while True:
            #socket does not receive all data at once, data comes in part, need to be concatenated at the end of process
            data = sock.recv(16)
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
    command_str=f"LIST"
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
    command_str=f"GET {filename}"
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

def remote_upload(local_filename="", remote_filename=""):
    """
    Upload a file to the server
    local_filename: path to the file on the local machine
    remote_filename: filename to be used on the server (if empty, use local filename)
    """
    if not os.path.exists(local_filename):
        print(f"Error: file {local_filename} not found")
        return False
    
    # If remote filename is not specified, use the local filename
    if remote_filename == "":
        remote_filename = os.path.basename(local_filename)
    
    # Read the file and encode it in base64
    with open(local_filename, 'rb') as fp:
        file_content = base64.b64encode(fp.read()).decode()
    
    command_str = f"UPLOAD {remote_filename} {file_content}"
    hasil = send_command(command_str)
    
    if (hasil['status'] == 'OK'):
        print(f"File successfully uploaded as {remote_filename}")
        return True
    else:
        print(f"Upload failed: {hasil['data']}")
        return False

def remote_delete(filename=""):
    """
    Delete a file from the server
    filename: name of the file to delete on the server
    """
    if filename == "":
        print("Error: filename not specified")
        return False
    
    command_str = f"DELETE {filename}"
    hasil = send_command(command_str)
    
    if (hasil['status'] == 'OK'):
        print(f"File {filename} successfully deleted from server")
        return True
    else:
        print(f"Delete failed: {hasil['data']}")
        return False


if __name__=='__main__':
    server_address=('172.16.16.101',6665)
    
    while True:
        print("\n===== FILE CLIENT MENU =====")
        print("1. List files on server")
        print("2. Download file")
        print("3. Upload file")
        print("4. Delete file")
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ")
        
        if choice == '1':
            print("\nListing files on server:")
            remote_list()
            
        elif choice == '2':
            filename = input("Enter filename to download: ")
            if filename:
                print(f"\nDownloading {filename}...")
                if remote_get(filename):
                    print(f"Successfully downloaded {filename}")
                else:
                    print(f"Failed to download {filename}")
            
        elif choice == '3':
            local_file = input("Enter local file path to upload: ")
            if os.path.exists(local_file):
                remote_name = input("Enter remote filename (leave empty to use local filename): ")
                print(f"\nUploading {local_file} to server...")
                remote_upload(local_file, remote_name)
            else:
                print(f"Local file {local_file} not found")
            
        elif choice == '4':
            # First list files to show what's available
            print("\nCurrent files on server:")
            remote_list()
            # Then ask for file to delete
            filename = input("\nEnter filename to delete: ")
            if filename:
                print(f"\nDeleting {filename} from server...")
                remote_delete(filename)
            
        elif choice == '5':
            print("Exiting program...")
            break
            
        else:
            print("Invalid choice. Please enter a number between 1-5.")

