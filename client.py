import random
import time
import requests
import sys
from datetime import datetime
import threading
import socket
import json
import multiprocessing 

emojis = ['🏏', '🏆', '🎉', '🔥', '❤️']

server_url = "http://127.0.0.1:5000/process_emojis"

exit_program = multiprocessing.Value('b', False)  
client_threads = {}


def generate_emojis(user_id, thread_ident):
    
    global exit_program

    while not exit_program.value:
        emoji_data = []
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for _ in range(450):
            random_emoji = random.choice(emojis)
            emoji_data.append({
                "user_id": user_id,
                "emoji_type": random_emoji,
                "timestamp": timestamp
            })

        try:
            response = requests.post(server_url, json={'user_id': user_id, 'emojis_data': emoji_data, 'thread_ident': thread_ident})
            if response.status_code == 200:
                print(f"Sent emojis for user {user_id} to the server.")
            else:
                print(f"Failed to send emojis for user {user_id}. Server responded with {response.status_code}.")
        except Exception as e:
            print(f"Error sending emojis for user {user_id}: {str(e)}")

        time.sleep(1)


def client_receive_data(host, port, client_name):
    
    global exit_program
    if exit_program.value == True:
        print("Exiting the program...")
        sys.exit(1)

    try:
       
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        print(f"{client_name} connected to server at {host}:{port}")

        while not exit_program.value:
            if exit_program.value == True:
                print("Exiting the program...")
                sys.exit(1)
            data = client_socket.recv(1024)
            if data:
                decoded_data = json.loads(data.decode('utf-8'))
                
            
                if isinstance(decoded_data, list):
                    for item in decoded_data:
                        print(item)
                else:
                    print(decoded_data)
            time.sleep(1)
    except Exception as e:
        print(f"{client_name} encountered a connection error: {e}")
    finally:
        client_socket.close()


def capture_input():
    
    global exit_program
    while not exit_program.value:
        command = input("Press 'e' to exit: ").strip().lower()
        if command == 'e':
            exit_program.value = True
            print("Exiting the program...")
            sys.exit(1)


def send_and_receive(host, port, user_id, client_name):
   
    global exit_program
    if exit_program.value == True:
        print("Exiting the program...")
        sys.exit(1)
    

    receive_process = multiprocessing.Process(target=client_receive_data, args=(host, port, client_name))
    receive_process.start()

    
    input_thread = threading.Thread(target=capture_input)
    input_thread.start()

    
    response = requests.post(server_url, json={'user_id': user_id, 'emojis_data': [], 'thread_ident': 'get'})
    if response.status_code == 200:
        response_json = response.json()
        if 'thread_ident' in response_json:
            thread_ident = response_json['thread_ident']
            client_threads[user_id] = thread_ident

            
            send_process = multiprocessing.Process(target=generate_emojis, args=(user_id, thread_ident))
            send_process.start()

          
            send_process.join()
            receive_process.join()

        else:
            print("Server response did not contain 'thread_ident'. Check server code or response format.")
    else:
        print(f"Failed to get thread ident from the server. Server responded with {response.status_code}.")
        print("Server response:", response.text)


if __name__ == "__main__":
   
    host = "localhost"
    port = int(input("Enter the port number to connect to: "))
    client_name = input("Enter your client name: ")

  
    print("\nWelcome to Emo-stream", client_name)
    choice = int(input("\n1] Send and Receive Emojis.\n2] Receive Emojis Only.\n"))

    if choice == 1:
        if len(sys.argv) != 2:
            print("Usage: python client_code.py <user_id>")
            sys.exit(1)

        user_id = sys.argv[1]
        send_and_receive(host, port, user_id, client_name)

    elif choice == 2:
       
        input_thread = threading.Thread(target=capture_input)
        input_thread.start()
        client_receive_data(host, port, client_name)

    else:
        print("Please enter 1 or 2 only!")

