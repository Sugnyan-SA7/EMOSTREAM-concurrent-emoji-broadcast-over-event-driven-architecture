from flask import Flask, request, jsonify
from kafka import KafkaProducer
from json import dumps
from threading import Thread
from queue import Queue
import time

app = Flask(__name__)

KAFKA_TOPIC = "emoji_stream"
KAFKA_SERVER = "localhost:9092" 

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

client_queues = {}
client_threads = {}

def process_emojis_for_user(user_id):
    """ Continuously process emojis from the user's queue. """
    last_flush_time = time.time()

    while True:
        emojis_data = client_queues[user_id].get()
        if emojis_data is None: 
            break

        for emoji in emojis_data:
            emoji_entry = {
                "user_id": user_id,
                "emoji_type": emoji["emoji_type"],
                "timestamp": emoji["timestamp"]
            }
            producer.send(KAFKA_TOPIC, value=emoji_entry)
            #print(f"Queued message to Kafka: {emoji_entry}")

        if time.time() - last_flush_time >= 0.5:
            producer.flush()
            last_flush_time = time.time()
            #print("Flushed messages to Kafka")

@app.route('/process_emojis', methods=['POST'])
def process_emojis():
    try:
        user_id = request.json.get('user_id')
        emojis_data = request.json.get('emojis_data', [])
        thread_ident = request.json.get('thread_ident')

        if thread_ident == 'get':
            if user_id not in client_queues:
                client_queues[user_id] = Queue()
                client_thread = Thread(target=process_emojis_for_user, args=(user_id,), daemon=True)
                client_thread.start()
                client_threads[user_id] = client_thread.ident

                print(f"New thread (ident: {client_thread.ident}) created for user {user_id}")

            return jsonify({"status": "success", "thread_ident": client_threads[user_id]}), 200

        if user_id in client_threads and client_threads[user_id] != thread_ident:
            return jsonify({"status": "error", "message": "Invalid thread ident"}), 400

        client_queues[user_id].put(emojis_data)
        
        return jsonify({"status": "success", "message": "Emojis received and queued for processing"}), 200

    except Exception as e:
        print(f"Error processing emojis: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

try:
    if __name__ == "__main__":
        app.run(port=5000, threaded=True)
except KeyboardInterrupt:
    print("Server interrupted, sending stop signal to consumer.")
    producer.send(KAFKA_TOPIC, value={"stop": True})
    producer.flush()
finally:
    producer.close()
