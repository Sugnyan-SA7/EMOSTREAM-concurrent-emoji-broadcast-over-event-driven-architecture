import multiprocessing
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import socket
import threading
import json

# Kafka configurations
MAIN_BROKER = "localhost:9092"
EMOJI_STREAM_TOPIC = "aggregated_emojis"

# Cluster Configurations
CLUSTER_CONFIGS = {
    "cluster1": {
        "sub_publisher_topic": "cluster1_sub_publisher_topic",
        "subscribers": [
            {
                "topic": "cluster1_subscriber1_topic",
                "client_port": 5001,
                "client_topics": ["client1_topic1", "client1_topic2"]
            },
            {
                "topic": "cluster1_subscriber2_topic", 
                "client_port": 5002,
                "client_topics": ["client2_topic1", "client2_topic2"]
            }
        ]
    },
    "cluster2": {
        "sub_publisher_topic": "cluster2_sub_publisher_topic",
        "subscribers": [
            {
                "topic": "cluster2_subscriber1_topic",
                "client_port": 5003,
                "client_topics": ["client3_topic1", "client3_topic2"]
            },
            {
                "topic": "cluster2_subscriber2_topic", 
                "client_port": 5004,
                "client_topics": ["client4_topic1", "client4_topic2"]
            }
        ]
    }
}

class SocketServer:
    """Socket server for broadcasting messages to connected clients."""
    def __init__(self, port, max_clients=5):
        self.port = port
        self.max_clients = max_clients
        self.clients = []
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("0.0.0.0", self.port))
        self.server_socket.listen(self.max_clients)
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self.accept_clients, daemon=True).start()

    def accept_clients(self):
        print(f"SocketServer running on port {self.port}")
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"Client connected from {addr}")
                self.clients.append(client_socket)
            except Exception as e:
                print(f"Error accepting clients: {e}")

    def broadcast(self, data):
        """Send data to all connected clients."""
        for client in self.clients:
            try:
                client.sendall(json.dumps(data).encode('utf-8'))
            except Exception as e:
                print(f"Error sending data to client: {e}")
                self.clients.remove(client)

    def stop(self):
        self.running = False
        for client in self.clients:
            client.close()
        self.server_socket.close()

def main_publisher():
    """Main publisher that distributes data to sub-publishers"""
    consumer = KafkaConsumer(
        EMOJI_STREAM_TOPIC,
        bootstrap_servers=[MAIN_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='main_publisher_group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Create producers for each cluster's sub-publisher
    sub_publishers = {
        cluster: KafkaProducer(
            bootstrap_servers=[MAIN_BROKER],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        for cluster in CLUSTER_CONFIGS
    }

    try:
        for message in consumer:
            emoji_data = message.value
            print(f"Main Publisher Received: {emoji_data}")
            
            # Distribute to each cluster's sub-publisher
            for cluster, producer in sub_publishers.items():
                sub_publisher_topic = CLUSTER_CONFIGS[cluster]["sub_publisher_topic"]
                producer.send(sub_publisher_topic, value=emoji_data)
                producer.flush()
                print(f"Sent to {cluster} sub-publisher: {emoji_data}")
    except Exception as e:
        print(f"Main Publisher Error: {e}")
    finally:
        consumer.close()
        for producer in sub_publishers.values():
            producer.close()

def sub_publisher(cluster_name):
    """Sub-publisher for a specific cluster"""
    sub_publisher_consumer = KafkaConsumer(
        CLUSTER_CONFIGS[cluster_name]["sub_publisher_topic"],
        bootstrap_servers=[MAIN_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'{cluster_name}_sub_publisher_group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    subscriber_producers = {}
    for subscriber_config in CLUSTER_CONFIGS[cluster_name]["subscribers"]:
        subscriber_producers[subscriber_config["topic"]] = KafkaProducer(
            bootstrap_servers=[MAIN_BROKER],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    try:
        for message in sub_publisher_consumer:
            data = message.value
            print(f"{cluster_name} Sub-Publisher Received: {data}")
            
            for subscriber_topic, producer in subscriber_producers.items():
                producer.send(subscriber_topic, value=data)
                producer.flush()
                print(f"Sent to {subscriber_topic}: {data}")
    except Exception as e:
        print(f"{cluster_name} Sub-Publisher Error: {e}")
    finally:
        sub_publisher_consumer.close()
        for producer in subscriber_producers.values():
            producer.close()

def subscriber(cluster_name, subscriber_config):
    """Subscriber that receives from sub-publisher and serves clients"""
    subscriber_consumer = KafkaConsumer(
        subscriber_config["topic"],
        bootstrap_servers=[MAIN_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'{cluster_name}_subscriber_group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    socket_server = SocketServer(
        port=subscriber_config["client_port"], 
        max_clients=subscriber_config.get("max_clients", 2)
    )
    socket_server.start()

    try:
        for message in subscriber_consumer:
            data = message.value
            print(f"Subscriber Received: {data}")
            socket_server.broadcast(data)
    except Exception as e:
        print(f"Subscriber Error: {e}")
    finally:
        subscriber_consumer.close()
        socket_server.stop()

def start_cluster_components(cluster_name):
    """Start all components for a specific cluster"""
    sub_pub_process = multiprocessing.Process(
        target=sub_publisher, 
        args=(cluster_name,)
    )
    sub_pub_process.start()

    subscriber_processes = []
    for subscriber_config in CLUSTER_CONFIGS[cluster_name]["subscribers"]:
        sub_process = multiprocessing.Process(
            target=subscriber, 
            args=(cluster_name, subscriber_config)
        )
        sub_process.start()
        subscriber_processes.append(sub_process)

    return sub_pub_process, subscriber_processes

def main():
    main_pub_process = multiprocessing.Process(target=main_publisher)
    main_pub_process.start()

    cluster_processes = {}
    for cluster_name in CLUSTER_CONFIGS:
        cluster_processes[cluster_name] = start_cluster_components(cluster_name)

    try:
        main_pub_process.join()
        for cluster_processes_list in cluster_processes.values():
            cluster_processes_list[0].join()
            for process in cluster_processes_list[1]:
                process.join()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        main_pub_process.terminate()
        for cluster_processes_list in cluster_processes.values():
            cluster_processes_list[0].terminate()
            for process in cluster_processes_list[1]:
                process.terminate()

if __name__ == "__main__":
    main()
