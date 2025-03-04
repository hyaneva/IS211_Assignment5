
import csv
import queue
import argparse

class Request:
    def __init__(self, request_id, arrival_time, processing_time):
        self.request_id = request_id
        self.arrival_time = arrival_time
        self.processing_time = processing_time
        self.start_time = None
        self.end_time = None
    
    def process(self, start_time):
        self.start_time = start_time
        self.end_time = start_time + self.processing_time
        return self.end_time

class Server:
    def __init__(self):
        self.current_time = 0
        self.queue = queue.Queue()

    def process_request(self):
        if not self.queue.empty():
            request = self.queue.get()
            start_time = max(self.current_time, request.arrival_time)
            request.process(start_time)
            self.current_time = request.end_time
            return request
        return None

def simulate_one_server(requests):
    server = Server()
    completed_requests = []

    for request in requests:
        server.queue.put(request)

    while not server.queue.empty():
        completed_requests.append(server.process_request())

    return completed_requests

def simulate_many_servers(requests, num_servers):
    servers = [Server() for _ in range(num_servers)]
    completed_requests = []
    
    for i, request in enumerate(requests):
        server = servers[i % num_servers]  #Round-robin distribution
        server.queue.put(request)

    while any(server.queue.qsize() > 0 for server in servers):
        for server in servers:
            if not server.queue.empty():
                completed_requests.append(server.process_request())

    return completed_requests

def load_requests_from_csv(file_path):
    requests = []
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        for idx, row in enumerate(reader):
            try:
                arrival_time = int(row[0])
                processing_time = int(row[2])
                requests.append(Request(idx, arrival_time, processing_time))
            except ValueError as e:
                print(f"Skipping invalid row: {row} - Error: {e}")
    return requests

def calculate_average_latency(completed_requests):
    total_latency = sum(req.end_time - req.arrival_time for req in completed_requests)
    return total_latency / len(completed_requests) if completed_requests else 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to the CSV file containing requests")
    parser.add_argument("--servers", type=int, default=1, help="Number of servers (default: 1)")
    args = parser.parse_args()

    requests = load_requests_from_csv(args.file)

    if args.servers == 1:
        results = simulate_one_server(requests)
        print("Single server average latency:", calculate_average_latency(results))
    else:
        results = simulate_many_servers(requests, args.servers)
        print(f"Multi-server ({args.servers} servers) average latency:", calculate_average_latency(results))

if __name__ == "__main__":
    main()

