import requests
from flask import Flask,jsonify
from flask_sock import Sock
import json
import websocket
import time
import threading
from config import worker_http_urls worker_ws_urls
speedlock = threading.Lock()

#print(websocket._exceptions.WebSocketException)
app = Flask(__name__)
sock = Sock(app)



lastSpeed = [0.1]

def get_available_worker():
    """
    Check each worker's /count endpoint to see if it's available.
    If count (resources in use) is less than total, the worker is considered available.
    Returns the HTTP URL of the selected worker or None if none are available.
    """
    available_worker = None
    min_load = float('inf')
    for worker_http in worker_http_urls:
        try:
            resp = requests.get(f"{worker_http}/count", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                used = data.get("count", 0)
                total = data.get("total", 1)
                # If the worker is not fully used and has the smallest load so far, choose it.
                if used < total and used < min_load:
                    min_load = used
                    available_worker = worker_http
        except Exception as e:
            # Skip workers that can't be reached.
            continue
    return available_worker

@app.route("/dbg")
def dbg():
    usedTotal = 0
    totalTotal = 0
    for worker_http in worker_http_urls:
        try:
            resp = requests.get(f"{worker_http}/count", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                used = data.get("count", 0)
                total = data.get("total",1)
                totalTotal += total
                usedTotal += used
        except Exception as e:
            # Skip workers that can't be reached.
            print(repr(e))
            continue
    return jsonify({"total":totalTotal,"count":usedTotal,"averageTokensPerSecond":1/(sum(lastSpeed)/len(lastSpeed))})

@sock.route('/worker')
def proxy_worker(client_ws):
    """
    When a client connects to /worker:
      1. Wait for the initial message.
      2. Find an available worker by checking its /count.
      3. Connect to the worker's WebSocket endpoint.
      4. Send the initial message and then stream responses back to the client.
    """
    # Wait for the first message from the client.
    initial_message = client_ws.receive()
    if not initial_message:
        client_ws.send("No initial message received. Closing connection.")
        return

    # Find an available worker.
    worker_http = get_available_worker()
    if not worker_http:
        client_ws.send("No available workers at the moment. Please try again later.")
        return

    worker_ws_url = worker_ws_urls.get(worker_http)
    if not worker_ws_url:
        client_ws.send("Worker WebSocket URL not configured.")
        return

    try:
        # Connect to the worker's WebSocket endpoint.
        worker_ws = websocket.create_connection(worker_ws_url)
    except Exception as e:
        client_ws.send("Error connecting to worker: " + str(e))
        return

    try:
        # Forward the initial client message to the worker.
        worker_ws.send(initial_message)
        # Relay responses from the worker to the client until the connection is closed.
        while True:
            startTime = time.time()
            message = worker_ws.recv()
            if message is None:
                break
            with speedlock:
                endTime = time.time()
                lastSpeed.append(endTime - startTime)
                if len(lastSpeed) > 100:
                    lastSpeed.pop(0)
            client_ws.send(message)
    except websocket._exceptions.WebSocketException:
        pass
    except Exception as e:
        client_ws.send("Error during communication: " + str(e))
    finally:
        worker_ws.close()

@app.route("/")
def ahh():
    return "hi"

if __name__ == "__main__":
    # Run the Compute Manager on port 4000 (adjust as needed)
    app.run(host="0.0.0.0", port=5000)
