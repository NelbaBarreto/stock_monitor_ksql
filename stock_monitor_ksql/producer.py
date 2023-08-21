import json
import websocket
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

topic = "stock-updates"

# WS handlers
def on_ws_message(ws, message):
    data = json.loads(message)["data"]
    records = [
        {
            "symbol": d["s"],
            "price": d["p"],
            "volume": d["v"],
        }
        for d in data
    ]

    for record in records:
        future = producer.send (
            topic,
            value = record
        )
        future.add_callback(on_success)
        future.add_errback(on_error)

def on_ws_error(ws, error):
    print(error)

def on_ws_close(ws, close_status_code, close_message):
    print("### closed ###")
    producer.flush()
    producer.close()

def on_ws_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

# Redpanda handlers
def on_success(metadata):
    print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")


def on_error(e):
    print(f"Error sending message: {e}")

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                              on_message = on_ws_message,
                              on_error = on_ws_error,
                              on_close = on_ws_close)
    ws.on_open = on_ws_open
    ws.run_forever()
