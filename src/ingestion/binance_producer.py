import os
import json
import websocket
from kafka import KafkaProducer
from dotenv import load_dotenv

from src.utils.logger import get_logger

load_dotenv()

# Configure logging for observability for this specific file
logger = get_logger(__name__)


# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
# Binance WebSocket URL for live BTC/USDT trades
BINANCE_WS_URL = os.getenv('BINANCE_WS_URL')

if not KAFKA_BROKER or not KAFKA_TOPIC or not BINANCE_WS_URL:
    logger.error("CRITICAL: Missing environment variables. Please check your .env file.")
    exit(1)



# Initialize Kafka Producer
# We serialize the data as JSON before sending it to Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    exit(1)

def on_message(ws, message):
    """
    Triggered every time a new trade message arrives from Binance.
    """
    try:
        # Load the raw string into a Python dictionary
        data = json.loads(message)
        
        # Send the payload to our Kafka topic
        producer.send(KAFKA_TOPIC, data)
        
        # We use a brief log to show activity. In a true production environment 
        # doing thousands of messages a second, you'd sample or batch these logs.
        logger.info(f"Sent trade to Kafka: Price={data.get('p')}, Quantity={data.get('q')}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket connection closed.")
    producer.flush() # Ensure all messages are sent before shutting down
    producer.close()

def on_open(ws):
    logger.info(f"Connected to Binance WebSocket: {BINANCE_WS_URL}")
    logger.info("Streaming live trades to Kafka...")

if __name__ == "__main__":
    # Create the persistent WebSocket connection
    ws = websocket.WebSocketApp(
        
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run the connection forever (until you press Ctrl+C)
    ws.run_forever()