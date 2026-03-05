import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DistributedOrderSystem")

NODE_ID = int(os.getenv("NODE_ID", 1))
PORT = int(os.getenv("PORT", 8000))
HOST = os.getenv("HOST", "0.0.0.0")

# PEERS dictionary: {id: "url"}
# Example PEERS: {2: "http://192.168.1.102:8002", ...}
PEERS = {}
for i in range(1, 5):
    if i == NODE_ID:
        continue
    peer_url = os.getenv(f"PEER_{i}_URL")
    if peer_url:
        PEERS[i] = peer_url

# Aurora DB Config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "dbms8759"),
    "database": os.getenv("DB_NAME", "ecommerce"),
} 

logger.debug(f"[CONFIG] NODE_ID: {NODE_ID}, PORT: {PORT}")
logger.debug(f"[CONFIG] PEERS: {PEERS}")
