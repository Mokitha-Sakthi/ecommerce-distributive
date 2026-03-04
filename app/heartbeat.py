import time
import threading
import requests
from app.config import NODE_ID, PEERS, logger
from app.state import state
from app.election import start_election

HEARTBEAT_INTERVAL = 3  # Seconds
FAILURE_TIMEOUT = 10    # Seconds

def send_heartbeat():
    """Leader sends heartbeat to all followers."""
    while True:
        if state.is_leader:
            for node_id, url in PEERS.items():
                try:
                    requests.post(f"{url}/heartbeat", json={"leader_id": NODE_ID}, timeout=1)
                except Exception:
                    pass # Quietly ignore follower downtime in logs
        time.sleep(HEARTBEAT_INTERVAL)

def monitor_leader():
    """Followers monitor leader's heartbeat."""
    while True:
        if not state.is_leader and state.leader_id is not None:
            time_since_last = time.time() - state.last_heartbeat
            if time_since_last > FAILURE_TIMEOUT:
                logger.warning(f"[HEARTBEAT] Leader {state.leader_id} timeout! Last seen {time_since_last:.1f}s ago.")
                state.leader_id = None
                start_election()
        elif not state.is_leader and state.leader_id is None and not state.election_in_progress:
            logger.info("[HEARTBEAT] No leader known. Starting election.")
            start_election()
        
        time.sleep(2)

def start_heartbeat_threads():
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=monitor_leader, daemon=True).start()
    logger.debug("[HEARTBEAT] Heartbeat threads started.")
