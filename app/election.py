import requests
import threading
from app.config import NODE_ID, PEERS, logger
from app.state import state

def start_election():
    if state.election_in_progress:
        logger.debug("[ELECTION] Election already in progress, skipping.")
        return
    
    state.election_in_progress = True
    logger.info(f"[ELECTION] Node {NODE_ID} starting election.")
    
    higher_nodes = [id for id in PEERS if id > NODE_ID]
    
    if not higher_nodes:
        # I am the highest ID node
        logger.debug("[ELECTION] I am the highest node. Declaring leadership.")
        announce_leadership()
        return

    any_response = False
    for node_id in higher_nodes:
        url = f"{PEERS[node_id]}/start_election"
        try:
            logger.debug(f"[ELECTION] Sending election message to Node {node_id} at {url}")
            response = requests.post(url, timeout=2)
            if response.status_code == 200:
                any_response = True
                logger.debug(f"[ELECTION] Node {node_id} responded to election.")
        except Exception:
            logger.debug(f"[ELECTION] Node {node_id} is unreachable or busy.")

    if not any_response:
        logger.debug("[ELECTION] No higher nodes responded. Declaring leadership.")
        announce_leadership()

def announce_leadership():
    state.leader_id = NODE_ID
    state.is_leader = True
    state.election_in_progress = False
    
    # State Cleanup for New Leader
    state.locks = {}
    state.snapshot_data = {}
    state.snapshot_in_progress = False
    
    logger.info(f"[ELECTION] Node {NODE_ID} is now the leader.")
    
    # Broadcast leadership to all peers
    for node_id, url in PEERS.items():
        try:
            logger.debug(f"[ELECTION] Announcing leadership to Node {node_id} at {url}/leader_announce")
            requests.post(f"{url}/leader_announce", json={"leader_id": NODE_ID}, timeout=1)
        except Exception:
            logger.debug(f"[ELECTION] Could not announce leadership to Node {node_id}")

def receive_election(caller_id):
    logger.debug(f"[ELECTION] Received election message from Node {caller_id}")
    # Higher nodes should start their own election
    threading.Thread(target=start_election).start()
    return True

def receive_leader_announce(leader_id):
    logger.info(f"[ELECTION] Node {leader_id} announced as new leader.")
    state.leader_id = leader_id
    state.is_leader = (leader_id == NODE_ID)
    state.election_in_progress = False
    
    # State Cleanup for Followers
    state.locks = {}
    state.snapshot_in_progress = False
    state.last_heartbeat = __import__('time').time()
