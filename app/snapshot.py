import time
import requests
from app.config import NODE_ID, PEERS, logger
from app.state import state
from app.aurora_db import get_inventory

def initiate_snapshot():
    """Leader initiates a Chandy-Lamport global snapshot."""
    if not state.is_leader:
        logger.error("[SNAPSHOT] Only leader can initiate a snapshot.")
        return None

    snapshot_id = f"snap_{NODE_ID}_{int(time.time())}"
    logger.info(f"[SNAPSHOT] Initiating global snapshot: {snapshot_id}")

    # Step 1: Record own local state
    local_state = _capture_local_state()
    
    state.snapshot_data = {
        "snapshot_id": snapshot_id,
        "initiated_by": NODE_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "node_states": {
            NODE_ID: local_state
        },
        "channel_states": {}
    }
    state.snapshot_in_progress = True

    # Step 2: Send marker to all followers
    for node_id, url in PEERS.items():
        try:
            logger.info(f"[SNAPSHOT] Sending marker to Node {node_id}")
            response = requests.post(
                f"{url}/receive_marker",
                json={"snapshot_id": snapshot_id, "sender_id": NODE_ID},
                timeout=3
            )
            if response.status_code == 200:
                follower_state = response.json().get("local_state", {})
                state.snapshot_data["node_states"][node_id] = follower_state
                logger.info(f"[SNAPSHOT] Received state from Node {node_id}")
        except Exception as e:
            logger.warning(f"[SNAPSHOT] Could not reach Node {node_id}: {e}")
            state.snapshot_data["node_states"][node_id] = {"status": "unreachable"}

    state.snapshot_in_progress = False
    logger.info(f"[SNAPSHOT] Global snapshot {snapshot_id} complete.")
    return state.snapshot_data


def receive_marker(snapshot_id, sender_id):
    """Follower receives a marker: records own state and returns it."""
    logger.info(f"[SNAPSHOT] Received marker from Node {sender_id} for snapshot {snapshot_id}")
    
    local_state = _capture_local_state()
    state.last_snapshot = {
        "snapshot_id": snapshot_id,
        "local_state": local_state,
        "received_from": sender_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return local_state


def _capture_local_state():
    """Captures the current local state of this node."""
    # Inventory from DB
    inventory = {}
    for item in ["item1", "item2", "laptop", "Mechanical Keyboard"]:
        qty, status = get_inventory(item)
        inventory[item] = {"quantity": qty, "db_status": status}

    return {
        "node_id": NODE_ID,
        "is_leader": state.is_leader,
        "leader_id": state.leader_id,
        "orders_processed": len(state.order_buffer),
        "order_ids": [o.get("id") for o in state.order_buffer],
        "pending_orders": list(state.pending_orders.keys()),
        "active_locks": dict(state.locks),
        "inventory": inventory,
        "captured_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
