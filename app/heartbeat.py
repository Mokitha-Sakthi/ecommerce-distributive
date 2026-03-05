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
        
        # Better Candidate / Join Reconciliation Trigger
        if not state.is_leader and state.leader_id is not None:
            if not state.synced_once:
                # Trigger sync check as soon as a leader is found
                should_take_over = (NODE_ID > state.leader_id)
                threading.Thread(target=sync_with_leader, args=(state.leader_id, should_take_over), daemon=True).start()
                state.synced_once = True
            elif NODE_ID > state.leader_id and not state.election_in_progress:
                logger.info(f"[ELECTION] Higher priority node detected (ID {NODE_ID} > {state.leader_id}). Taking over.")
                start_election()
            
        time.sleep(2)

def sync_with_leader(leader_id, elect_after=False):
    """Fetches full state from leader and updates local DB if different."""
    from app.aurora_db import overwrite_local_data, get_db_summary
    leader_url = PEERS.get(leader_id)
    if not leader_url: return
    
    try:
        # Step 1: Check Metadata first
        logger.info(f"[SYNC] Checking state consistency with Leader {leader_id}...")
        resp = requests.get(f"{leader_url}/get_sync_summary", timeout=5)
        if resp.status_code == 200:
            leader_summary = resp.json().get("summary")
            local_summary = get_db_summary()
            
            if leader_summary == local_summary:
                logger.info("[SYNC] Local database is already in sync with leader. No data pull needed.")
                if elect_after:
                    from app.election import start_election
                    start_election()
                return
                
            # Step 2: Fetch full data only if summary differs
            logger.info(f"[SYNC] Discrepancy detected (Leader: {leader_summary}, Local: {local_summary}). Pulling full data...")
            response = requests.get(f"{leader_url}/get_all_data", timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "success" and result.get("data"):
                    logger.info(f"[SYNC] Overwriting local tables with leader data...")
                    overwrite_local_data(result["data"])
                    logger.info("[SYNC] Local database is now in sync.")
                    if elect_after:
                        from app.election import start_election
                        start_election()
                else:
                    logger.warning(f"[SYNC] Invalid response from leader.")
            else:
                logger.warning(f"[SYNC] Leader returned {response.status_code}")
        else:
            logger.warning(f"[SYNC] Could not fetch summary from leader.")
    except Exception as e:
        logger.error(f"[SYNC] Sync failed: {e}")
        state.synced_once = False # Retry later

def start_heartbeat_threads():
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=monitor_leader, daemon=True).start()
    logger.debug("[HEARTBEAT] Heartbeat threads started.")
