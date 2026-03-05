import requests
from app.config import PEERS, logger
from app.state import state
from app.aurora_db import save_order, update_inventory

def replicate_order(order):
    """Two-phase quorum-based replication logic (Leader only)."""
    if not state.is_leader:
        logger.error("[REPLICATION] Only leader can initiate replication.")
        return False

    order_id = order.get('id')
    logger.info(f"[REPLICATION] Starting replication for order {order_id}")

    # ---- PHASE 1: PREPARE ----
    logger.info(f"[REPLICATION] Phase 1 (PREPARE): Sending order {order_id} to followers.")

    ack_count = 1  # Counting itself (Leader)
    total_nodes = len(PEERS) + 1
    quorum_size = (total_nodes // 2) + 1

    logger.debug(f"[REPLICATION] Quorum size required: {quorum_size}")

    for node_id, url in PEERS.items():
        try:
            logger.debug(f"[REPLICATION] Sending PREPARE to Node {node_id}")
            response = requests.post(f"{url}/prepare_replication", json=order, timeout=2)
            if response.status_code == 200:
                ack_count += 1
                logger.debug(f"[REPLICATION] Node {node_id} ACKed PREPARE. Total ACKs: {ack_count}")
        except Exception:
            logger.debug(f"[REPLICATION] Node {node_id} is currently unreachable.")

    # ---- PHASE 2: COMMIT or ABORT ----
    if ack_count >= quorum_size:
        logger.info(f"[REPLICATION] Quorum reached ({ack_count}/{total_nodes}). Phase 2 (COMMIT).")

        # Commit on leader first
        product_id = order.get("item", "item1").strip()
        requested_qty = order.get("quantity", 1)
        
        # 1. Update Inventory locally on Leader
        logger.info(f"[DB] Leader updating inventory for '{product_id}' (-{requested_qty})...")
        update_inventory(product_id, requested_qty)
        
        # 2. Save Order locally on Leader
        logger.info(f"[DB] Leader saving order details for {order_id}...")
        if save_order(order):
            logger.info(f"[REPLICATION] Order {order_id} committed on leader.")
        else:
            logger.error(f"[REPLICATION] Leader failed to commit order {order_id} to Aurora!")
            _send_abort(order_id)
            return False

        # Send COMMIT to all followers
        for node_id, url in PEERS.items():
            try:
                logger.debug(f"[REPLICATION] Sending COMMIT to Node {node_id}")
                requests.post(f"{url}/commit_order", json={"order_id": order_id}, timeout=2)
            except Exception:
                logger.debug(f"[REPLICATION] Could not send COMMIT to Node {node_id}")

        logger.info(f"[REPLICATION] Order {order_id} successfully committed and replicated.")
        return True
    else:
        logger.error(f"[REPLICATION] Quorum NOT reached ({ack_count}/{total_nodes}). Phase 2 (ABORT).")
        _send_abort(order_id)
        return False


def _send_abort(order_id):
    """Send ABORT to all followers to discard the pending order."""
    for node_id, url in PEERS.items():
        try:
            logger.debug(f"[REPLICATION] Sending ABORT to Node {node_id}")
            requests.post(f"{url}/abort_order", json={"order_id": order_id}, timeout=2)
        except Exception:
            logger.debug(f"[REPLICATION] Could not send ABORT to Node {node_id}")