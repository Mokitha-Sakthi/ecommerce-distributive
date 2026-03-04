import requests
from app.config import PEERS, logger
from app.state import state
from app.aurora_db import save_order

def replicate_order(order):
    """Quorum-based replication logic (Leader only)."""
    if not state.is_leader:
        logger.error("[REPLICATION] Only leader can initiate replication.")
        return False

    logger.info(f"[REPLICATION] Starting replication for order {order.get('id')}")
    
    ack_count = 1  # Counting itself (Leader)
    total_nodes = len(PEERS) + 1
    quorum_size = (total_nodes // 2) + 1

    logger.debug(f"[REPLICATION] Quorum size required: {quorum_size}")

    for node_id, url in PEERS.items():
        try:
            logger.debug(f"[REPLICATION] Sending order to Node {node_id}")
            response = requests.post(f"{url}/receive_replication", json=order, timeout=2)
            if response.status_code == 200:
                ack_count += 1
                logger.debug(f"[REPLICATION] Node {node_id} ACKed. Total ACKs: {ack_count}")
        except Exception:
            logger.debug(f"[REPLICATION] Node {node_id} is currently unreachable.")

    if ack_count >= quorum_size:
        logger.info(f"[REPLICATION] Quorum reached ({ack_count}/{total_nodes}). Committing to Aurora.")
        if save_order(order):
            logger.info(f"[REPLICATION] Order {order.get('id')} successfully committed.")
            return True
        else:
            logger.error(f"[REPLICATION] Committing to Aurora failed!")
            return False
    else:
        logger.error(f"[REPLICATION] Quorum NOT reached ({ack_count}/{total_nodes}). Aborting.")
        return False
