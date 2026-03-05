from fastapi import FastAPI, Request, HTTPException
from app.state import state
from app.config import NODE_ID, PEERS, logger
from app.election import receive_election, receive_leader_announce
from app.replication import replicate_order
from app.aurora_db import save_order, get_inventory, update_inventory, initialize_inventory, get_all_data, overwrite_local_data
from app.snapshot import initiate_snapshot, receive_marker
import requests
import time

app = FastAPI(title=f"Node {NODE_ID} API")

@app.post("/initialize_inventory")
async def init_inventory_endpoint():
    initialize_inventory()
    return {"status": "success", "message": "Inventory reset with code-based values."}

@app.post("/place_order")
async def place_order(order: dict):
    product_id = order.get("item", "item1")
    requested_qty = order.get("quantity", 1)
    
    if not state.leader_id:
        return {"status": "error", "message": "No leader elected yet."}

    if not state.is_leader:
        leader_url = PEERS.get(state.leader_id)
        if not leader_url:
            logger.error(f"[PROXY] Leader ID {state.leader_id} found but its URL is missing in PEERS.")
            return {"status": "error", "message": f"Leader URL not found for ID {state.leader_id}"}
            
        try:
            logger.info(f"[PROXY] Forwarding order to Leader {state.leader_id} at {leader_url}")
            return requests.post(f"{leader_url}/place_order", json=order, timeout=5).json()
        except Exception as e:
            logger.error(f"[PROXY] Connection to Leader {state.leader_id} at {leader_url} failed: {e}")
            return {"status": "error", "message": "Leader node is unreachable."}

    # 1. Acquire Lock (Centralized on leader)
    if product_id in state.locks:
        logger.info(f"[LOCK] Order for '{product_id}' denied - Lock already held by Node {state.locks[product_id]}.")
        return {"status": "error", "message": f"Lock for {product_id} busy."}
    
    state.locks[product_id] = NODE_ID
    logger.info(f"[LOCK] Node {NODE_ID} acquired lock for '{product_id}'.")
    try:
        # 2. Check Inventory (Fetch from DB)
        available_qty, db_status = get_inventory(product_id)
        if db_status != "OK":
            logger.error(f"[ORDER] Inventory check failed for '{product_id}': {db_status}")
            return {"status": "error", "message": f"Inventory check failed: {db_status}"}
            
        if available_qty < requested_qty:
            logger.info(f"[ORDER] Rejected: Insufficient stock for '{product_id}' (Requested: {requested_qty}, Available: {available_qty}).")
            return {"status": "error", "message": f"Insufficient stock for {product_id}."}

        # 3. Replicate
        logger.info(f"[ORDER] Inventory check passed. Starting replication for '{product_id}'...")
        success = replicate_order(order)
        if success:
            return {"status": "success", "order_id": order.get("id"), "handled_by": NODE_ID}
        return {"status": "error", "message": "Failed to replicate order."}
    finally:
        # 4. Release Lock
        state.locks.pop(product_id, None)
        logger.info(f"[LOCK] Node {NODE_ID} released lock for '{product_id}'.")

@app.post("/heartbeat")
async def heartbeat(data: dict):
    leader_id = data.get("leader_id")
    state.last_heartbeat = time.time()
    state.leader_id = leader_id
    # logger.debug(f"[API] Heartbeat received from Leader {leader_id}")
    return {"status": "ok"}

@app.post("/start_election")
async def start_election_endpoint():
    # In a real scenario, we'd pass the caller_id
    receive_election(caller_id="peer")
    return {"status": "ack"}

@app.post("/leader_announce")
async def leader_announce(data: dict):
    receive_leader_announce(data.get("leader_id"))
    return {"status": "ack"}

@app.post("/acquire_lock")
async def acquire_lock(data: dict):
    product_id = data.get("product_id")
    node_id = data.get("node_id")
    
    if not state.is_leader:
        leader_url = PEERS.get(state.leader_id)
        if not leader_url: return {"status": "error"}
        return requests.post(f"{leader_url}/acquire_lock", json=data).json()

    if product_id in state.locks:
        return {"status": "busy", "owner": state.locks[product_id]}
    
    state.locks[product_id] = node_id
    return {"status": "granted"}

@app.post("/release_lock")
async def release_lock(data: dict):
    product_id = data.get("product_id")
    node_id = data.get("node_id")

    if not state.is_leader:
        leader_url = PEERS.get(state.leader_id)
        if not leader_url: return {"status": "error"}
        return requests.post(f"{leader_url}/release_lock", json=data).json()

    if state.locks.get(product_id) == node_id:
        state.locks.pop(product_id, None)
        return {"status": "released"}
    return {"status": "error", "message": "Not lock owner"}

@app.post("/prepare_replication")
async def prepare_replication(order: dict):
    """Phase 1: Buffer the order in memory, ACK back to leader."""
    order_id = order.get('id')
    logger.info(f"[PREPARE] Buffering order {order_id} in pending_orders.")
    state.pending_orders[order_id] = order
    return {"status": "ack"}

@app.post("/commit_order")
async def commit_order(data: dict):
    """Phase 2 (Commit): Write the pending order and update inventory in MySQL."""
    order_id = data.get("order_id")
    order = state.pending_orders.pop(order_id, None)
    if order:
        product_id = order.get("item", "item1")
        requested_qty = order.get("quantity", 1)
        
        # 1. Update Inventory Table in MySQL
        logger.info(f"[DB] Follower updating inventory for '{product_id}' (-{requested_qty})...")
        db_success = update_inventory(product_id, requested_qty)
        
        # 2. Save Order to MySQL
        logger.info(f"[DB] Follower saving order details for {order_id}...")
        save_order(order)
        
        state.order_buffer.append(order)
        logger.info(f"[COMMIT] Order {order_id} logic completed. DB Success: {db_success}")
    else:
        logger.warning(f"[COMMIT] Order {order_id} not found.")
    return {"status": "ack"}

@app.post("/abort_order")
async def abort_order(data: dict):
    """Phase 2 (Abort): Discard the pending order."""
    order_id = data.get("order_id")
    discarded = state.pending_orders.pop(order_id, None)
    if discarded:
        logger.info(f"[ABORT] Order {order_id} discarded from pending_orders.")
    else:
        logger.warning(f"[ABORT] Order {order_id} not found in pending_orders (already discarded?).")
    return {"status": "ack"}


# ---- Chandy-Lamport Snapshot Endpoints ----

@app.post("/initiate_snapshot")
async def initiate_snapshot_endpoint():
    """Leader initiates a global snapshot."""
    if not state.is_leader:
        return {"status": "error", "message": "Only leader can initiate snapshot."}
    result = initiate_snapshot()
    if result:
        return {"status": "success", "snapshot": result}
    return {"status": "error", "message": "Snapshot failed."}

@app.post("/receive_marker")
async def receive_marker_endpoint(data: dict):
    """Follower receives a marker, records and returns local state."""
    snapshot_id = data.get("snapshot_id")
    sender_id = data.get("sender_id")
    local_state = receive_marker(snapshot_id, sender_id)
    return {"status": "ack", "local_state": local_state}

@app.get("/snapshot")
async def get_snapshot():
    """View the latest global snapshot (leader) or local snapshot (follower)."""
    if state.is_leader and state.snapshot_data:
        return {"role": "leader", "global_snapshot": state.snapshot_data}
    elif state.last_snapshot:
        return {"role": "follower", "local_snapshot": state.last_snapshot}
    return {"message": "No snapshot available yet."}

@app.get("/get_all_data")
async def get_all_data_endpoint():
    """Returns all DB data for syncing followers."""
    if not state.is_leader:
        return {"status": "error", "message": "Only leader provides sync data."}
    data = get_all_data()
    return {"status": "success", "data": data}

@app.get("/status")
async def get_status():
    return {
        "node_id": NODE_ID,
        "is_leader": state.is_leader,
        "leader_id": state.leader_id,
        "orders_count": len(state.order_buffer),
        "election_in_progress": state.election_in_progress
    }