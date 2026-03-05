from fastapi import FastAPI, Request, HTTPException
from app.state import state
from app.config import NODE_ID, PEERS, logger
from app.election import receive_election, receive_leader_announce
from app.replication import replicate_order
from app.aurora_db import save_order, get_inventory, update_inventory, initialize_inventory
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
        try:
            return requests.post(f"{leader_url}/place_order", json=order, timeout=5).json()
        except Exception:
            return {"status": "error", "message": "Leader node is unreachable."}

    # 1. Acquire Lock (Centralized on leader)
    if product_id in state.locks:
        return {"status": "error", "message": f"Lock for {product_id} busy."}
    
    state.locks[product_id] = NODE_ID
    try:
        # 2. Check Inventory (Fetch from DB)
        available_qty, db_status = get_inventory(product_id)
        if db_status != "OK":
            return {"status": "error", "message": f"Inventory check failed: {db_status}"}
            
        if available_qty < requested_qty:
            return {"status": "error", "message": f"Insufficient stock for {product_id}."}

        # 3. Replicate
        success = replicate_order(order)
        if success:
            return {"status": "success", "order_id": order.get("id"), "handled_by": NODE_ID}
        return {"status": "error", "message": "Failed to replicate order."}
    finally:
        # 4. Release Lock
        state.locks.pop(product_id, None)

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
        db_success = update_inventory(product_id, requested_qty)
        
        # 2. Save Order to MySQL
        save_order(order)
        
        state.order_buffer.append(order)
        logger.info(f"[COMMIT] Order {order_id} committed. DB Update: {db_success}")
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


@app.get("/status")
async def get_status():
    return {
        "node_id": NODE_ID,
        "is_leader": state.is_leader,
        "leader_id": state.leader_id,
        "orders_count": len(state.order_buffer),
        "election_in_progress": state.election_in_progress
    }