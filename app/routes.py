from fastapi import FastAPI, Request, HTTPException
from app.state import state
from app.config import NODE_ID, PEERS, logger
from app.election import receive_election, receive_leader_announce
from app.replication import replicate_order
import requests
import time

app = FastAPI(title=f"Node {NODE_ID} API")

@app.post("/place_order")
async def place_order(order: dict):
    logger.info(f"[API] Received /place_order request: {order}")
    
    if not state.leader_id:
        return {"status": "error", "message": "No leader elected yet."}

    if not state.is_leader:
        # Forward to leader
        leader_url = PEERS.get(state.leader_id)
        logger.debug(f"[API] Not leader. Forwarding to Node {state.leader_id} at {leader_url}")
        try:
            response = requests.post(f"{leader_url}/place_order", json=order, timeout=5)
            return response.json()
        except Exception:
            return {"status": "error", "message": "Leader node is unreachable."}

    # If leader, replicate
    success = replicate_order(order)
    if success:
        return {"status": "success", "order_id": order.get("id"), "handled_by": NODE_ID}
    else:
        raise HTTPException(status_code=500, detail="Failed to replicate order.")

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

@app.post("/receive_replication")
async def receive_replication(order: dict):
    logger.debug(f"[API] Received replication request for order {order.get('id')}")
    state.order_buffer.append(order)
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
