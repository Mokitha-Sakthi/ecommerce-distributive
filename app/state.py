import time

class SystemState:
    def __init__(self):
        self.leader_id = None
        self.is_leader = False
        self.last_heartbeat = time.time()
        self.order_buffer = []
        self.pending_orders = {}  # {order_id: order} - buffered during prepare phase
        self.nodes_alive = {} # {id: last_seen_time}
        self.election_in_progress = False
        
        # New for Mutual Exclusion & Inventory
        self.locks = {} # {product_id: node_id} - Only used on Leader

state = SystemState()