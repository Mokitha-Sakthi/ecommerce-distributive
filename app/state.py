import time

class SystemState:
    def __init__(self):
        self.leader_id = None
        self.is_leader = False
        self.last_heartbeat = time.time()
        self.order_buffer = []
        self.nodes_alive = {} # {id: last_seen_time}
        self.election_in_progress = False

state = SystemState()
