# Distributed E-Commerce Order Processing System

A 4-node distributed system demonstrating Bully Leader Election, Heartbeat Failure Detection, and Quorum-based Replication. This is a case study to understand the distributed implementation of Amazon Aurora

## Architecture
- **Language**: Python (FastAPI)
- **Nodes**: 4 Laptops (or 4 local instances for testing)
- **Election**: Bully Algorithm
- **Replication**: Quorum (N/2 + 1)
- **Database**: MySQL - to replicate Amazon Aurora (Leader-only commits)

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure environment variables in `.env` (see `app/config.py` for variables).
3. Run the application:
   ```bash
   python main.py
   ```

## Demo Instructions
1. Start all 4 nodes.
2. Watch the logs for Leader Election.
3. Place an order via `POST /place_order` on any node.
4. Kill the leader node and observe the re-election process.
