import uvicorn
from app.config import PORT, HOST, logger
from app.heartbeat import start_heartbeat_threads

if __name__ == "__main__":
    logger.info(f"--- Node starting up ---")
    start_heartbeat_threads()
    uvicorn.run("app.routes:app", host=HOST, port=PORT, log_level="warning")
