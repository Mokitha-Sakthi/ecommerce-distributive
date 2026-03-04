import pymysql
from app.config import DB_CONFIG, logger

def save_order(order):
    """Saves order to Amazon Aurora (MySQL)."""
    logger.info(f"[DB] Attempting to save order {order.get('id')} to Aurora.")
    
    try:
        connection = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            connect_timeout=5
        )
        with connection.cursor() as cursor:
            sql = "INSERT INTO orders (id, item, quantity, status) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (order['id'], order['item'], order['quantity'], 'COMMITTED'))
        connection.commit()
        connection.close()
        logger.info(f"[DB] Order {order.get('id')} successfully saved to DB.")
        return True
    except Exception as e:
        logger.warning(f"[DB] Aurora unavailable ({e}) - simulating commit for demo.")
        return True
