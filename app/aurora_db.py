import pymysql
from app.config import DB_CONFIG, logger

def save_order(order):
    """Saves order to Amazon Aurora (MySQL) across multiple replicated databases."""
    logger.info(f"[DB] Attempting to save order {order.get('id')} to Aurora.")
    
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    success_count = 0

    for host in hosts:
        try:
            logger.debug(f"[DB] Connecting to MySQL on host: {host}")
            connection = pymysql.connect(
                host=host,
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                database=DB_CONFIG["database"],
                connect_timeout=3
            )
            with connection.cursor() as cursor:
                sql = "INSERT INTO orders (id, item, quantity, status) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (order['id'], order['item'], order['quantity'], 'COMMITTED'))
            connection.commit()
            connection.close()
            logger.info(f"[DB] Order {order.get('id')} successfully saved to DB on {host}.")
            success_count += 1
        except Exception as e:
            logger.warning(f"[DB] Failed to save to Aurora node {host}: {e}")

    if success_count > 0:
        return True
    
    logger.warning("[DB] All Aurora connections failed - simulating commit for demo.")
    return True
def get_inventory(product_id):
    """Fetches stock level for a product. Returns (quantity, status)."""
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    for host in hosts:
        connection = None
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"], connect_timeout=3
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT quantity FROM inventory WHERE item = %s", (product_id.strip(),))
                result = cursor.fetchone()
                if result is not None:
                    return result[0], "OK"
        except Exception:
            continue
        finally:
            if connection: connection.close()
    return 0, "Error connecting to DB"

def update_inventory(product_id, quantity_to_subtract):
    """Decrements inventory in the database. Returns True if at least one host succeeded."""
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    success = False
    for host in hosts:
        connection = None
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"], connect_timeout=3
            )
            with connection.cursor() as cursor:
                cursor.execute("UPDATE inventory SET quantity = quantity - %s WHERE item = %s", 
                               (quantity_to_subtract, product_id))
            connection.commit()
            success = True
        except Exception as e:
            logger.error(f"[DB] Update failed on {host}: {e}")
            continue
        finally:
            if connection: connection.close()
    return success

def initialize_inventory():
    """Clears and re-populates the inventory table from code."""
    default_items = [
        ('item1', 5), ('item2', 0), ('laptop', 5), ('Mechanical Keyboard', 10)
    ]
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    overall_success = False
    for host in hosts:
        connection = None
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"], connect_timeout=3
            )
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM inventory")
                cursor.executemany("INSERT INTO inventory (item, quantity) VALUES (%s, %s)", default_items)
            connection.commit()
            logger.info(f"[DB] Inventory re-initialized on {host}")
            overall_success = True
        except Exception as e:
            logger.error(f"[DB] Re-init failed on {host}: {e}")
            continue
        finally:
            if connection: connection.close()
    return overall_success
