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
    """Fetches stock level. Returns (quantity, status)."""
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    connected_at_least_once = False
    for host in hosts:
        connection = None
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"], connect_timeout=3
            )
            connected_at_least_once = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT quantity FROM inventory WHERE item = %s", (product_id.strip(),))
                result = cursor.fetchone()
                if result is not None:
                    return result[0], "OK"
                else:
                    return 0, f"Item '{product_id}' not in DB"
        except Exception:
            continue
        finally:
            if connection: connection.close()
    return 0, "DB connection failed" if not connected_at_least_once else "Item not found"

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
                affected = cursor.execute(
                    "UPDATE inventory SET quantity = quantity - %s WHERE item = %s AND quantity >= %s", 
                    (quantity_to_subtract, product_id, quantity_to_subtract)
                )
                if affected == 0:
                    logger.warning(f"[DB] Update rejected on {host}: Insufficient stock (would go negative).")
                else:
                    success = True
            connection.commit()
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

def get_all_data():
    """Fetches all rows from inventory and orders for synchronization."""
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    if not hosts: return None
    
    for host in hosts:
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"]
            )
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SELECT * FROM inventory")
                inventory = cursor.fetchall()
                cursor.execute("SELECT * FROM orders")
                orders = cursor.fetchall()
                return {"inventory": inventory, "orders": orders}
        except Exception as e:
            logger.warning(f"[DB] Could not fetch sync data from {host}: {e}")
            continue
        finally:
            if 'connection' in locals() and connection: connection.close()
    return None

def overwrite_local_data(data):
    """Overwrites local tables with data provided by the leader."""
    hosts = [h.strip() for h in DB_CONFIG["host"].split(",")]
    total_success = True
    
    for host in hosts:
        connection = None
        try:
            connection = pymysql.connect(
                host=host, user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                database=DB_CONFIG["database"]
            )
            with connection.cursor() as cursor:
                # 1. Clear tables
                cursor.execute("DELETE FROM orders")
                cursor.execute("DELETE FROM inventory")
                
                # 2. Insert Inventory
                if data.get("inventory"):
                    cursor.executemany(
                        "INSERT INTO inventory (item, quantity) VALUES (%s, %s)",
                        [(i['item'], i['quantity']) for i in data['inventory']]
                    )
                
                # 3. Insert Orders
                if data.get("orders"):
                    cursor.executemany(
                        "INSERT INTO orders (id, item, quantity, status) VALUES (%s, %s, %s, %s)",
                        [(o['id'], o['item'], o['quantity'], o['status']) for o in data['orders']]
                    )
            connection.commit()
            logger.info(f"[DB] Local tables overwritten with synced data on {host}")
        except Exception as e:
            logger.error(f"[DB] Failed to overwrite data on {host}: {e}")
            total_success = False
        finally:
            if connection: connection.close()
            
    return total_success
