import os
import random
import time
import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("workload-generator")

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "shop")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")

# Rate control — deliberately conservative. One action every MIN..MAX seconds,
# not a tight loop. Keeps total data volume small and lets you actually watch
# individual events flow through Kafka UI / consumers as they happen.
MIN_SLEEP_SECONDS = float(os.environ.get("MIN_SLEEP_SECONDS", "3"))
MAX_SLEEP_SECONDS = float(os.environ.get("MAX_SLEEP_SECONDS", "8"))

FIRST_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
LAST_NAMES = ["Smith", "Jones", "White", "Miller", "Brown", "Davis", "Garcia", "Wilson"]
PRODUCTS = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig", "Sprocket"]
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]


def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    return conn


def random_existing_id(cur, table):
    """NOTE: ORDER BY random() is O(n log n) — fine at this scale, not how
    you'd do this against a large production table."""
    cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def create_customer(cur):
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    email = f"{name.lower().replace(' ', '.')}{random.randint(1, 9999)}@example.com"
    cur.execute(
        "INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING id",
        (name, email),
    )
    log.info(f"created customer id={cur.fetchone()[0]} name={name}")


def create_order(cur):
    customer_id = random_existing_id(cur, "customers")
    if customer_id is None:
        log.info("no customers yet, skipping order creation")
        return
    cur.execute(
        "INSERT INTO orders (customer_id, status) VALUES (%s, %s) RETURNING id",
        (customer_id, "pending"),
    )
    log.info(f"created order id={cur.fetchone()[0]} customer_id={customer_id}")


def create_order_item(cur):
    order_id = random_existing_id(cur, "orders")
    if order_id is None:
        log.info("no orders yet, skipping order_item creation")
        return
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    price = round(random.uniform(4.99, 99.99), 2)
    cur.execute(
        "INSERT INTO order_items (order_id, product_name, quantity, unit_price) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (order_id, product, qty, price),
    )
    log.info(f"created order_item id={cur.fetchone()[0]} order_id={order_id} product={product}")


def update_order_status(cur):
    order_id = random_existing_id(cur, "orders")
    if order_id is None:
        return
    new_status = random.choice(ORDER_STATUSES)
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
        (new_status, order_id),
    )
    log.info(f"updated order id={order_id} -> status={new_status}")


ACTIONS = [
    (create_customer, 0.15),
    (create_order, 0.35),
    (create_order_item, 0.30),
    (update_order_status, 0.20),
]


def pick_action():
    r = random.random()
    cumulative = 0.0
    for action, weight in ACTIONS:
        cumulative += weight
        if r <= cumulative:
            return action
    return ACTIONS[-1][0]


def main():
    log.info(f"connecting to postgres at {DB_HOST}:{DB_PORT}/{DB_NAME}")
    conn = get_connection()
    cur = conn.cursor()
    log.info("connected. starting workload loop.")

    while True:
        action = pick_action()
        try:
            action(cur)
        except Exception:
            log.exception(f"action {action.__name__} failed, reconnecting")
            try:
                conn.close()
            except Exception:
                pass
            conn = get_connection()
            cur = conn.cursor()

        time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))


if __name__ == "__main__":
    main()