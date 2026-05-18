# include/data_gen.py

"""
data_gen.py — Deterministic synthetic order generator.
Same date + same seed = same orders every time (safe for backfill).
"""

import random
import uuid
from datetime import date

CATEGORIES = ['Electronics', 'Clothing', 'Groceries']

PRICE_RANGE = {
    'Electronics': (500, 15000),
    'Clothing': (200, 3000),
    'Groceries': (50, 800),
}


def generate_orders(for_date: date, n_orders: int = 120) -> list[dict]:
    """
    Return a list of order dicts for for_date.
    Seed = YYYYMMDD integer — deterministic across backfills.
    """

    seed = int(for_date.strftime('%Y%m%d'))
    rng = random.Random(seed)

    orders = []

    for _ in range(n_orders):

        cat = rng.choice(CATEGORIES)
        lo, hi = PRICE_RANGE[cat]

        orders.append({
            'order_id': str(uuid.UUID(int=rng.getrandbits(128))),
            'order_date': for_date.isoformat(),
            'category': cat,
            'amount': round(rng.uniform(lo, hi), 2),
            'customer_id': f'CUST{rng.randint(1000, 9999)}',
        })

    return orders