"""Generate raw data for the PySpark homework.

Usage:
    ./ .venv/bin/python homeworks/02_pyspark/generate_data.py
    ./ .venv/bin/python homeworks/02_pyspark/generate_data.py \
        --output-dir homeworks/02_pyspark/data/raw \
        --users 5000 --products 1500 --orders 25000 --seed 42

The script generates four CSV files:
    - users.csv
    - products.csv
    - orders.csv
    - order_items.csv

The data intentionally contains a small amount of "dirt":
    - duplicate orders by order_id
    - null user_id in orders
    - null product_id in order_items
    - negative quantity in order_items
    - orphan product_id in order_items
    - products with null category
    - cancelled orders
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import random

import numpy as np
import pandas as pd


COUNTRIES = ["RU", "DE", "GE", "RS", "AM", "KZ", "UZ", "PL"]
CATEGORIES = [
    "electronics",
    "books",
    "home",
    "beauty",
    "fashion",
    "sports",
    "toys",
    "grocery",
]
ORDER_STATUSES = ["completed", "completed", "completed", "pending", "shipped", "cancelled"]


@dataclass
class GeneratorConfig:
    output_dir: Path
    users: int = 5_000
    products: int = 1_500
    orders: int = 25_000
    max_items_per_order: int = 4
    duplicate_order_ratio: float = 0.02
    null_user_ratio: float = 0.01
    null_product_ratio: float = 0.01
    negative_quantity_ratio: float = 0.01
    orphan_product_ratio: float = 0.01
    null_category_ratio: float = 0.02
    orders_without_items_ratio: float = 0.03
    seed: int = 42


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate raw data for the PySpark homework.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("homeworks/02_pyspark/data/raw"),
        help="Directory for generated CSV files.",
    )
    parser.add_argument("--users", type=int, default=5_000, help="Number of users.")
    parser.add_argument("--products", type=int, default=1_500, help="Number of products.")
    parser.add_argument("--orders", type=int, default=25_000, help="Number of orders before duplicates.")
    parser.add_argument(
        "--max-items-per-order",
        type=int,
        default=4,
        help="Maximum number of items per order.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser


def random_dates(rng: np.random.Generator, start: datetime, end: datetime, size: int) -> pd.Series:
    total_seconds = int((end - start).total_seconds())
    offsets = rng.integers(0, total_seconds, size=size)
    return pd.to_datetime([start + timedelta(seconds=int(offset)) for offset in offsets])


def generate_users(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    signup_start = datetime(2024, 1, 1)
    signup_end = datetime(2025, 12, 31, 23, 59, 59)
    users = pd.DataFrame(
        {
            "user_id": np.arange(1, cfg.users + 1, dtype=int),
            "country": rng.choice(COUNTRIES, size=cfg.users),
            "signup_date": random_dates(rng, signup_start, signup_end, cfg.users).date,
        }
    )
    return users


def generate_products(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    products = pd.DataFrame(
        {
            "product_id": np.arange(1, cfg.products + 1, dtype=int),
            "category": rng.choice(CATEGORIES, size=cfg.products),
            "price": np.round(rng.uniform(5, 700, size=cfg.products), 2),
        }
    )

    null_category_count = max(1, int(cfg.products * cfg.null_category_ratio))
    null_idx = rng.choice(products.index, size=null_category_count, replace=False)
    products.loc[null_idx, "category"] = None
    return products


def generate_orders(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    order_start = datetime(2026, 1, 1)
    order_end = datetime(2026, 1, 31, 23, 59, 59)

    orders = pd.DataFrame(
        {
            "order_id": [f"ORD-{i:07d}" for i in range(1, cfg.orders + 1)],
            "user_id": rng.integers(1, cfg.users + 1, size=cfg.orders),
            "order_ts": random_dates(rng, order_start, order_end, cfg.orders),
            "status": rng.choice(ORDER_STATUSES, size=cfg.orders),
        }
    )

    null_user_count = max(1, int(cfg.orders * cfg.null_user_ratio))
    null_idx = rng.choice(orders.index, size=null_user_count, replace=False)
    orders.loc[null_idx, "user_id"] = None

    duplicate_count = max(1, int(cfg.orders * cfg.duplicate_order_ratio))
    duplicates = orders.sample(n=duplicate_count, random_state=cfg.seed).copy()
    duplicates["order_ts"] = duplicates["order_ts"] + pd.to_timedelta(
        rng.integers(1, 1800, size=duplicate_count), unit="s"
    )

    orders = pd.concat([orders, duplicates], ignore_index=True)
    orders = orders.sample(frac=1, random_state=cfg.seed).reset_index(drop=True)
    return orders


def generate_order_items(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    base_orders = orders.drop_duplicates(subset=["order_id"]).copy()

    orders_without_items_count = max(1, int(len(base_orders) * cfg.orders_without_items_ratio))
    orders_without_items = set(
        rng.choice(base_orders["order_id"].to_numpy(), size=orders_without_items_count, replace=False)
    )

    rows: list[dict[str, object]] = []
    product_ids = products["product_id"].to_numpy()

    for order_id in base_orders["order_id"]:
        if order_id in orders_without_items:
            continue

        n_items = int(rng.integers(1, cfg.max_items_per_order + 1))
        chosen_products = rng.choice(product_ids, size=n_items, replace=True)
        for product_id in chosen_products:
            rows.append(
                {
                    "order_id": order_id,
                    "product_id": int(product_id),
                    "quantity": int(rng.integers(1, 5)),
                }
            )

    order_items = pd.DataFrame(rows)

    if order_items.empty:
        raise RuntimeError("No order_items were generated. Increase the number of orders.")

    null_product_count = max(1, int(len(order_items) * cfg.null_product_ratio))
    null_product_idx = rng.choice(order_items.index, size=null_product_count, replace=False)
    order_items.loc[null_product_idx, "product_id"] = None

    negative_quantity_count = max(1, int(len(order_items) * cfg.negative_quantity_ratio))
    negative_idx = rng.choice(order_items.index, size=negative_quantity_count, replace=False)
    order_items.loc[negative_idx, "quantity"] *= -1

    orphan_product_count = max(1, int(len(order_items) * cfg.orphan_product_ratio))
    orphan_idx = rng.choice(order_items.index, size=orphan_product_count, replace=False)
    order_items.loc[orphan_idx, "product_id"] = rng.integers(
        cfg.products + 1,
        cfg.products + 500,
        size=orphan_product_count,
    )

    order_items = order_items.sample(frac=1, random_state=cfg.seed).reset_index(drop=True)
    return order_items


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Generated {path} with {len(df)} rows")


def print_summary(users: pd.DataFrame, products: pd.DataFrame, orders: pd.DataFrame, order_items: pd.DataFrame) -> None:
    print("\nSummary:")
    print(f"  users: {len(users)}")
    print(f"  products: {len(products)}")
    print(f"  orders: {len(orders)}")
    print(f"  order_items: {len(order_items)}")
    print(f"  duplicate orders by order_id: {orders['order_id'].duplicated().sum()}")
    print(f"  orders with null user_id: {orders['user_id'].isna().sum()}")
    print(f"  products with null category: {products['category'].isna().sum()}")
    print(f"  order_items with null product_id: {order_items['product_id'].isna().sum()}")
    print(f"  order_items with negative quantity: {(order_items['quantity'] <= 0).sum()}")


def main() -> None:
    args = build_parser().parse_args()
    cfg = GeneratorConfig(
        output_dir=args.output_dir,
        users=args.users,
        products=args.products,
        orders=args.orders,
        max_items_per_order=args.max_items_per_order,
        seed=args.seed,
    )

    random.seed(cfg.seed)
    rng = np.random.default_rng(cfg.seed)

    users = generate_users(cfg, rng)
    products = generate_products(cfg, rng)
    orders = generate_orders(cfg, rng)
    order_items = generate_order_items(cfg, rng, orders, products)

    save_csv(users, cfg.output_dir / "users.csv")
    save_csv(products, cfg.output_dir / "products.csv")
    save_csv(orders, cfg.output_dir / "orders.csv")
    save_csv(order_items, cfg.output_dir / "order_items.csv")
    print_summary(users, products, orders, order_items)


if __name__ == "__main__":
    main()
