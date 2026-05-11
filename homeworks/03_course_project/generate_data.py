import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


countries = ["RU", "KZ", "AM", "GE", "RS", "TR", "PL", "DE"]
categories = ["electronics", "books", "home", "beauty", "fashion", "sports", "grocery"]
brands = ["Aster", "Boreal", "Cobalt", "Delta", "Evergreen"]
channels = ["organic", "ads", "referral", "partner", "email"]
payments = ["card", "sbp", "cash", "gift_card"]
statuses = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]


def random_dates(start, end, n):
    seconds = int((end - start).total_seconds())
    return [start + timedelta(seconds=int(x)) for x in np.random.randint(0, seconds, size=n)]


def write_csv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(path, len(df))


def write_jsonl(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(path, len(rows))


def make_users(n):
    return pd.DataFrame(
        {
            "user_id": range(1, n + 1),
            "country": np.random.choice(countries, size=n),
            "signup_date": [x.date() for x in random_dates(datetime(2024, 1, 1), datetime(2025, 12, 31), n)],
            "marketing_channel": np.random.choice(channels, size=n),
        }
    )


def make_products(n):
    df = pd.DataFrame(
        {
            "product_id": range(1, n + 1),
            "category": np.random.choice(categories, size=n),
            "brand": np.random.choice(brands, size=n),
            "price": np.round(np.random.uniform(5, 900, size=n), 2),
            "is_active": np.random.choice([True, False], size=n, p=[0.93, 0.07]),
        }
    )

    bad_idx = np.random.choice(df.index, size=max(1, n // 50), replace=False)
    df.loc[bad_idx, "category"] = None
    return df


def make_orders(n, users_n):
    df = pd.DataFrame(
        {
            "order_id": [f"ORD-{i:07d}" for i in range(1, n + 1)],
            "user_id": np.random.randint(1, users_n + 1, size=n),
            "order_ts": random_dates(datetime(2026, 3, 1), datetime(2026, 3, 31, 23, 59, 59), n),
            "status": np.random.choice(statuses, size=n, p=[0.10, 0.36, 0.20, 0.22, 0.08, 0.04]),
            "payment_method": np.random.choice(payments, size=n),
        }
    )

    null_idx = np.random.choice(df.index, size=max(1, n // 100), replace=False)
    df.loc[null_idx, "user_id"] = None

    dup = df.sample(max(1, n // 70), random_state=42).copy()
    dup["order_ts"] = pd.to_datetime(dup["order_ts"]) + pd.to_timedelta(
        np.random.randint(60, 3600, size=len(dup)), unit="s"
    )

    df = pd.concat([df, dup], ignore_index=True)
    return df.sample(frac=1, random_state=42).reset_index(drop=True)


def make_order_items(orders, products_n):
    rows = []

    for order_id in orders["order_id"].drop_duplicates():
        for _ in range(random.randint(1, 5)):
            rows.append(
                {
                    "order_id": order_id,
                    "product_id": random.randint(1, products_n),
                    "quantity": random.randint(1, 4),
                }
            )

    df = pd.DataFrame(rows)

    null_idx = np.random.choice(df.index, size=max(1, len(df) // 100), replace=False)
    df.loc[null_idx, "product_id"] = None

    orphan_idx = np.random.choice(df.index, size=max(1, len(df) // 100), replace=False)
    df.loc[orphan_idx, "product_id"] = np.random.randint(products_n + 1, products_n + 200, size=len(orphan_idx))

    negative_idx = np.random.choice(df.index, size=max(1, len(df) // 120), replace=False)
    df.loc[negative_idx, "quantity"] *= -1

    return df.sample(frac=1, random_state=42).reset_index(drop=True)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(event_id, order_id, op, event_ts, ingest_ts, before, after):
    return {
        "event_id": event_id,
        "entity": "orders",
        "entity_id": order_id,
        "op": op,
        "event_ts": iso(event_ts),
        "ingest_ts": iso(ingest_ts),
        "before": before,
        "after": after,
    }


def make_cdc(orders):
    orders = (
        orders.dropna(subset=["user_id"])
        .drop_duplicates("order_id")
        .sample(min(12, len(orders)), random_state=42)
        .sort_values("order_ts")
        .reset_index(drop=True)
    )

    batches = [[], [], [], []]
    base_ingest = datetime(2026, 4, 1, 9, 0, 0)
    event_num = 1

    for i, row in orders.iterrows():
        order_id = row["order_id"]
        created_ts = pd.to_datetime(row["order_ts"]).to_pydatetime()
        paid_ts = created_ts + timedelta(minutes=random.randint(2, 40))
        shipped_ts = paid_ts + timedelta(hours=random.randint(2, 24))

        created = {
            "order_id": order_id,
            "user_id": int(row["user_id"]),
            "status": "created",
            "payment_method": row["payment_method"],
        }
        paid = dict(created)
        paid["status"] = "paid"
        shipped = dict(created)
        shipped["status"] = "shipped"

        batches[0].append(
            make_event(
                f"EVT-{event_num:06d}",
                order_id,
                "create",
                created_ts,
                base_ingest + timedelta(seconds=i * 10),
                None,
                created,
            )
        )
        event_num += 1

        batches[1].append(
            make_event(
                f"EVT-{event_num:06d}",
                order_id,
                "update",
                paid_ts,
                base_ingest + timedelta(minutes=5, seconds=i * 10),
                created,
                paid,
            )
        )
        event_num += 1

        if i % 4 == 0:
            batches[2].append(
                make_event(
                    f"EVT-{event_num:06d}",
                    order_id,
                    "delete",
                    shipped_ts,
                    base_ingest + timedelta(minutes=10, seconds=i * 10),
                    paid,
                    None,
                )
            )
        else:
            batches[2].append(
                make_event(
                    f"EVT-{event_num:06d}",
                    order_id,
                    "update",
                    shipped_ts,
                    base_ingest + timedelta(minutes=10, seconds=i * 10),
                    paid,
                    shipped,
                )
            )
        event_num += 1

    if batches[1]:
        batches[2].append(dict(batches[1][0]))

    if len(orders) > 1:
        row = orders.iloc[1]
        order_id = row["order_id"]
        late_ts = pd.to_datetime(row["order_ts"]).to_pydatetime() + timedelta(minutes=1)
        before = {
            "order_id": order_id,
            "user_id": int(row["user_id"]),
            "status": "created",
            "payment_method": row["payment_method"],
        }
        after = dict(before)
        after["status"] = "payment_retry"

        batches[3].append(
            make_event(
                f"EVT-{event_num:06d}",
                order_id,
                "update",
                late_ts,
                base_ingest + timedelta(hours=3),
                before,
                after,
            )
        )

    return batches


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--variant", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--products", type=int, default=600)
    parser.add_argument("--orders", type=int, default=8000)
    args = parser.parse_args()

    seed = args.seed
    if seed is None:
        seed = 1100 + args.variant if args.variant is not None else 42

    random.seed(seed)
    np.random.seed(seed)

    users = make_users(args.users)
    products = make_products(args.products)
    orders = make_orders(args.orders, args.users)
    order_items = make_order_items(orders, args.products)
    cdc_batches = make_cdc(orders)

    write_csv(users, args.output_dir / "users.csv")
    write_csv(products, args.output_dir / "products.csv")
    write_csv(orders, args.output_dir / "orders.csv")
    write_csv(order_items, args.output_dir / "order_items.csv")

    names = [
        "batch_001_create.jsonl",
        "batch_002_update.jsonl",
        "batch_003_update_delete_duplicate.jsonl",
        "batch_004_late_event.jsonl",
    ]
    for name, rows in zip(names, cdc_batches):
        write_jsonl(rows, args.output_dir / "cdc_orders" / name)

    events = [event for batch in cdc_batches for event in batch]
    event_ids = [event["event_id"] for event in events]

    print()
    print("summary")
    print("seed:", seed)
    print("users:", len(users))
    print("products:", len(products))
    print("orders:", len(orders))
    print("order_items:", len(order_items))
    print("cdc_events:", len(events))
    print("duplicate_orders:", orders["order_id"].duplicated().sum())
    print("orders_null_user_id:", orders["user_id"].isna().sum())
    print("products_null_category:", products["category"].isna().sum())
    print("items_null_product_id:", order_items["product_id"].isna().sum())
    print("items_negative_quantity:", (order_items["quantity"] <= 0).sum())
    print("duplicate_event_id:", len(event_ids) - len(set(event_ids)))


if __name__ == "__main__":
    main()
