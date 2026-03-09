# uvx --with pandas  python generate_data.py
import os
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def generate_transactions_for_date(
    date,
    n_rows = 1000,
    duplicate_ratio = 0.02,
    negative_ratio = 0.01,
    null_ratio = 0.01,
    output_dir = "data/raw/"
):
    os.makedirs(output_dir, exist_ok=True)
    transaction_ids = [f"{date.strftime('%Y%m%d')}_{i}" for i in range(n_rows)]
    user_ids = np.random.randint(1, 1000, size=n_rows)
    amounts = np.round(np.random.uniform(1, 1000, size=n_rows), 2)
    currencies = np.random.choice(["USD", "EUR", "GBP"], size=n_rows)
    event_times = [
        datetime(
            date.year,
            date.month,
            date.day,
            random.randint(0, 23),
            random.randint(0, 59),
            random.randint(0, 59),
        )
        for _ in range(n_rows)
    ]
    df = pd.DataFrame({
        "transaction_id": transaction_ids,
        "user_id": user_ids,
        "amount": amounts,
        "currency": currencies,
        "event_time": event_times,
    })
    n_duplicates = int(n_rows * duplicate_ratio)
    if n_duplicates > 0:
        duplicates = df.sample(n_duplicates, random_state=42)
        df = pd.concat([df, duplicates], ignore_index=True)
    n_negative = int(n_rows * negative_ratio)
    if n_negative > 0:
        negative_indices = np.random.choice(df.index, size=n_negative, replace=False)
        df.loc[negative_indices, "amount"] *= -1
    n_null = int(n_rows * null_ratio)
    if n_null > 0:
        null_indices = np.random.choice(df.index, size=n_null, replace=False)
        df.loc[null_indices, "transaction_id"] = None
    df = df.sample(frac=1).reset_index(drop=True)
    filename = f"transactions_{date.strftime('%Y-%m-%d')}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)

    print(f"Generated {filepath} with {len(df)} rows")


if __name__ == "__main__":
    n_rows_per_day = 1000
    current = datetime.now() - timedelta(weeks=4)
    while current <= datetime.now():
        generate_transactions_for_date(
            date=current,
            n_rows=n_rows_per_day
        )
        current += timedelta(days=1)
