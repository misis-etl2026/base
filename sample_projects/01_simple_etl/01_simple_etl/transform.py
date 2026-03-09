import pandas as pd


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(subset=["amount"])
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df


def enrich_orders(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    df = orders.merge(users, on="user_id", how="left")
    if df["name"].isna().any():
        print("Warning: Some users not found!")
    return df
