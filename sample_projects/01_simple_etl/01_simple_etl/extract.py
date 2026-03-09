import pandas as pd
from pathlib import Path


def extract_csv(path: str) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(file_path)
    return df
