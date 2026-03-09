from pathlib import Path
import pandas as pd


def load_csv(df: pd.DataFrame, output_path: str):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
