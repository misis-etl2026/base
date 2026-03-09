import logging
from extract import extract_csv
from transform import clean_orders, enrich_orders
from load import load_csv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    logging.info("Starting ETL pipeline")

    orders = extract_csv("data/raw/orders.csv")
    users = extract_csv("data/raw/users.csv")

    logging.info(f"Orders loaded: {len(orders)}")
    logging.info(f"Users loaded: {len(users)}")

    orders_clean = clean_orders(orders)
    logging.info(f"Orders after cleaning: {len(orders_clean)}")

    enriched = enrich_orders(orders_clean, users)

    load_csv(enriched, "data/processed/orders_enriched.csv")

    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()