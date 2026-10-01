"""Build data/sample: a consistent slice of the real Olist dataset for tests and CI (the full dataset is not committed).

    python scripts/make_sample.py <path to the Olist CSV folder> [orders]

Takes the first N orders by purchase time and every row that belongs to them (items, payments, reviews, customers,
and the sellers and products those items reference), so foreign keys stay valid in the sample.
Olist data: Brazilian E-Commerce Public Dataset by Olist, CC BY-NC-SA 4.0 (kaggle.com/datasets/olistbr/brazilian-ecommerce).
"""
import sys
from pathlib import Path

import pandas as pd

OUT = Path(__file__).resolve().parents[1] / "data" / "sample"


def main(raw: Path, n: int = 2000) -> dict:
    read = lambda f: pd.read_csv(raw / f, dtype=str, keep_default_na=False)
    orders = read("olist_orders_dataset.csv").sort_values("order_purchase_timestamp").head(n)
    ids = set(orders.order_id)
    items = read("olist_order_items_dataset.csv").query("order_id in @ids")
    out = {
        "olist_orders_dataset.csv": orders,
        "olist_order_items_dataset.csv": items,
        "olist_order_payments_dataset.csv": read("olist_order_payments_dataset.csv").query("order_id in @ids"),
        "olist_order_reviews_dataset.csv": read("olist_order_reviews_dataset.csv").query("order_id in @ids"),
        "olist_customers_dataset.csv": read("olist_customers_dataset.csv").query("customer_id in @orders.customer_id"),
        "olist_sellers_dataset.csv": read("olist_sellers_dataset.csv").query("seller_id in @items.seller_id"),
        "olist_products_dataset.csv": read("olist_products_dataset.csv").query("product_id in @items.product_id"),
        "product_category_name_translation.csv": pd.read_csv(raw / "product_category_name_translation.csv", dtype=str, encoding="utf-8-sig"),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    for name, df in out.items():
        df.to_csv(OUT / name, index=False)
    return {k: len(v) for k, v in out.items()}


if __name__ == "__main__":
    print(main(Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 2000))
