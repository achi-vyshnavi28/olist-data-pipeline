"""Data contracts: the schema, keys, owner and sensitivity of every source table.

The bronze stage reads each file with the schema declared here (never inferred), so a changed upstream file fails
loudly instead of silently becoming strings. The same contract drives the quality checks and the governance catalog.
"""
SOURCES = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "schema": "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
                  "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
                  "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP",
        "key": ["order_id"],
        "owner": "commerce", "pii": [],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "schema": "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
                  "shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE",
        "key": ["order_id", "order_item_id"],
        "owner": "commerce", "pii": [],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "schema": "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
                  "customer_city STRING, customer_state STRING",
        "key": ["customer_id"],
        "owner": "crm", "pii": ["customer_zip_code_prefix", "customer_city"],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "schema": "seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING",
        "key": ["seller_id"],
        "owner": "marketplace", "pii": ["seller_zip_code_prefix"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "schema": "product_id STRING, product_category_name STRING, product_name_lenght INT, "
                  "product_description_lenght INT, product_photos_qty INT, product_weight_g DOUBLE, "
                  "product_length_cm DOUBLE, product_height_cm DOUBLE, product_width_cm DOUBLE",
        "key": ["product_id"],
        "owner": "catalog", "pii": [],
    },
    "payments": {
        "file": "olist_order_payments_dataset.csv",
        "schema": "order_id STRING, payment_sequential INT, payment_type STRING, payment_installments INT, "
                  "payment_value DOUBLE",
        "key": ["order_id", "payment_sequential"],
        "owner": "finance", "pii": [],
    },
    "reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "schema": "review_id STRING, order_id STRING, review_score INT, review_comment_title STRING, "
                  "review_comment_message STRING, review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP",
        "key": ["review_id", "order_id"],
        "owner": "cx", "pii": ["review_comment_title", "review_comment_message"],
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "schema": "product_category_name STRING, product_category_name_english STRING",
        "key": ["product_category_name"],
        "owner": "catalog", "pii": [],
    },
}

ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped", "delivered", "unavailable", "canceled"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
