"""Tests for curated transformation rules."""

from __future__ import annotations

from pathlib import Path
import hashlib

import pytest

from pyspark.sql import functions as F

import curated.products as products_job
import curated.customers as customers_job
import curated.deliveries as deliveries_job
import curated.order_lines as order_lines_job
import curated.orders as orders_job
import curated.payments as payments_job
import curated_io
from curated.customers import transform as transform_customers
from curated.deliveries import transform as transform_deliveries
from curated.order_lines import transform as transform_order_lines
from curated.orders import transform as transform_orders
from curated.payments import transform as transform_payments
from curated.products import transform as transform_products
from curated_io import data_root, latest_per_key, read_source


def test_products_quarantines_zero_price_and_flags_margin(spark):
    frame = spark.createDataFrame(
        [("p1", 12.0, 10.0), ("p2", 2.0, 0.0)],
        "product_id string, unit_cost double, unit_price double",
    )
    valid, rejected = transform_products(frame)
    assert valid.select("product_id", "is_negative_margin").collect()[0][1] is True
    assert rejected.select("reason").first()[0] == "unit_price_zero"


def test_orders_keeps_latest_and_quarantines_mismatch(spark):
    frame = spark.createDataFrame(
        [
            ("o1", "2025-01-01 00:00:00", "2025-01-02 00:00:00", 10.0, 0.0, 0.0, 10.0),
            ("o1", "2025-01-01 00:00:00", "2025-01-03 00:00:00", 12.0, 0.0, 0.0, 12.0),
            ("o2", "2025-01-01 00:00:00", "2025-01-02 00:00:00", 9.0, 0.0, 0.0, 10.0),
        ],
        "order_id string, order_date string, updated_at string, subtotal_amount double, "
        "shipping_amount double, discount_amount double, total_amount double",
    )
    valid, rejected, duplicates = transform_orders(frame)
    assert duplicates == 1
    assert valid.select("order_id").first()[0] == "o1"
    assert rejected.select("reason").first()[0] == "order_total_mismatch"


def test_order_lines_quarantines_orphan_before_amount_check(spark):
    lines = spark.createDataFrame(
        [("l1", "p1", 1, 5.0, 0.0, 5.0), ("l2", "ghost", 1, 5.0, 0.0, 8.0)],
        "order_line_id string, product_id string, quantity string, unit_price string, "
        "line_discount string, line_total string",
    )
    products = spark.createDataFrame([("p1",)], "product_id string")
    valid, rejected, _ = transform_order_lines(lines, products)
    assert valid.count() == 1
    assert rejected.select("reason").first()[0] == "unknown_product_id|line_total_mismatch"


def test_customer_collision_keeps_first_and_quarantines_other(spark, monkeypatch):
    monkeypatch.setenv("PII_HASH_SALT", "test-salt")
    frame = spark.createDataFrame(
        [
            ("c1", "email-a", "phone-a", "2024-01-01"),
            ("c1", "email-b", "phone-b", "2024-01-02"),
            ("c2", "email-a", "phone-a", "2024-01-03"),
        ],
        "customer_id string, email_hash string, phone_hash string, registration_date string",
    )
    valid, rejected, _ = transform_customers(frame)
    assert valid.filter("customer_id = 'c1'").count() == 1
    assert rejected.select("reason").first()[0] == "customer_id_collision"
    assert valid.filter("customer_master_id = 'c1'").count() == 2


def test_deliveries_quarantines_delivery_before_shipment(spark):
    frame = spark.createDataFrame(
        [("d1", "2025-01-02", "2025-01-01"), ("d2", "2025-01-01", "2025-01-02")],
        "delivery_id string, shipped_at string, delivered_at string",
    )
    valid, rejected, _ = transform_deliveries(frame)
    assert valid.count() == 1
    assert rejected.select("reason").first()[0] == "delivered_before_shipped"


def test_payments_keeps_distinct_attempts_and_deduplicates_ids(spark):
    frame = spark.createDataFrame(
        [
            ("pay1", "FAILED", "2025-01-01"),
            ("pay1", "FAILED", "2025-01-01"),
            ("pay2", "CAPTURED", "2025-01-02"),
        ],
        "payment_id string, payment_status string, payment_date string",
    )
    valid, rejected, duplicates = transform_payments(frame)
    assert valid.count() == 2
    assert rejected.count() == 0
    assert duplicates == 1


def test_latest_per_key_keeps_updated_record(spark):
    frame = spark.createDataFrame(
        [("k1", "2025-01-01"), ("k1", "2025-01-02")],
        "id string, updated_at string",
    ).withColumnRenamed("id", "key")
    assert latest_per_key(frame, "key").select("updated_at").first()[0] == "2025-01-02"


def test_products_job_is_idempotent(spark, tmp_path, monkeypatch):
    frame = spark.createDataFrame(
        [("p1", 3.0, 5.0), ("p2", 8.0, 4.0)],
        "product_id string, unit_cost double, unit_price double",
    )
    monkeypatch.setattr(products_job, "get_spark", lambda app_name: spark)
    monkeypatch.setattr(products_job, "read_source", lambda session, table, csv_source: frame)
    monkeypatch.setattr(curated_io, "data_root", lambda: Path(tmp_path))

    products_job.run()
    first = spark.read.format("delta").load(str(tmp_path / "lakehouse/curated/products"))
    first_count = first.count()
    first_checksum = first.select(
        F.sum(F.xxhash64("product_id", "unit_cost", "unit_price", "is_negative_margin"))
    ).first()[0]
    products_job.run()
    second = spark.read.format("delta").load(str(tmp_path / "lakehouse/curated/products"))
    second_checksum = second.select(
        F.sum(F.xxhash64("product_id", "unit_cost", "unit_price", "is_negative_margin"))
    ).first()[0]
    assert second.count() == first_count
    assert second_checksum == first_checksum


@pytest.mark.integration
@pytest.mark.parametrize(
    ("job", "table", "schema", "rows"),
    [
        (
            products_job,
            "products",
            "product_id string, unit_cost double, unit_price double",
            [("p1", 3.0, 5.0)],
        ),
        (
            customers_job,
            "customers",
            "customer_id string, email_hash string, phone_hash string, registration_date string",
            [("c1", "email", "phone", "2025-01-01")],
        ),
        (
            orders_job,
            "orders",
            "order_id string, order_date string, updated_at string, subtotal_amount double, "
            "shipping_amount double, discount_amount double, total_amount double",
            [("o1", "2025-01-01", "2025-01-01", 10.0, 0.0, 0.0, 10.0)],
        ),
        (
            order_lines_job,
            "order_lines",
            "order_line_id string, product_id string, quantity string, unit_price string, "
            "line_discount string, line_total string",
            [("l1", "p1", "2", "5.00", "0.00", "10.00")],
        ),
        (
            payments_job,
            "payments",
            "payment_id string, payment_status string, payment_date string",
            [("pay1", "CAPTURED", "2025-01-01")],
        ),
        (
            deliveries_job,
            "deliveries",
            "delivery_id string, shipped_at string, delivered_at string",
            [("d1", "2025-01-01", "2025-01-02")],
        ),
    ],
    ids=["products", "customers", "orders", "order_lines", "payments", "deliveries"],
)
def test_curated_jobs_are_idempotent(spark, tmp_path, monkeypatch, job, table, schema, rows):
    frame = spark.createDataFrame(rows, schema)
    product_frame = spark.createDataFrame([("p1",)], "product_id string")
    monkeypatch.setattr(job, "get_spark", lambda app_name: spark)
    monkeypatch.setattr(
        job,
        "read_source",
        lambda session, source, csv_source=False: product_frame if source == "products" else frame,
    )
    monkeypatch.setattr(curated_io, "data_root", lambda: Path(tmp_path))
    monkeypatch.setenv("PII_HASH_SALT", "test-salt")

    job.run()
    target_path = str(tmp_path / "lakehouse/curated" / table)
    first = spark.read.format("delta").load(target_path)
    first_rows = first.orderBy(*sorted(first.columns)).toJSON().collect()
    first_fingerprint = hashlib.sha256("".join(first_rows).encode()).hexdigest()

    job.run()
    second = spark.read.format("delta").load(target_path)
    second_rows = second.orderBy(*sorted(second.columns)).toJSON().collect()
    second_fingerprint = hashlib.sha256("".join(second_rows).encode()).hexdigest()

    assert second.count() == first.count() == 1
    assert second_fingerprint == first_fingerprint


@pytest.mark.integration
def test_products_raw_csv_integration_when_available(spark):
    if not (data_root() / "raw" / "products.csv").exists():
        pytest.skip("Synthetic products CSV is not available")
    source = read_source(spark, "products", csv_source=True)
    valid, rejected = transform_products(source)
    assert source.count() == 12_000
    assert rejected.count() == 119
    assert valid.filter("is_negative_margin").count() == 60


@pytest.mark.integration
def test_products_raw_parquet_integration_when_available(spark):
    raw_path = data_root() / "lakehouse" / "raw" / "products"
    if not raw_path.exists():
        pytest.skip("Ingested products Parquet data is not available")
    source = read_source(spark, "products")
    valid, rejected = transform_products(source)
    assert source.count() == 12_000
    assert rejected.count() == 119
    assert valid.filter("is_negative_margin").count() == 60
