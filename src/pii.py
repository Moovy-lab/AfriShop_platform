"""Salted hashing helpers for personally identifying source fields."""

from __future__ import annotations

import hashlib
import os

from pyspark.sql import Column
from pyspark.sql import functions as F


def _salt() -> str:
    """Return the configured PII salt or fail explicitly."""
    salt = os.getenv("PII_HASH_SALT")
    if not salt:
        raise ValueError("PII_HASH_SALT must be set and non-empty")
    return salt


def hash_value(value: str | None) -> str | None:
    """Return a deterministic SHA-256 hex digest of a normalized value."""
    salt = _salt()
    if value is None:
        return None
    normalized = value.strip().lower()
    return hashlib.sha256((salt + normalized).encode("utf-8")).hexdigest()


def hash_column(column: str) -> Column:
    """Build a Spark expression for hashing a string column with the configured salt."""
    salt = _salt()
    normalized = F.lower(F.trim(F.col(column).cast("string")))
    return F.when(F.col(column).isNull(), F.lit(None)).otherwise(
        F.sha2(F.concat(F.lit(salt), normalized), 256)
    )
