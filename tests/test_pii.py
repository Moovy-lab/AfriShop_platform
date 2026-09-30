"""Tests for salted personal data hashing."""

from __future__ import annotations

import pytest

from pii import hash_value


def test_hash_is_deterministic_and_normalized(monkeypatch):
    monkeypatch.setenv("AFRISHOP_PII_SALT", "test-salt")
    first = hash_value(" AbC123 ")
    assert first == hash_value("abc123")
    assert len(first) == 64


def test_hash_requires_salt(monkeypatch):
    monkeypatch.delenv("AFRISHOP_PII_SALT", raising=False)
    with pytest.raises(ValueError, match="AFRISHOP_PII_SALT"):
        hash_value("value")
