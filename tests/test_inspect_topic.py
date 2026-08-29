"""Broker-free unit tests for the inspect-topic debug consumer."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from producer.inspect_topic import (
    consume_and_print,
    decode_key,
    format_record,
    parse_payload,
)


class _FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)

    def poll(self, _timeout):
        if not self._messages:
            return None
        return self._messages.pop(0)


def test_decode_key_and_parse_payload():
    payload = {"schema_version": 1, "account_id": "acct_1001"}

    assert decode_key(b"acct_1001") == "acct_1001"
    assert decode_key(None) == ""
    assert parse_payload(json.dumps(payload).encode("utf-8")) == payload


def test_format_record_includes_key_and_json():
    rendered = format_record("acct_1001", {"schema_version": 1, "amount": "10.00"})

    assert rendered.startswith("key=acct_1001")
    assert '"schema_version": 1' in rendered
    assert '"amount": "10.00"' in rendered


def test_consume_and_print_stops_at_max_messages(capsys):
    records = [
        SimpleNamespace(
            error=lambda: None,
            key=lambda index=index: f"acct_{index}".encode("utf-8"),
            value=lambda index=index: json.dumps({"schema_version": 1, "n": index}).encode(
                "utf-8"
            ),
        )
        for index in range(3)
    ]
    received = consume_and_print(_FakeConsumer(records), max_messages=2, timeout=5.0)

    captured = capsys.readouterr()
    assert received == 2
    assert captured.out.count("key=acct_") == 2
    assert '"n": 2' not in captured.out


def test_consume_and_print_rejects_invalid_limits():
    with pytest.raises(ValueError):
        consume_and_print(_FakeConsumer([]), max_messages=0, timeout=5.0)
