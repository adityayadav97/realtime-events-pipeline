"""Unit tests for event generation and schema validity."""

import sys
import json
sys.path.append(".")
from src.producer.event_producer import make_event
from src.config import EVENT_TYPES


def test_event_has_required_fields():
    e = make_event()
    for field in ["event_id", "user_id", "event_type", "event_ts", "page", "amount"]:
        assert field in e, f"missing field: {field}"


def test_event_type_is_valid():
    for _ in range(50):
        assert make_event()["event_type"] in EVENT_TYPES


def test_event_is_json_serializable():
    e = make_event()
    s = json.dumps(e)
    assert json.loads(s)["event_id"] == e["event_id"]


def test_amount_is_non_negative():
    for _ in range(50):
        assert make_event()["amount"] >= 0
