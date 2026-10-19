from datetime import datetime

from src.app import RawEvent, parse, apply_rules


def test_parse_valid():
    raw = RawEvent("test", 0, "u", "#OBJ1\nTYPE_A.PHASE1\nValue: 100.5")
    p = parse(raw)
    assert p is not None
    assert p.object_id == "OBJ1"
    assert p.value == 100.5


def test_parse_invalid():
    raw = RawEvent("test", 0, "u", "hello")
    assert parse(raw) is None


def test_rule_matches():
    raw = RawEvent("test", 0, "u", "#OBJ1\nTYPE_A.PHASE1\nValue: 10")
    p = parse(raw)
    r = apply_rules(p, {})
    assert r.matched is True
