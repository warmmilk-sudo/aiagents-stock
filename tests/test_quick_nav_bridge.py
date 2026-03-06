"""Tests for quick nav bridge helpers."""

from quick_nav import (
    apply_quick_to_state,
    clear_quick_param,
    clear_nav_flags,
    normalize_quick,
)


def test_normalize_quick():
    assert normalize_quick("home") == "home"
    assert normalize_quick(" SMART_MONITOR ") == "smart_monitor"
    assert normalize_quick("invalid") is None
    assert normalize_quick(None) is None


def test_clear_nav_flags():
    state = {"show_sector_strategy": True, "show_news_flow": True, "other": 1}
    clear_nav_flags(state)
    assert "show_sector_strategy" not in state
    assert "show_news_flow" not in state
    assert state["other"] == 1


def test_apply_quick_to_state_home():
    state = {"show_sector_strategy": True}
    applied = apply_quick_to_state(state, "home")
    assert applied is True
    assert "show_sector_strategy" not in state


def test_apply_quick_to_state_target():
    state = {"show_sector_strategy": True}
    applied = apply_quick_to_state(state, "news_flow")
    assert applied is True
    assert state.get("show_news_flow") is True
    assert "show_sector_strategy" not in state


def test_apply_quick_to_state_invalid():
    state = {"show_sector_strategy": True}
    applied = apply_quick_to_state(state, "abc")
    assert applied is False
    assert state.get("show_sector_strategy") is True


def test_clear_quick_param():
    params = {"quick": "home", "foo": "bar"}
    clear_quick_param(params)
    assert "quick" not in params
    assert params["foo"] == "bar"
