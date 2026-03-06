"""Navigation routing behavior tests."""

from navigation import (
    DEFAULT_PAGE,
    clear_legacy_flags,
    derive_page_from_legacy_flags,
    navigate_to,
    normalize_page,
    resolve_current_page,
)


class FakeQueryParams(dict):
    """Minimal mutable mapping to mimic st.query_params."""


class FakeStreamlit:
    """Tiny Streamlit stub for routing tests."""

    def __init__(self, initial_query=None):
        self.session_state = {}
        self.query_params = FakeQueryParams(initial_query or {})

    def experimental_get_query_params(self):
        return dict(self.query_params)

    def experimental_set_query_params(self, **kwargs):
        self.query_params.clear()
        self.query_params.update(kwargs)


def test_normalize_page_returns_default_for_invalid():
    assert normalize_page(None) == DEFAULT_PAGE
    assert normalize_page("") == DEFAULT_PAGE
    assert normalize_page("unknown_page") == DEFAULT_PAGE


def test_derive_page_from_legacy_flags():
    state = {"show_sector_strategy": True}
    assert derive_page_from_legacy_flags(state) == "sector_strategy"


def test_clear_legacy_flags():
    state = {"show_sector_strategy": True, "show_smart_monitor": True}
    clear_legacy_flags(state)
    assert "show_sector_strategy" not in state
    assert "show_smart_monitor" not in state


def test_resolve_current_page_url_param_has_highest_priority():
    fake_st = FakeStreamlit(initial_query={"page": "smart_monitor"})
    fake_st.session_state["current_page"] = "sector_strategy"
    fake_st.session_state["show_monitor"] = True

    page = resolve_current_page(st_module=fake_st)

    assert page == "smart_monitor"
    assert fake_st.session_state["current_page"] == "smart_monitor"
    assert fake_st.query_params["page"] == "smart_monitor"


def test_resolve_current_page_invalid_url_falls_back_home():
    fake_st = FakeStreamlit(initial_query={"page": "invalid_page"})
    fake_st.session_state["current_page"] = "sector_strategy"

    page = resolve_current_page(st_module=fake_st)

    assert page == DEFAULT_PAGE
    assert fake_st.session_state["current_page"] == DEFAULT_PAGE
    assert fake_st.query_params["page"] == DEFAULT_PAGE


def test_resolve_current_page_legacy_flag_mapping():
    fake_st = FakeStreamlit()
    fake_st.session_state["show_monitor"] = True

    page = resolve_current_page(st_module=fake_st)

    assert page == "monitor"
    assert fake_st.session_state["current_page"] == "monitor"
    assert fake_st.query_params["page"] == "monitor"


def test_navigate_to_updates_session_query_and_legacy_flag():
    fake_st = FakeStreamlit()
    fake_st.session_state["show_history"] = True

    page = navigate_to("sector_strategy", st_module=fake_st)

    assert page == "sector_strategy"
    assert fake_st.session_state["current_page"] == "sector_strategy"
    assert fake_st.query_params["page"] == "sector_strategy"
    assert fake_st.session_state.get("show_sector_strategy") is True
    assert "show_history" not in fake_st.session_state
