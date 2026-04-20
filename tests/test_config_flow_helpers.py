# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for pure helper functions in config_flow.py."""

import pytest

from custom_components.recorder_tuning.config_flow import (
    _build_rule,
    _split_csv,
    _valid_time,
    _validate_rule_input,
)
from custom_components.recorder_tuning.const import (
    CONF_DEVICE_IDS,
    CONF_ENABLED,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_INTEGRATION_FILTER,
    CONF_KEEP_DAYS,
    CONF_RULE_NAME,
)

# ── _split_csv ────────────────────────────────────────────────────────────────


def test_split_csv_basic():
    assert _split_csv("a, b, c") == ["a", "b", "c"]


def test_split_csv_strips_whitespace():
    assert _split_csv("  x  ,  y  ") == ["x", "y"]


def test_split_csv_empty_string():
    assert _split_csv("") == []


def test_split_csv_none():
    assert _split_csv(None) == []


def test_split_csv_single():
    assert _split_csv("sensor.foo") == ["sensor.foo"]


def test_split_csv_skips_blank_entries():
    assert _split_csv("a,,b") == ["a", "b"]


# ── _valid_time ───────────────────────────────────────────────────────────────


def test_valid_time_ok():
    assert _valid_time("03:00") == "03:00"


def test_valid_time_midnight():
    assert _valid_time("0:00") == "00:00"


def test_valid_time_end_of_day():
    assert _valid_time("23:59") == "23:59"


def test_valid_time_bad_format():
    import voluptuous as vol

    with pytest.raises(vol.Invalid):
        _valid_time("3am")


def test_valid_time_out_of_range():
    import voluptuous as vol

    with pytest.raises(vol.Invalid):
        _valid_time("25:00")


# ── _validate_rule_input ──────────────────────────────────────────────────────


def _base_input(**kwargs):
    defaults = {
        CONF_RULE_NAME: "My Rule",
        CONF_ENTITY_GLOBS: "sensor.foo_*",
        CONF_KEEP_DAYS: 7,
    }
    defaults.update(kwargs)
    return defaults


def test_validate_rule_input_ok():
    assert _validate_rule_input(_base_input()) == {}


def test_validate_rule_input_missing_name():
    inp = _base_input()
    inp[CONF_RULE_NAME] = ""
    errors = _validate_rule_input(inp)
    assert CONF_RULE_NAME in errors


def test_validate_rule_input_no_targets():
    inp = {CONF_RULE_NAME: "My Rule", CONF_KEEP_DAYS: 7}
    errors = _validate_rule_input(inp)
    assert "base" in errors


def test_validate_rule_input_integration_filter_counts_as_target():
    inp = {
        CONF_RULE_NAME: "My Rule",
        CONF_INTEGRATION_FILTER: "frigate",
        CONF_KEEP_DAYS: 7,
    }
    assert _validate_rule_input(inp) == {}


def test_validate_rule_input_regex_include_counts_as_target():
    inp = {
        CONF_RULE_NAME: "My Rule",
        CONF_ENTITY_REGEX_INCLUDE: r"^sensor\.frigate",
        CONF_KEEP_DAYS: 7,
    }
    assert _validate_rule_input(inp) == {}


def test_validate_rule_input_bad_regex_include():
    inp = _base_input(**{CONF_ENTITY_REGEX_INCLUDE: "["})
    errors = _validate_rule_input(inp)
    assert CONF_ENTITY_REGEX_INCLUDE in errors


def test_validate_rule_input_bad_regex_exclude():
    inp = _base_input(**{CONF_ENTITY_REGEX_EXCLUDE: "("})
    errors = _validate_rule_input(inp)
    assert CONF_ENTITY_REGEX_EXCLUDE in errors


def test_validate_rule_input_valid_regex_exclude_no_error():
    inp = _base_input(**{CONF_ENTITY_REGEX_EXCLUDE: "_debug$"})
    assert _validate_rule_input(inp) == {}


# ── _build_rule ───────────────────────────────────────────────────────────────


def test_build_rule_basic():
    inp = {
        CONF_RULE_NAME: "  Frigate FPS  ",
        CONF_ENTITY_GLOBS: "sensor.frigate_*_fps, sensor.frigate_*_skipped",
        CONF_KEEP_DAYS: 3,
    }
    rule = _build_rule(inp)
    assert rule[CONF_RULE_NAME] == "Frigate FPS"
    assert rule[CONF_ENTITY_GLOBS] == [
        "sensor.frigate_*_fps",
        "sensor.frigate_*_skipped",
    ]
    assert rule[CONF_KEEP_DAYS] == 3
    assert rule[CONF_ENABLED] is True


def test_build_rule_integration_filter():
    inp = {
        CONF_RULE_NAME: "All Frigate",
        CONF_INTEGRATION_FILTER: "frigate, esphome",
        CONF_KEEP_DAYS: 7,
    }
    rule = _build_rule(inp)
    assert rule[CONF_INTEGRATION_FILTER] == ["frigate", "esphome"]


def test_build_rule_regex_fields():
    inp = {
        CONF_RULE_NAME: "Regex Rule",
        CONF_ENTITY_REGEX_INCLUDE: r"^sensor\.frigate",
        CONF_ENTITY_REGEX_EXCLUDE: "_debug$, _raw$",
        CONF_KEEP_DAYS: 5,
    }
    rule = _build_rule(inp)
    assert rule[CONF_ENTITY_REGEX_INCLUDE] == [r"^sensor\.frigate"]
    assert rule[CONF_ENTITY_REGEX_EXCLUDE] == ["_debug$", "_raw$"]


def test_build_rule_disabled():
    inp = _base_input(**{CONF_ENABLED: False})
    rule = _build_rule(inp)
    assert rule[CONF_ENABLED] is False


def test_build_rule_empty_optional_fields():
    inp = {CONF_RULE_NAME: "Minimal", CONF_ENTITY_IDS: "sensor.foo", CONF_KEEP_DAYS: 1}
    rule = _build_rule(inp)
    assert rule[CONF_ENTITY_GLOBS] == []
    assert rule[CONF_INTEGRATION_FILTER] == []
    assert rule[CONF_DEVICE_IDS] == []


# ── Round-trip: build then validate ──────────────────────────────────────────


def test_build_rule_survives_validate():
    """A rule built from valid input should pass validate when re-submitted."""
    inp = _base_input(
        **{
            CONF_INTEGRATION_FILTER: "frigate",
            CONF_ENTITY_REGEX_EXCLUDE: "_debug$",
        }
    )
    rule = _build_rule(inp)
    # Convert lists back to CSV strings as the form would
    roundtrip = {
        CONF_RULE_NAME: rule[CONF_RULE_NAME],
        CONF_ENTITY_GLOBS: ", ".join(rule[CONF_ENTITY_GLOBS]),
        CONF_INTEGRATION_FILTER: ", ".join(rule[CONF_INTEGRATION_FILTER]),
        CONF_ENTITY_REGEX_EXCLUDE: ", ".join(rule[CONF_ENTITY_REGEX_EXCLUDE]),
        CONF_KEEP_DAYS: rule[CONF_KEEP_DAYS],
        CONF_ENABLED: rule[CONF_ENABLED],
    }
    assert _validate_rule_input(roundtrip) == {}
