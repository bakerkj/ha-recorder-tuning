# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for the top-level CONFIG_SCHEMA.

These tests cover behaviour that used to live in ``test_yaml_loader.py``:
defaults, field coercion, and rejection of bad values.  Because the
integration is now YAML-driven, CONFIG_SCHEMA *is* the parser — there is no
separate loader to test.
"""

from __future__ import annotations

import pytest
import voluptuous as vol


def _validate(domain_block: dict) -> dict:
    from custom_components.recorder_tuning import CONFIG_SCHEMA
    from custom_components.recorder_tuning.const import DOMAIN

    return CONFIG_SCHEMA({DOMAIN: domain_block})[DOMAIN]


# ---------------------------------------------------------------------------
# Top-level defaults
# ---------------------------------------------------------------------------


def test_top_level_defaults_filled_when_only_rules_given():
    """An almost-empty block should receive all defaults."""
    result = _validate({"rules": []})
    assert result["purge_time"] == "03:00"
    assert result["stats_keep_days"] == 30
    assert result["dry_run"] is True
    assert result["rules"] == []


def test_top_level_accepts_all_fields():
    result = _validate(
        {
            "purge_time": "04:30",
            "stats_keep_days": 60,
            "dry_run": False,
            "rules": [],
        }
    )
    assert result["purge_time"] == "04:30"
    assert result["stats_keep_days"] == 60
    assert result["dry_run"] is False


def test_top_level_rejects_bad_purge_time():
    with pytest.raises(vol.Invalid):
        _validate({"purge_time": "24:00", "rules": []})


def test_top_level_rejects_out_of_range_stats_keep_days():
    with pytest.raises(vol.Invalid):
        _validate({"stats_keep_days": 0, "rules": []})
    with pytest.raises(vol.Invalid):
        _validate({"stats_keep_days": 500, "rules": []})


# ---------------------------------------------------------------------------
# Rule defaults
# ---------------------------------------------------------------------------


def test_rule_defaults_filled():
    """A minimal rule gets all optional-with-default fields populated."""
    result = _validate({"rules": [{"name": "minimal", "keep_days": 7}]})
    rule = result["rules"][0]
    assert rule == {
        "name": "minimal",
        "keep_days": 7,
        "integration_filter": [],
        "device_ids": [],
        "entity_ids": [],
        "entity_globs": [],
        "entity_regex_include": [],
        "entity_regex_exclude": [],
        "enabled": True,
        "match_mode": "all",
        "dry_run": None,
    }


# ---------------------------------------------------------------------------
# match_mode
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["all", "any"])
def test_rule_match_mode_accepts_valid_values(mode):
    result = _validate(
        {
            "rules": [
                {
                    "name": "m",
                    "entity_ids": ["sensor.a"],
                    "keep_days": 7,
                    "match_mode": mode,
                }
            ]
        }
    )
    assert result["rules"][0]["match_mode"] == mode


def test_rule_match_mode_rejects_bad_value():
    with pytest.raises(vol.Invalid):
        _validate(
            {
                "rules": [
                    {
                        "name": "bad",
                        "entity_ids": ["sensor.a"],
                        "keep_days": 7,
                        "match_mode": "intersection",
                    }
                ]
            }
        )


# ---------------------------------------------------------------------------
# Per-rule dry_run override
# ---------------------------------------------------------------------------


def test_rule_dry_run_defaults_to_none():
    result = _validate(
        {"rules": [{"name": "no override", "entity_ids": ["sensor.a"], "keep_days": 7}]}
    )
    assert result["rules"][0]["dry_run"] is None


@pytest.mark.parametrize("value", [True, False])
def test_rule_dry_run_accepts_bool(value):
    result = _validate(
        {
            "rules": [
                {
                    "name": "override",
                    "entity_ids": ["sensor.a"],
                    "keep_days": 7,
                    "dry_run": value,
                }
            ]
        }
    )
    assert result["rules"][0]["dry_run"] is value


def test_rule_dry_run_rejects_non_bool():
    with pytest.raises(vol.Invalid):
        _validate(
            {
                "rules": [
                    {
                        "name": "bad override",
                        "entity_ids": ["sensor.a"],
                        "keep_days": 7,
                        "dry_run": "yes",
                    }
                ]
            }
        )


# ---------------------------------------------------------------------------
# Regex validation happens at parse time
# ---------------------------------------------------------------------------


def test_rule_bad_regex_rejected_at_validation():
    """Invalid regex in entity_regex_include must surface at schema validation."""
    with pytest.raises(vol.Invalid):
        _validate(
            {
                "rules": [
                    {
                        "name": "bad regex",
                        "entity_regex_include": ["[unclosed"],
                        "keep_days": 7,
                    }
                ]
            }
        )


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------


def test_rule_missing_name_rejected():
    with pytest.raises(vol.Invalid):
        _validate({"rules": [{"keep_days": 7}]})


def test_rule_missing_keep_days_rejected():
    with pytest.raises(vol.Invalid):
        _validate({"rules": [{"name": "no keep days"}]})


def test_rule_keep_days_out_of_range_rejected():
    with pytest.raises(vol.Invalid):
        _validate({"rules": [{"name": "too small", "keep_days": 0}]})
    with pytest.raises(vol.Invalid):
        _validate({"rules": [{"name": "too big", "keep_days": 500}]})
