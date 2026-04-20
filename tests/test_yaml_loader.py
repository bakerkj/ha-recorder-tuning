# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for ``_load_yaml_rules``.

Parses ``recorder_tuning.yaml`` directly via ``tmp_path``. The integration
tests in ``tests/integration/test_yaml_config.py`` exercise the same paths
end-to-end through a full HA setup; these cheaper unit tests make it easy
to add edge cases without spinning up a recorder instance.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from homeassistant.exceptions import HomeAssistantError


def _hass_with_config_dir(config_dir: Path) -> MagicMock:
    """Return a minimal hass stub whose ``hass.config.path(name)`` points at tmp_path."""
    hass = MagicMock()
    hass.config.path = lambda name: str(config_dir / name)
    return hass


def _write_yaml(config_dir: Path, body: str) -> Path:
    path = config_dir / "recorder_tuning.yaml"
    path.write_text(body, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Missing file is legitimate — returns empty list, no error
# ---------------------------------------------------------------------------


def test_missing_file_returns_empty(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) == []


# ---------------------------------------------------------------------------
# File present but malformed — must raise HomeAssistantError so the reload
# service surfaces the failure. Setup catches and falls back to empty.
# ---------------------------------------------------------------------------


def test_empty_file_raises(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "")
    with pytest.raises(HomeAssistantError, match="must contain a top-level 'rules:'"):
        _load_yaml_rules(_hass_with_config_dir(tmp_path))


def test_top_level_not_a_mapping_raises(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "- just_a_list\n- of_strings\n")
    with pytest.raises(HomeAssistantError, match="must contain a top-level 'rules:'"):
        _load_yaml_rules(_hass_with_config_dir(tmp_path))


def test_missing_rules_key_raises(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "some_other_key: value\n")
    with pytest.raises(HomeAssistantError, match="must contain a top-level 'rules:'"):
        _load_yaml_rules(_hass_with_config_dir(tmp_path))


def test_rules_as_mapping_not_list_raises(tmp_path):
    """`rules:` must be a YAML list, not a mapping."""
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "rules:\n  foo: 1\n  bar: 2\n")
    with pytest.raises(HomeAssistantError, match="'rules:' must be a YAML list"):
        _load_yaml_rules(_hass_with_config_dir(tmp_path))


def test_bad_yaml_syntax_raises(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "rules:\n  - name: broken\n   keep_days: 3\n")  # bad indent
    with pytest.raises(HomeAssistantError, match="YAML parse error"):
        _load_yaml_rules(_hass_with_config_dir(tmp_path))


# ---------------------------------------------------------------------------
# Valid rules, including schema defaults
# ---------------------------------------------------------------------------


def test_valid_minimal_rule_loaded_with_defaults(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: minimal
    keep_days: 7
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert rules == [
        {
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
        }
    ]


def test_multiple_valid_rules_preserve_order(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: first
    keep_days: 7
    entity_ids: [sensor.a]
  - name: second
    keep_days: 3
    entity_globs: ["sensor.frigate_*"]
    enabled: false
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["first", "second"]
    assert rules[0]["entity_ids"] == ["sensor.a"]
    assert rules[1]["entity_globs"] == ["sensor.frigate_*"]
    assert rules[1]["enabled"] is False


# ---------------------------------------------------------------------------
# Partial validation: one bad rule shouldn't lose the good ones
# ---------------------------------------------------------------------------


def test_invalid_rule_skipped_good_rules_kept(tmp_path, caplog):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: good
    keep_days: 7
  - keep_days: 3                 # missing required 'name'
  - name: also_good
    keep_days: 5
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["good", "also_good"]
    assert "skipping rule[1]" in caplog.text


def test_invalid_regex_include_skips_rule(tmp_path, caplog):
    """A rule with an invalid regex_include pattern is dropped at load time."""
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: bad_regex
    entity_regex_include: ["[unclosed"]
    keep_days: 7
  - name: good
    entity_regex_include: ["^sensor\\\\."]
    keep_days: 3
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["good"]
    assert "invalid regex" in caplog.text
    assert "skipping rule[0]" in caplog.text


def test_invalid_regex_exclude_skips_rule(tmp_path, caplog):
    """Invalid regex in regex_exclude also drops the rule at load time."""
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: bad_exclude
    entity_globs: ["sensor.*"]
    entity_regex_exclude: ["(unclosed"]
    keep_days: 7
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert rules == []
    assert "invalid regex" in caplog.text


@pytest.mark.parametrize("bad_keep_days", [0, -1, 400])
def test_rule_with_keep_days_out_of_range_skipped(tmp_path, bad_keep_days):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        f"""\
rules:
  - name: out_of_range
    keep_days: {bad_keep_days}
  - name: ok
    keep_days: 5
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["ok"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_rules_list_returns_empty_list(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "rules: []\n")
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) == []


def test_match_mode_defaults_to_all(tmp_path):
    """Rules without an explicit match_mode default to intersection semantics."""
    from custom_components.recorder_tuning import _load_yaml_rules
    from custom_components.recorder_tuning.const import CONF_MATCH_MODE, MATCH_MODE_ALL

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: no mode
    entity_ids: [sensor.a]
    keep_days: 7
""",
    )
    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert rules[0][CONF_MATCH_MODE] == MATCH_MODE_ALL


@pytest.mark.parametrize("mode", ["all", "any"])
def test_match_mode_accepts_valid_values(tmp_path, mode):
    from custom_components.recorder_tuning import _load_yaml_rules
    from custom_components.recorder_tuning.const import CONF_MATCH_MODE

    _write_yaml(
        tmp_path,
        f"""\
rules:
  - name: mode test
    match_mode: {mode}
    entity_ids: [sensor.a]
    keep_days: 7
""",
    )
    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert rules[0][CONF_MATCH_MODE] == mode


def test_match_mode_rejects_bad_value_skips_rule(tmp_path, caplog):
    """A rule with an unknown match_mode value is skipped with a warning."""
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: bad mode
    match_mode: intersection
    entity_ids: [sensor.a]
    keep_days: 7
  - name: ok
    keep_days: 5
    entity_ids: [sensor.b]
""",
    )
    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["ok"]
    assert "validation error" in caplog.text


def test_duplicate_rule_names_warned_but_all_returned(tmp_path, caplog):
    """Duplicates still load (rule engine supports them), but a warning is emitted."""
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(
        tmp_path,
        """\
rules:
  - name: shared_name
    keep_days: 7
    entity_ids: [sensor.a]
  - name: unique
    keep_days: 3
    entity_ids: [sensor.b]
  - name: shared_name
    keep_days: 5
    entity_ids: [sensor.c]
""",
    )

    rules = _load_yaml_rules(_hass_with_config_dir(tmp_path))
    assert [r["name"] for r in rules] == ["shared_name", "unique", "shared_name"]
    assert "rule name 'shared_name' appears more than once" in caplog.text
