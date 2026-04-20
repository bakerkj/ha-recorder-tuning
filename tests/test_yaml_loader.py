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
# Missing / empty / unreadable file
# ---------------------------------------------------------------------------


def test_missing_file_returns_none(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) is None


def test_empty_file_returns_none(tmp_path, caplog):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "")
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) is None
    assert "must contain a top-level 'rules:'" in caplog.text


def test_top_level_not_a_mapping_returns_none(tmp_path, caplog):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "- just_a_list\n- of_strings\n")
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) is None
    assert "must contain a top-level 'rules:'" in caplog.text


def test_missing_rules_key_returns_none(tmp_path, caplog):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "some_other_key: value\n")
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) is None
    assert "must contain a top-level 'rules:'" in caplog.text


# ---------------------------------------------------------------------------
# Invalid YAML syntax
# ---------------------------------------------------------------------------


def test_bad_yaml_syntax_returns_none(tmp_path, caplog):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "rules:\n  - name: broken\n   keep_days: 3\n")  # bad indent
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) is None
    assert "YAML parse error" in caplog.text


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
    assert rules is not None
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
    assert rules is not None
    assert [r["name"] for r in rules] == ["good", "also_good"]
    assert "skipping rule[1]" in caplog.text


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
    assert rules is not None
    assert [r["name"] for r in rules] == ["ok"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_rules_list_returns_empty_list(tmp_path):
    from custom_components.recorder_tuning import _load_yaml_rules

    _write_yaml(tmp_path, "rules: []\n")
    assert _load_yaml_rules(_hass_with_config_dir(tmp_path)) == []
