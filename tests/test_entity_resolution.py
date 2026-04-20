# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for RecorderTuningManager._resolve_entities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


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


# ---------------------------------------------------------------------------
# Minimal stubs for HA types
# ---------------------------------------------------------------------------


def _make_entry(
    entity_id: str,
    platform: str = "test",
    device_id: str | None = None,
    disabled: bool = False,
):
    entry = MagicMock()
    entry.entity_id = entity_id
    entry.platform = platform
    entry.device_id = device_id
    entry.disabled = disabled
    return entry


def _make_registry(*entries):
    reg = MagicMock()
    reg.entities = {e.entity_id: e for e in entries}
    return reg


def _make_manager(hass=None, entries=()):
    """Create a RecorderTuningManager with a minimal stub."""
    from custom_components.recorder_tuning import RecorderTuningManager

    mock_hass = hass or MagicMock()
    mock_entry = MagicMock()
    mock_store = MagicMock()
    manager = RecorderTuningManager(mock_hass, mock_entry, mock_store, {"rules": []})
    return manager


# ---------------------------------------------------------------------------
# Entity ID resolution
# ---------------------------------------------------------------------------


def test_resolve_explicit_entity_ids():
    manager = _make_manager()
    reg = _make_registry(_make_entry("sensor.foo"), _make_entry("sensor.bar"))
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_IDS: ["sensor.foo"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.foo" in result
    assert "sensor.bar" not in result


# ---------------------------------------------------------------------------
# Glob pattern resolution
# ---------------------------------------------------------------------------


def test_resolve_glob_pattern():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.frigate_cam2_fps"),
        _make_entry("sensor.cpu_usage"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.frigate_*_fps"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam2_fps" in result
    assert "sensor.cpu_usage" not in result


def test_resolve_multiple_globs_union():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.cpu_usage"),
        _make_entry("sensor.gpu_temp"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.frigate_*", "sensor.gpu_*"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.gpu_temp" in result
    assert "sensor.cpu_usage" not in result


# ---------------------------------------------------------------------------
# Integration filter
# ---------------------------------------------------------------------------


def test_resolve_integration_filter():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps", platform="frigate"),
        _make_entry("sensor.frigate_cam2_fps", platform="frigate"),
        _make_entry("sensor.cpu_usage", platform="system_monitor"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["frigate"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam2_fps" in result
    assert "sensor.cpu_usage" not in result


def test_resolve_multiple_integrations_union():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_a", platform="frigate"),
        _make_entry("sensor.esp_temp", platform="esphome"),
        _make_entry("sensor.other", platform="zha"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["frigate", "esphome"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_a" in result
    assert "sensor.esp_temp" in result
    assert "sensor.other" not in result


# ---------------------------------------------------------------------------
# Device ID resolution
# ---------------------------------------------------------------------------


def test_resolve_device_id():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.cam1_fps", device_id="dev_abc"),
        _make_entry("binary_sensor.cam1_motion", device_id="dev_abc"),
        _make_entry("sensor.unrelated", device_id="dev_xyz"),
    )

    device_entries = [
        reg.entities["sensor.cam1_fps"],
        reg.entities["binary_sensor.cam1_motion"],
    ]

    with patch(
        "custom_components.recorder_tuning.__init__.er.async_entries_for_device",
        return_value=device_entries,
    ):
        rule = {
            CONF_RULE_NAME: "r",
            CONF_DEVICE_IDS: ["dev_abc"],
            CONF_KEEP_DAYS: 5,
            CONF_ENABLED: True,
        }
        result = manager._resolve_entities(rule, reg)

    assert "sensor.cam1_fps" in result
    assert "binary_sensor.cam1_motion" in result
    assert "sensor.unrelated" not in result


# ---------------------------------------------------------------------------
# Regex include / exclude
# ---------------------------------------------------------------------------


def test_resolve_regex_include():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.frigate_cam1_skipped"),
        _make_entry("sensor.cpu_usage"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_REGEX_INCLUDE: [r"frigate.*_(fps|skipped)$"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam1_skipped" in result
    assert "sensor.cpu_usage" not in result


def test_resolve_regex_exclude_removes_from_candidates():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.frigate_cam1_fps_debug"),
        _make_entry("sensor.cpu_usage"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.frigate_*"],
        CONF_ENTITY_REGEX_EXCLUDE: ["_debug$"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam1_fps_debug" not in result
    assert "sensor.cpu_usage" not in result


def test_resolve_regex_exclude_can_remove_all():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.frigate_*"],
        CONF_ENTITY_REGEX_EXCLUDE: [r"sensor\.frigate"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == []


def test_resolve_invalid_regex_include_skipped(caplog):
    manager = _make_manager()
    reg = _make_registry(_make_entry("sensor.foo"))
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_REGEX_INCLUDE: ["[invalid"],
        CONF_KEEP_DAYS: 3,
        CONF_ENABLED: True,
    }
    # Should not raise; bad pattern is logged and skipped
    result = manager._resolve_entities(rule, reg)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Combined selectors — union of positives, then exclude
# ---------------------------------------------------------------------------


def test_resolve_combined_selectors():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps", platform="frigate"),
        _make_entry("sensor.esp_temp", platform="esphome"),
        _make_entry("sensor.esp_temp_debug", platform="esphome"),
        _make_entry("sensor.other"),
    )

    with patch(
        "custom_components.recorder_tuning.__init__.er.async_entries_for_device",
        return_value=[],
    ):
        rule = {
            CONF_RULE_NAME: "r",
            CONF_INTEGRATION_FILTER: ["frigate"],
            CONF_ENTITY_IDS: ["sensor.esp_temp"],
            CONF_ENTITY_REGEX_EXCLUDE: ["_debug$"],
            CONF_KEEP_DAYS: 7,
            CONF_ENABLED: True,
        }
        result = manager._resolve_entities(rule, reg)

    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.esp_temp" in result
    assert "sensor.esp_temp_debug" not in result
    assert "sensor.other" not in result


# ---------------------------------------------------------------------------
# Empty rule returns empty list
# ---------------------------------------------------------------------------


def test_resolve_no_selectors_returns_empty():
    manager = _make_manager()
    reg = _make_registry(_make_entry("sensor.foo"))
    rule = {CONF_RULE_NAME: "r", CONF_KEEP_DAYS: 7, CONF_ENABLED: True}
    result = manager._resolve_entities(rule, reg)
    assert result == []


# ---------------------------------------------------------------------------
# Disabled entities are included in every path — they may still hold
# pre-disable recorder history that the rule needs to purge.
# ---------------------------------------------------------------------------


def test_resolve_integration_filter_includes_disabled():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps", platform="frigate"),
        _make_entry("sensor.frigate_cam2_fps", platform="frigate", disabled=True),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["frigate"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam2_fps" in result


def test_resolve_glob_includes_disabled():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.frigate_cam2_fps", disabled=True),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.frigate_*"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam2_fps" in result


def test_resolve_regex_include_includes_disabled():
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.frigate_cam1_fps"),
        _make_entry("sensor.frigate_cam2_fps", disabled=True),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_REGEX_INCLUDE: [r"^sensor\.frigate_"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.frigate_cam1_fps" in result
    assert "sensor.frigate_cam2_fps" in result


def test_resolve_device_id_includes_disabled():
    """The device-id path passes include_disabled_entities=True to HA."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.cam1_fps", device_id="dev_abc"),
        _make_entry("sensor.cam1_broken", device_id="dev_abc", disabled=True),
    )

    with patch(
        "custom_components.recorder_tuning.__init__.er.async_entries_for_device",
    ) as mock_entries_for_device:
        mock_entries_for_device.return_value = [
            reg.entities["sensor.cam1_fps"],
            reg.entities["sensor.cam1_broken"],
        ]
        rule = {
            CONF_RULE_NAME: "r",
            CONF_DEVICE_IDS: ["dev_abc"],
            CONF_KEEP_DAYS: 5,
            CONF_ENABLED: True,
        }
        result = manager._resolve_entities(rule, reg)

        # Assert HA was asked for disabled entities too
        _, kwargs = mock_entries_for_device.call_args
        assert kwargs.get("include_disabled_entities") is True

    assert "sensor.cam1_fps" in result
    assert "sensor.cam1_broken" in result


def test_resolve_explicit_entity_id_honoured_when_disabled():
    """Explicit entity_ids work regardless of registry disabled state."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.foo", disabled=True),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_IDS: ["sensor.foo"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.foo" in result
