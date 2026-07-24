# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for RecorderTuningManager._resolve_entities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from custom_components.recorder_tuning.const import (
    CONF_DEVICE_IDS,
    CONF_DEVICE_INTEGRATION_FILTER,
    CONF_ENABLED,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_INTEGRATION_FILTER,
    CONF_KEEP_DAYS,
    CONF_MATCH_MODE,
    CONF_RULE_NAME,
    MATCH_MODE_ALL,
    MATCH_MODE_ANY,
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
    return RecorderTuningManager(mock_hass, {"rules": []})


def _hass_with_states(*entity_ids):
    """Return a MagicMock hass whose states.async_all() yields the given ids."""
    states = [MagicMock(entity_id=eid) for eid in entity_ids]
    hass = MagicMock()
    hass.states.async_all.return_value = states
    return hass


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


def test_resolve_regex_matches_unregistered_state_machine_entity():
    """Regex selector must find entities present only in the state machine.

    Covers yaml-configured MQTT sensors etc. that never enter the entity
    registry — the rule engine now unions registry + state machine when
    resolving regex/glob patterns.
    """
    hass = _hass_with_states(
        "sensor.container_foo_network_rx_total",  # unregistered, in state machine only
        "sensor.cpu_usage",  # unregistered, should NOT match regex
    )
    manager = _make_manager(hass=hass)
    reg = _make_registry()  # intentionally empty — prove we don't need a registry entry
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_REGEX_INCLUDE: [r"^sensor\.container_.*_total$"],
        CONF_KEEP_DAYS: 8,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.container_foo_network_rx_total" in result
    assert "sensor.cpu_usage" not in result


def test_resolve_glob_matches_unregistered_state_machine_entity():
    """Glob selector also sees state-machine-only entities."""
    hass = _hass_with_states(
        "sensor.container_bar_io_write_total",
        "sensor.cpu_usage",
    )
    manager = _make_manager(hass=hass)
    reg = _make_registry()
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.container_*_total"],
        CONF_KEEP_DAYS: 8,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert "sensor.container_bar_io_write_total" in result
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


# ---------------------------------------------------------------------------
# Combined selectors — "any" mode (union of positives, then exclude)
# ---------------------------------------------------------------------------


def test_resolve_combined_selectors_any_mode_unions():
    """Opt-in any mode keeps the legacy union-of-selectors behaviour."""
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
            CONF_MATCH_MODE: MATCH_MODE_ANY,
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
# match_mode: "all" is the default and computes set intersection over every
# present positive selector. This is the fix for the "ESPHome diagnostic
# sensors" family of rules where the author intended intersection.
# ---------------------------------------------------------------------------


def test_default_mode_is_all_integration_and_regex_intersect():
    """Without an explicit match_mode, integration_filter + regex is intersection."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.esp_voltage", platform="esphome"),
        _make_entry("sensor.esp_temperature", platform="esphome"),
        _make_entry("sensor.mqtt_voltage", platform="mqtt"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    # Only the ESPHome entity that also matches _voltage$
    assert result == ["sensor.esp_voltage"]


def test_match_mode_all_explicit_matches_default_behaviour():
    """Explicit match_mode=all produces the same result as the default."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.esp_voltage", platform="esphome"),
        _make_entry("sensor.esp_temperature", platform="esphome"),
        _make_entry("sensor.mqtt_voltage", platform="mqtt"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_MATCH_MODE: MATCH_MODE_ALL,
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.esp_voltage"]


def test_match_mode_all_three_selectors_all_must_match():
    """integration + glob + regex — all three must agree."""
    manager = _make_manager()
    reg = _make_registry(
        # Matches all three
        _make_entry("sensor.esp_workshop_voltage", platform="esphome"),
        # Matches glob + regex but wrong platform
        _make_entry("sensor.esp_workshop_voltage_mqtt", platform="mqtt"),
        # Matches platform + glob but not regex
        _make_entry("sensor.esp_workshop_temp", platform="esphome"),
        # Matches platform + regex but not glob
        _make_entry("sensor.esp_basement_voltage", platform="esphome"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_ENTITY_GLOBS: ["sensor.esp_workshop_*"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.esp_workshop_voltage"]


def test_match_mode_all_single_selector_behaves_like_any():
    """With one positive selector, all and any collapse to the same set."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.a", platform="esphome"),
        _make_entry("sensor.b", platform="mqtt"),
    )
    rule_all = {
        CONF_RULE_NAME: "r",
        CONF_MATCH_MODE: MATCH_MODE_ALL,
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    rule_any = dict(rule_all)
    rule_any[CONF_MATCH_MODE] = MATCH_MODE_ANY

    assert manager._resolve_entities(rule_all, reg) == manager._resolve_entities(
        rule_any, reg
    )
    assert manager._resolve_entities(rule_all, reg) == ["sensor.a"]


def test_match_mode_all_within_selector_values_still_or():
    """Within one selector, values OR; across selectors, AND."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.esp_voltage", platform="esphome"),
        _make_entry("sensor.mqtt_voltage", platform="mqtt"),
        _make_entry("sensor.mqtt_temp", platform="mqtt"),
        _make_entry("sensor.zha_voltage", platform="zha"),
    )
    # Platform is esphome OR mqtt, AND id ends in _voltage.
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["esphome", "mqtt"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.esp_voltage", "sensor.mqtt_voltage"]


def test_match_mode_all_exclude_still_applied_after_intersection():
    """entity_regex_exclude subtracts from the AND'd candidate set."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.esp_voltage", platform="esphome"),
        _make_entry("sensor.esp_voltage_debug", platform="esphome"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage"],
        CONF_ENTITY_REGEX_EXCLUDE: [r"_debug$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.esp_voltage"]


def test_match_mode_all_empty_intersection_yields_empty():
    """When no entity satisfies every selector, result is empty (not the universe)."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.esp_temp", platform="esphome"),
        _make_entry("sensor.mqtt_voltage", platform="mqtt"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["esphome"],
        CONF_ENTITY_REGEX_INCLUDE: [r"_voltage$"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    assert manager._resolve_entities(rule, reg) == []


def test_match_mode_all_device_and_regex_intersect():
    """device_ids intersected with entity_regex_include under all mode."""
    manager = _make_manager()
    reg = _make_registry(
        _make_entry("sensor.cam1_fps", device_id="dev_abc"),
        _make_entry("sensor.cam1_status", device_id="dev_abc"),
        _make_entry("sensor.cam2_fps", device_id="dev_xyz"),
    )
    with patch(
        "custom_components.recorder_tuning.__init__.er.async_entries_for_device",
        return_value=[
            reg.entities["sensor.cam1_fps"],
            reg.entities["sensor.cam1_status"],
        ],
    ):
        rule = {
            CONF_RULE_NAME: "r",
            CONF_DEVICE_IDS: ["dev_abc"],
            CONF_ENTITY_REGEX_INCLUDE: [r"_fps$"],
            CONF_KEEP_DAYS: 5,
            CONF_ENABLED: True,
        }
        result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.cam1_fps"]


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


def test_resolve_output_is_sorted():
    """Resolved entity_ids must be returned in sorted order (deterministic)."""
    manager = _make_manager()
    # Register entries in non-alphabetical order.
    reg = _make_registry(
        _make_entry("sensor.zeta"),
        _make_entry("sensor.alpha"),
        _make_entry("sensor.mu"),
        _make_entry("sensor.beta"),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_ENTITY_GLOBS: ["sensor.*"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.alpha", "sensor.beta", "sensor.mu", "sensor.zeta"]


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


# ---------------------------------------------------------------------------
# Device-integration filter — match by the integration that owns the entity's
# *device*, not the entity's own platform. The motivating case: a
# recorder_downsampler mirror sensor is created by recorder_downsampler but
# glued onto its source's device (e.g. greeneye_monitor), so it can only be
# isolated by combining its platform with its device's integration.
# ---------------------------------------------------------------------------


def _make_device(device_id: str, *config_entry_ids: str):
    device = MagicMock()
    device.id = device_id
    device.config_entries = set(config_entry_ids)
    return device


def _hass_with_devices(devices, entry_domains):
    """A hass whose config_entries.async_get_entry maps entry_id -> .domain."""

    def _get_entry(entry_id):
        domain = entry_domains.get(entry_id)
        if domain is None:
            return None
        return MagicMock(domain=domain)

    hass = MagicMock()
    hass.config_entries.async_get_entry.side_effect = _get_entry
    dev_reg = MagicMock()
    dev_reg.devices = {d.id: d for d in devices}
    return hass, dev_reg


def test_resolve_device_integration_filter_alone():
    """Matches every entity on a device owned by the named integration."""
    hass, dev_reg = _hass_with_devices(
        devices=[
            _make_device("dev_geye", "ce_geye", "ce_ds"),
            _make_device("dev_wled", "ce_wled", "ce_ds"),
        ],
        entry_domains={
            "ce_geye": "greeneye_monitor",
            "ce_wled": "wled",
            "ce_ds": "recorder_downsampler",
        },
    )
    manager = _make_manager(hass=hass)
    reg = _make_registry(
        _make_entry(
            "sensor.rack_power", platform="greeneye_monitor", device_id="dev_geye"
        ),
        _make_entry(
            "sensor.rack_power_recorded",
            platform="recorder_downsampler",
            device_id="dev_geye",
        ),
        _make_entry(
            "sensor.wled_rssi_recorded",
            platform="recorder_downsampler",
            device_id="dev_wled",
        ),
        _make_entry("sensor.no_device", platform="average", device_id=None),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_DEVICE_INTEGRATION_FILTER: ["greeneye_monitor"],
        CONF_KEEP_DAYS: 30,
        CONF_ENABLED: True,
    }
    with patch(
        "custom_components.recorder_tuning.__init__.dr.async_get",
        return_value=dev_reg,
    ):
        result = manager._resolve_entities(rule, reg)
    # Everything on the GreenEye device — the raw source AND its mirror — but
    # nothing on the WLED device or without a device.
    assert result == ["sensor.rack_power", "sensor.rack_power_recorded"]


def test_double_integration_filter_isolates_downsampler_mirrors():
    """integration_filter (platform) + device_integration_filter (device) under
    the default match_mode 'all' selects exactly the GreenEye mirrors."""
    hass, dev_reg = _hass_with_devices(
        devices=[
            _make_device("dev_geye", "ce_geye", "ce_ds"),
            _make_device("dev_wled", "ce_wled", "ce_ds"),
        ],
        entry_domains={
            "ce_geye": "greeneye_monitor",
            "ce_wled": "wled",
            "ce_ds": "recorder_downsampler",
        },
    )
    manager = _make_manager(hass=hass)
    reg = _make_registry(
        # the target: mirror created by recorder_downsampler, on a GreenEye device
        _make_entry(
            "sensor.rack_power_recorded",
            platform="recorder_downsampler",
            device_id="dev_geye",
        ),
        # raw GreenEye source: right device, wrong platform
        _make_entry(
            "sensor.rack_power", platform="greeneye_monitor", device_id="dev_geye"
        ),
        # mirror of a non-GreenEye source: right platform, wrong device
        _make_entry(
            "sensor.wled_rssi_recorded",
            platform="recorder_downsampler",
            device_id="dev_wled",
        ),
        # an 'average'-platform _recorded helper with no device
        _make_entry("sensor.average_temp_recorded", platform="average", device_id=None),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_INTEGRATION_FILTER: ["recorder_downsampler"],
        CONF_DEVICE_INTEGRATION_FILTER: ["greeneye_monitor"],
        CONF_KEEP_DAYS: 90,
        CONF_ENABLED: True,
    }
    with patch(
        "custom_components.recorder_tuning.__init__.dr.async_get",
        return_value=dev_reg,
    ):
        result = manager._resolve_entities(rule, reg)
    assert result == ["sensor.rack_power_recorded"]


def test_device_integration_filter_no_match_returns_empty():
    """A device-integration that owns no device yields no entities."""
    hass, dev_reg = _hass_with_devices(
        devices=[_make_device("dev_geye", "ce_geye")],
        entry_domains={"ce_geye": "greeneye_monitor"},
    )
    manager = _make_manager(hass=hass)
    reg = _make_registry(
        _make_entry(
            "sensor.rack_power", platform="greeneye_monitor", device_id="dev_geye"
        ),
    )
    rule = {
        CONF_RULE_NAME: "r",
        CONF_DEVICE_INTEGRATION_FILTER: ["nonexistent_integration"],
        CONF_KEEP_DAYS: 30,
        CONF_ENABLED: True,
    }
    with patch(
        "custom_components.recorder_tuning.__init__.dr.async_get",
        return_value=dev_reg,
    ):
        result = manager._resolve_entities(rule, reg)
    assert result == []
