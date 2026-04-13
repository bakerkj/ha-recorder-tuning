# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for YAML-based rule configuration.

These tests verify that rules loaded from ``recorder_tuning.yaml`` in the HA
config directory take effect correctly, override stored rules, survive a reload,
fall back gracefully on errors, and block add_rule/remove_rule service calls
while a YAML file is active.

The YAML file is written into the real HA config directory (``hass.config.config_dir``)
so that ``_load_yaml_rules`` finds it via ``hass.config.path()``.  Each test
cleans up the file after itself via a fixture.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pytest
from homeassistant.core import HomeAssistant

from custom_components.recorder_tuning.const import (
    CONF_ENTITY_IDS,
    CONF_KEEP_DAYS,
    CONF_RULE_NAME,
    DOMAIN,
    YAML_CONFIG_FILE,
)

from .conftest import (
    NOW,
    configure_rules,
    count_states,
    set_state_at,
    wait_for_recorder,
)

OLD_TIME = NOW - timedelta(days=10)
RECENT_TIME = NOW - timedelta(days=3)
KEEP_DAYS = 4  # OLD_TIME (10d) is beyond this; RECENT_TIME (3d) is within


# ---------------------------------------------------------------------------
# Fixture: write / clean up recorder_tuning.yaml in HA config dir
# ---------------------------------------------------------------------------


@pytest.fixture
def yaml_config(integration_entry: tuple[HomeAssistant, Any]):
    """Provide a helper that writes recorder_tuning.yaml and cleans up after."""
    hass, _ = integration_entry
    yaml_path = hass.config.path(YAML_CONFIG_FILE)

    def _write(content: str) -> str:
        with open(yaml_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return yaml_path

    yield hass, _write

    # Cleanup — remove the file so it doesn't bleed into other tests
    if os.path.isfile(yaml_path):
        os.remove(yaml_path)


async def run_purge(hass: HomeAssistant) -> None:
    """Fire the service and wait for the recorder to finish."""
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)


# ---------------------------------------------------------------------------
# Basic YAML loading
# ---------------------------------------------------------------------------


async def test_yaml_rules_loaded_at_setup(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Rules in recorder_tuning.yaml are active immediately after reload."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.yaml_target", "1", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: yaml_rule
    entity_ids: [sensor.yaml_target]
    keep_days: {KEEP_DAYS}
""")

    # Trigger reload so the manager picks up the new file
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.yaml_target") == 0


async def test_yaml_rules_replace_stored_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """YAML rules replace (not merge with) stored rules."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.stored_only", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.yaml_only", "2", OLD_TIME, freezer)

    # Store a rule for sensor.stored_only
    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "stored_rule",
                "entity_globs": [],
                CONF_ENTITY_IDS: ["sensor.stored_only"],
                "device_ids": [],
                "integration_filter": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                "enabled": True,
            }
        ],
    )

    # Write YAML with a different rule
    write_yaml(f"""
rules:
  - name: yaml_rule
    entity_ids: [sensor.yaml_only]
    keep_days: {KEEP_DAYS}
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # Only the YAML rule is active — stored rule is suppressed
    assert count_states(hass, "sensor.yaml_only") == 0
    assert count_states(hass, "sensor.stored_only") > 0


async def test_yaml_multiple_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Multiple rules in the YAML file all run independently."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.group_a", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.group_b", "2", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.untouched", "3", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: rule_a
    entity_ids: [sensor.group_a]
    keep_days: {KEEP_DAYS}
  - name: rule_b
    entity_ids: [sensor.group_b]
    keep_days: {KEEP_DAYS}
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.group_a") == 0
    assert count_states(hass, "sensor.group_b") == 0
    assert count_states(hass, "sensor.untouched") > 0


async def test_yaml_glob_selector(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Glob patterns in YAML rules work the same as via the service API."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.yaml_power_a", "10", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.yaml_power_b", "20", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.other_sensor", "5", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: glob_rule
    entity_globs: ["sensor.yaml_power_*"]
    keep_days: {KEEP_DAYS}
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.yaml_power_a") == 0
    assert count_states(hass, "sensor.yaml_power_b") == 0
    assert count_states(hass, "sensor.other_sensor") > 0


async def test_yaml_disabled_rule_skipped(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with enabled: false in YAML does not purge."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.yaml_disabled_target", "1", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: disabled_rule
    entity_ids: [sensor.yaml_disabled_target]
    keep_days: {KEEP_DAYS}
    enabled: false
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.yaml_disabled_target") > 0


# ---------------------------------------------------------------------------
# Hot reload
# ---------------------------------------------------------------------------


async def test_reload_service_picks_up_file_changes(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Editing the YAML file and calling reload hot-swaps the rules."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.first_target", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.second_target", "2", OLD_TIME, freezer)

    # First version of the file — only targets sensor.first_target
    write_yaml(f"""
rules:
  - name: rule_v1
    entity_ids: [sensor.first_target]
    keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.first_target") == 0
    assert count_states(hass, "sensor.second_target") > 0

    # Now set the second target's state again so it's purgeable
    await set_state_at(hass, "sensor.first_target", "new", OLD_TIME, freezer)

    # Edit the file — now targets sensor.second_target
    write_yaml(f"""
rules:
  - name: rule_v2
    entity_ids: [sensor.second_target]
    keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.second_target") == 0
    # first_target state written above is NOT targeted by v2 — must survive
    assert count_states(hass, "sensor.first_target") > 0


async def test_reload_without_file_reverts_to_stored_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Removing the YAML file and calling reload reverts to stored rules."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.stored_target", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.yaml_target", "2", OLD_TIME, freezer)

    # Stored rule targets sensor.stored_target
    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "stored_rule",
                "entity_globs": [],
                CONF_ENTITY_IDS: ["sensor.stored_target"],
                "device_ids": [],
                "integration_filter": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                "enabled": True,
            }
        ],
    )

    # Write YAML targeting a different entity
    yaml_path = write_yaml(f"""
rules:
  - name: yaml_rule
    entity_ids: [sensor.yaml_target]
    keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Delete the file and reload — should fall back to stored rules
    os.remove(yaml_path)
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # Stored rule is back in effect
    assert count_states(hass, "sensor.stored_target") == 0
    assert count_states(hass, "sensor.yaml_target") > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


async def test_invalid_yaml_syntax_falls_back_to_stored_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A YAML parse error leaves existing rules unchanged."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.safe_entity", "1", OLD_TIME, freezer)

    # Put a good stored rule in place first
    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "safe_rule",
                "entity_globs": ["sensor.safe_*"],
                CONF_ENTITY_IDS: [],
                "device_ids": [],
                "integration_filter": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                "enabled": True,
            }
        ],
    )

    # Write broken YAML
    write_yaml("rules: [invalid: yaml: {{broken")

    # Reload — should log an error and fall back to stored rules
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # Stored rule still active — sensor was purged
    assert count_states(hass, "sensor.safe_entity") == 0


async def test_invalid_rule_schema_skips_bad_rule_keeps_good(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with a schema error is skipped; other valid rules still run."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.good_target", "1", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: bad_rule
    keep_days: 999999   # exceeds max(365) — invalid
    entity_ids: [sensor.should_not_matter]
  - name: good_rule
    entity_ids: [sensor.good_target]
    keep_days: {KEEP_DAYS}
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # good_rule ran despite bad_rule being skipped
    assert count_states(hass, "sensor.good_target") == 0


async def test_missing_required_field_skips_rule(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule missing required keep_days is skipped without crashing."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.no_keep_days", "1", OLD_TIME, freezer)

    write_yaml("""
rules:
  - name: missing_keep_days
    entity_ids: [sensor.no_keep_days]
    # keep_days intentionally omitted — should fail validation
""")

    # Should not raise; the rule is simply skipped
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # No rule ran — entity should be untouched
    assert count_states(hass, "sensor.no_keep_days") > 0


# ---------------------------------------------------------------------------
# add_rule / remove_rule blocked when YAML is active
# ---------------------------------------------------------------------------


async def test_add_rule_blocked_when_yaml_active(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """add_rule service call is silently ignored when YAML rules are active."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.service_add_target", "1", OLD_TIME, freezer)

    # YAML file with no rules — nothing will be purged
    write_yaml("rules: []")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Try to add a rule via service — should be ignored
    await hass.services.async_call(
        DOMAIN,
        "add_rule",
        {
            CONF_RULE_NAME: "blocked_rule",
            CONF_ENTITY_IDS: ["sensor.service_add_target"],
            CONF_KEEP_DAYS: KEEP_DAYS,
        },
        blocking=True,
    )
    await run_purge(hass)

    # Entity must be untouched because the service call was ignored
    assert count_states(hass, "sensor.service_add_target") > 0


async def test_remove_rule_blocked_when_yaml_active(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """remove_rule service call is silently ignored when YAML rules are active."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.yaml_remove_target", "1", OLD_TIME, freezer)

    write_yaml(f"""
rules:
  - name: active_yaml_rule
    entity_ids: [sensor.yaml_remove_target]
    keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Try to remove the rule — should be ignored
    await hass.services.async_call(
        DOMAIN, "remove_rule", {CONF_RULE_NAME: "active_yaml_rule"}, blocking=True
    )
    await run_purge(hass)

    # Rule is still active — entity was purged
    assert count_states(hass, "sensor.yaml_remove_target") == 0
