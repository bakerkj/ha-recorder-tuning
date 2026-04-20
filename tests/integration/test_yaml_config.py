# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for YAML-based rule configuration.

Rules are loaded exclusively from ``recorder_tuning.yaml`` in the HA config
directory. These tests verify that rules take effect at setup, survive a
reload after edits, fail gracefully on missing or malformed files, and
skip individual rules with schema errors without aborting the rest.

The YAML file is written into the real HA config directory
(``hass.config.config_dir``) so that ``_load_yaml_rules`` finds it via
``hass.config.path()``.  Each test cleans up the file after itself via a
fixture.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pytest
from homeassistant.core import HomeAssistant

from custom_components.recorder_tuning.const import (
    DOMAIN,
    YAML_CONFIG_FILE,
)

from .conftest import (
    NOW,
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

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.yaml_target") == 0


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
    """Glob patterns in YAML rules are honoured."""
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

    await set_state_at(hass, "sensor.first_target", "new", OLD_TIME, freezer)

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


async def test_reload_without_file_runs_zero_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Removing the YAML file and reloading deactivates all rules."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.was_yaml", "1", OLD_TIME, freezer)

    yaml_path = write_yaml(f"""
rules:
  - name: yaml_rule
    entity_ids: [sensor.was_yaml]
    keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Delete the file and reload — no rules should remain active
    os.remove(yaml_path)
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # Nothing purged — no rules were active
    assert count_states(hass, "sensor.was_yaml") > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


async def test_invalid_yaml_syntax_results_in_zero_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A YAML parse error is logged and leaves zero rules active."""
    hass, write_yaml = yaml_config

    await set_state_at(hass, "sensor.safe_entity", "1", OLD_TIME, freezer)

    write_yaml("rules: [invalid: yaml: {{broken")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # No rule was loaded — entity untouched
    assert count_states(hass, "sensor.safe_entity") > 0


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

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    # No rule ran — entity untouched
    assert count_states(hass, "sensor.no_keep_days") > 0
