# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for YAML-based configuration and the reload service.

The integration is configured via a top-level ``recorder_tuning:`` block in
``configuration.yaml`` (typically with ``rules: !include ...``). These tests
write ``configuration.yaml`` into the HA config dir, call the reload
service, and verify that the new rules take effect end-to-end.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.recorder_tuning.const import DOMAIN

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
# Fixture: write configuration.yaml in the HA config dir and clean up
# ---------------------------------------------------------------------------


@pytest.fixture
def yaml_config(integration_entry: tuple[HomeAssistant, Any]):
    """Provide a helper that writes configuration.yaml with recorder_tuning:."""
    hass, _ = integration_entry
    config_path = hass.config.path("configuration.yaml")

    def _write_rules(rules_yaml: str, *, dry_run: bool = False) -> str:
        """Write configuration.yaml with a recorder_tuning: block.

        ``rules_yaml`` is spliced in verbatim under ``rules:`` so tests can
        write invalid YAML to exercise error handling.
        """
        with open(config_path, "w", encoding="utf-8") as fh:
            fh.write(
                "recorder_tuning:\n"
                '  purge_time: "03:00"\n'
                "  stats_keep_days: 30\n"
                f"  dry_run: {'true' if dry_run else 'false'}\n"
                # Match the conftest default — isolate per-entity rule
                # behavior from the trailing global purge sweep.
                "  ha_recorder_purge:\n"
                "    enabled: false\n"
                f"{rules_yaml}"
            )
        return config_path

    yield hass, _write_rules

    if os.path.isfile(config_path):
        os.remove(config_path)


async def run_purge(hass: HomeAssistant) -> None:
    """Fire the service and wait for the recorder to finish."""
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)


# ---------------------------------------------------------------------------
# Reload hot-swaps rules from configuration.yaml
# ---------------------------------------------------------------------------


async def test_reload_service_picks_up_config_changes(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Editing configuration.yaml and calling reload hot-swaps the rules."""
    hass, write_rules = yaml_config

    await set_state_at(hass, "sensor.first_target", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.second_target", "2", OLD_TIME, freezer)

    write_rules(f"""  rules:
    - name: rule_v1
      entity_ids: [sensor.first_target]
      keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.first_target") == 0
    assert count_states(hass, "sensor.second_target") > 0

    await set_state_at(hass, "sensor.first_target", "new", OLD_TIME, freezer)

    write_rules(f"""  rules:
    - name: rule_v2
      entity_ids: [sensor.second_target]
      keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.second_target") == 0
    # first_target state written above is NOT targeted by v2 — must survive
    assert count_states(hass, "sensor.first_target") > 0


async def test_reload_with_multiple_rules_all_run(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Multiple rules after reload all run independently."""
    hass, write_rules = yaml_config

    await set_state_at(hass, "sensor.group_a", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.group_b", "2", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.untouched", "3", OLD_TIME, freezer)

    write_rules(f"""  rules:
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


async def test_reload_disabled_rule_skipped(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with enabled: false is loaded but does not purge."""
    hass, write_rules = yaml_config

    await set_state_at(hass, "sensor.disabled_target", "1", OLD_TIME, freezer)

    write_rules(f"""  rules:
    - name: disabled_rule
      entity_ids: [sensor.disabled_target]
      keep_days: {KEEP_DAYS}
      enabled: false
""")

    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)
    await run_purge(hass)

    assert count_states(hass, "sensor.disabled_target") > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


async def test_invalid_yaml_syntax_reload_raises_and_preserves_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A YAML parse error surfaces as a service failure; previous rules survive."""
    hass, write_rules = yaml_config

    await set_state_at(hass, "sensor.kept_entity", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.other_entity", "2", OLD_TIME, freezer)

    # Seed a valid rule first so there is state to preserve
    write_rules(f"""  rules:
    - name: guard
      entity_ids: [sensor.kept_entity]
      keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Now write a broken file and reload — the service call must raise
    write_rules("  rules: [invalid: yaml: {{broken")
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Rule from before the broken reload is still active
    await run_purge(hass)
    assert count_states(hass, "sensor.kept_entity") == 0
    assert count_states(hass, "sensor.other_entity") > 0


async def test_invalid_rule_schema_reload_raises_and_preserves_rules(
    yaml_config: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A schema error in a rule aborts the whole reload (atomic)."""
    hass, write_rules = yaml_config

    await set_state_at(hass, "sensor.kept_entity", "1", OLD_TIME, freezer)

    # Seed a valid rule first
    write_rules(f"""  rules:
    - name: guard
      entity_ids: [sensor.kept_entity]
      keep_days: {KEEP_DAYS}
""")
    await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Now attempt to reload with a rule that exceeds keep_days max
    write_rules("""  rules:
    - name: bad_rule
      entity_ids: [sensor.kept_entity]
      keep_days: 999999
""")
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(DOMAIN, "reload", {}, blocking=True)

    # Prior valid rule is still active
    await run_purge(hass)
    assert count_states(hass, "sensor.kept_entity") == 0
