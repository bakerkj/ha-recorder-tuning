# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for dry-run mode.

These tests verify that:
- dry_run=True logs what would be deleted without touching any data
- dry_run=False (default) still deletes data as normal
- The log messages contain entity ID and time range information
- Rules that match no purgeable data log "nothing to purge"
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import patch

from homeassistant.core import HomeAssistant

from custom_components.recorder_tuning.const import (
    CONF_DEVICE_IDS,
    CONF_DRY_RUN,
    CONF_ENABLED,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_INTEGRATION_FILTER,
    CONF_KEEP_DAYS,
    CONF_RULE_NAME,
    DOMAIN,
)

from .conftest import (
    NOW,
    configure_rules,
    count_states,
    set_dry_run,
    set_state_at,
    wait_for_recorder,
)

OLD_TIME = NOW - timedelta(days=10)
RECENT_TIME = NOW - timedelta(days=3)
KEEP_DAYS = 5  # OLD_TIME (10d) is beyond this; RECENT_TIME (3d) is within


async def run_dry_run(hass: HomeAssistant) -> None:
    """Call run_purge_now with dry_run=True and wait for recorder."""
    await hass.services.async_call(
        DOMAIN, "run_purge_now", {CONF_DRY_RUN: True}, blocking=True
    )
    await wait_for_recorder(hass)


async def run_purge(hass: HomeAssistant) -> None:
    """Call run_purge_now normally and wait for recorder."""
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)


# ---------------------------------------------------------------------------
# Core behaviour: dry-run does not delete
# ---------------------------------------------------------------------------


async def test_dry_run_does_not_delete_states(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """dry_run=True must not remove any states from the DB."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.dry_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "dry_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.dry_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    await run_dry_run(hass)

    # State must still exist — dry run must not have deleted it
    assert count_states(hass, "sensor.dry_target") > 0


async def test_normal_purge_still_deletes(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """dry_run=False (default) must continue to delete states as normal."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.normal_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "normal_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.normal_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    await run_purge(hass)

    assert count_states(hass, "sensor.normal_target") == 0


async def test_dry_run_then_purge_deletes(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A dry run followed by a real purge correctly deletes the data."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.staged_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "staged_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.staged_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    await run_dry_run(hass)
    # Still present after dry run
    assert count_states(hass, "sensor.staged_target") > 0

    await run_purge(hass)
    # Gone after real purge
    assert count_states(hass, "sensor.staged_target") == 0


# ---------------------------------------------------------------------------
# Log content
# ---------------------------------------------------------------------------


async def test_dry_run_logs_entity_id(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Dry run must log the entity ID of every entity with purgeable data."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.log_target", "99", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "log_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.log_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    with patch.object(
        __import__(
            "custom_components.recorder_tuning",
            fromlist=["_LOGGER"],
        ),
        "_LOGGER",
    ) as mock_logger:
        await run_dry_run(hass)

    logged = " ".join(
        str(a) for call in mock_logger.info.call_args_list for a in call.args
    )
    assert "sensor.log_target" in logged


async def test_dry_run_logs_nothing_to_purge_when_data_within_window(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """When all data is within keep_days, dry run must log 'nothing to purge'."""
    hass, _ = integration_entry

    # RECENT_TIME (3d ago) is within KEEP_DAYS (5d) — nothing purgeable
    await set_state_at(hass, "sensor.fresh_target", "42", RECENT_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "fresh_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.fresh_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    import custom_components.recorder_tuning as rt_module

    with patch.object(rt_module, "_LOGGER") as mock_logger:
        await run_dry_run(hass)

    logged = " ".join(
        str(a) for call in mock_logger.info.call_args_list for a in call.args
    )
    assert "nothing to purge" in logged


# ---------------------------------------------------------------------------
# Multiple rules
# ---------------------------------------------------------------------------


async def test_dry_run_covers_all_rules(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Dry run iterates every rule, not just the first."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.multi_a", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.multi_b", "2", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "rule_a",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.multi_a"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            },
            {
                CONF_RULE_NAME: "rule_b",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.multi_b"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            },
        ],
    )

    await run_dry_run(hass)

    # Neither entity should have been deleted
    assert count_states(hass, "sensor.multi_a") > 0
    assert count_states(hass, "sensor.multi_b") > 0


# ---------------------------------------------------------------------------
# Boundary: data exactly at the cutoff
# ---------------------------------------------------------------------------


async def test_dry_run_mixed_ages(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Dry run on an entity with both old and recent states only counts old rows."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.mixed_age", "old", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.mixed_age", "new", RECENT_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "mixed_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.mixed_age"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    import custom_components.recorder_tuning as rt_module

    with patch.object(rt_module, "_LOGGER") as mock_logger:
        await run_dry_run(hass)

    # The entity should appear in the dry-run log (it has purgeable rows)
    logged = " ".join(
        str(a) for call in mock_logger.info.call_args_list for a in call.args
    )
    assert "sensor.mixed_age" in logged

    # Nothing was actually deleted
    assert count_states(hass, "sensor.mixed_age") == 2


# ---------------------------------------------------------------------------
# Config-entry-driven dry-run (nightly timer behaviour)
# ---------------------------------------------------------------------------


async def test_config_dry_run_prevents_nightly_delete(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """When dry_run=True in config, the nightly timer must not delete data."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.nightly_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "nightly_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.nightly_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    set_dry_run(hass, True)

    # Simulate the nightly timer firing — no dry_run arg in service call
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)

    # Data must be intact — nightly timer ran in dry-run mode
    assert count_states(hass, "sensor.nightly_target") > 0


async def test_service_inherits_config_dry_run(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Service call without explicit dry_run inherits the config entry setting."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.inherit_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "inherit_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.inherit_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    set_dry_run(hass, True)

    # Service call with no dry_run arg — should inherit True from config
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)

    assert count_states(hass, "sensor.inherit_target") > 0


async def test_service_explicit_false_overrides_config_dry_run(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Explicit dry_run=False overrides a True config entry setting."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.override_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "override_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.override_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    set_dry_run(hass, True)

    # Explicit False forces a real purge despite config saying True
    await hass.services.async_call(
        DOMAIN, "run_purge_now", {CONF_DRY_RUN: False}, blocking=True
    )
    await wait_for_recorder(hass)

    assert count_states(hass, "sensor.override_target") == 0


async def test_config_dry_run_off_allows_delete(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """When dry_run=False in config, the nightly timer deletes normally."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.live_target", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "live_rule",
                CONF_ENTITY_GLOBS: [],
                CONF_ENTITY_IDS: ["sensor.live_target"],
                CONF_DEVICE_IDS: [],
                CONF_INTEGRATION_FILTER: [],
                CONF_ENTITY_REGEX_INCLUDE: [],
                CONF_ENTITY_REGEX_EXCLUDE: [],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_ENABLED: True,
            }
        ],
    )

    set_dry_run(hass, False)

    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)

    assert count_states(hass, "sensor.live_target") == 0


# ---------------------------------------------------------------------------
# Per-rule dry_run override
# ---------------------------------------------------------------------------


async def test_per_rule_dry_run_true_prevents_delete_when_run_live(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with dry_run=true must stay dry even when the run is live."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.rule_dry", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "rule_dry",
                CONF_ENTITY_IDS: ["sensor.rule_dry"],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_DRY_RUN: True,
            }
        ],
    )

    set_dry_run(hass, False)

    await run_purge(hass)

    # Rule override forced dry-run — entity must be intact
    assert count_states(hass, "sensor.rule_dry") > 0


async def test_per_rule_dry_run_false_forces_delete_when_run_dry(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with dry_run=false must delete even when the run is dry."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.rule_live", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "rule_live",
                CONF_ENTITY_IDS: ["sensor.rule_live"],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_DRY_RUN: False,
            }
        ],
    )

    set_dry_run(hass, True)

    # Service call with no dry_run arg — inherits True from config, but the
    # per-rule override should flip this rule back to live.
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)

    assert count_states(hass, "sensor.rule_live") == 0


async def test_per_rule_override_mixed_in_same_run(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Two rules in one run — one live, one overridden to dry — behave independently."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.mixed_live", "1", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.mixed_dry", "1", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "mixed_live_rule",
                CONF_ENTITY_IDS: ["sensor.mixed_live"],
                CONF_KEEP_DAYS: KEEP_DAYS,
            },
            {
                CONF_RULE_NAME: "mixed_dry_rule",
                CONF_ENTITY_IDS: ["sensor.mixed_dry"],
                CONF_KEEP_DAYS: KEEP_DAYS,
                CONF_DRY_RUN: True,
            },
        ],
    )

    set_dry_run(hass, False)

    await run_purge(hass)

    assert count_states(hass, "sensor.mixed_live") == 0
    assert count_states(hass, "sensor.mixed_dry") > 0
