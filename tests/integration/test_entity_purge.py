# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for per-entity purge rules.

Each test:
  1. Writes entity states stamped at a controlled past time via time-freezing.
  2. Configures one or more purge rules.
  3. Fires ``recorder_tuning.run_purge_now``.
  4. Queries the recorder SQLite DB directly to assert what survived.

Time model
----------
NOW        = 2026-04-04 12:00 UTC  (fixed anchor for all tests)
OLD_TIME   = NOW - 10 days         → older than PURGE_KEEP_DAYS (5), should be purgeable
RECENT_TIME = NOW - 3 days         → within PURGE_KEEP_DAYS, should be kept
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

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
    DOMAIN,
)

from .conftest import (
    NOW,
    PURGE_KEEP_DAYS,
    configure_rules,
    count_states,
    set_state_at,
    wait_for_recorder,
)

OLD_TIME = NOW - timedelta(days=10)
RECENT_TIME = NOW - timedelta(days=3)

# A keep_days value that makes OLD_TIME purgeable but RECENT_TIME safe.
KEEP_DAYS = PURGE_KEEP_DAYS - 1  # 4 — OLD_TIME (10d old) is beyond this


def _rule(**kwargs: Any) -> dict:
    """Build a rule dict with safe defaults for all optional fields."""
    defaults: dict = {
        CONF_RULE_NAME: "test_rule",
        CONF_ENTITY_GLOBS: [],
        CONF_ENTITY_IDS: [],
        CONF_DEVICE_IDS: [],
        CONF_INTEGRATION_FILTER: [],
        CONF_ENTITY_REGEX_INCLUDE: [],
        CONF_ENTITY_REGEX_EXCLUDE: [],
        CONF_KEEP_DAYS: KEEP_DAYS,
        CONF_ENABLED: True,
    }
    defaults.update(kwargs)
    return defaults


async def run_purge(hass: HomeAssistant) -> None:
    """Fire the service and wait for the recorder to finish."""
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await wait_for_recorder(hass)


# ---------------------------------------------------------------------------
# Glob selector
# ---------------------------------------------------------------------------


async def test_glob_old_states_purged(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """States matching a glob pattern, older than keep_days, are removed."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.target_power", "100", OLD_TIME, freezer)
    assert count_states(hass, "sensor.target_power") > 0

    await configure_rules(hass, [_rule(entity_globs=["sensor.target_*"])])
    await run_purge(hass)

    assert count_states(hass, "sensor.target_power") == 0


async def test_glob_recent_states_kept(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """States within keep_days are NOT removed even if the entity matches."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.target_power", "100", RECENT_TIME, freezer)
    assert count_states(hass, "sensor.target_power") > 0

    await configure_rules(
        hass,
        [_rule(entity_globs=["sensor.target_*"], keep_days=PURGE_KEEP_DAYS)],
    )
    await run_purge(hass)

    # RECENT_TIME is 3 days old; keep_days=5 → should survive
    assert count_states(hass, "sensor.target_power") > 0


async def test_glob_nonmatching_entities_untouched(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entities that do not match the glob are not purged, even if they're old."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.target_power", "100", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.control_voltage", "240", OLD_TIME, freezer)

    await configure_rules(hass, [_rule(entity_globs=["sensor.target_*"])])
    await run_purge(hass)

    assert count_states(hass, "sensor.target_power") == 0
    assert count_states(hass, "sensor.control_voltage") > 0


# ---------------------------------------------------------------------------
# Explicit entity-ID selector
# ---------------------------------------------------------------------------


async def test_explicit_entity_id_purged(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """An explicitly listed entity ID is purged; others are untouched."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.pinpoint", "42", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.bystander", "0", OLD_TIME, freezer)

    await configure_rules(hass, [_rule(entity_ids=["sensor.pinpoint"])])
    await run_purge(hass)

    assert count_states(hass, "sensor.pinpoint") == 0
    assert count_states(hass, "sensor.bystander") > 0


# ---------------------------------------------------------------------------
# Integration / platform filter
# ---------------------------------------------------------------------------


async def test_integration_filter_purges_by_platform(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """All entities from a specific integration platform are purged."""
    hass, _ = integration_entry
    # set_state_at registers the entity under the given platform automatically
    await set_state_at(
        hass, "sensor.fast_sensor", "100", OLD_TIME, freezer, platform="fast_platform"
    )
    await set_state_at(
        hass, "sensor.slow_sensor", "200", OLD_TIME, freezer, platform="slow_platform"
    )

    await configure_rules(hass, [_rule(integration_filter=["fast_platform"])])
    await run_purge(hass)

    assert count_states(hass, "sensor.fast_sensor") == 0
    assert count_states(hass, "sensor.slow_sensor") > 0


# ---------------------------------------------------------------------------
# Device-ID selector
# ---------------------------------------------------------------------------


async def test_device_id_purges_all_device_entities(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """All entities under a device ID are purged."""
    hass, _ = integration_entry
    ent_reg = er.async_get(hass)

    from homeassistant.helpers import device_registry as dr  # noqa: PLC0415
    from pytest_homeassistant_custom_component.common import MockConfigEntry  # noqa: PLC0415

    # Device registry requires a real config entry
    device_entry = MockConfigEntry(domain="test_device_platform")
    device_entry.add_to_hass(hass)

    device = dr.async_get(hass).async_get_or_create(
        config_entry_id=device_entry.entry_id,
        identifiers={("test_device_platform", "device_1")},
        name="Test Device",
    )
    ent_reg.async_get_or_create(
        "sensor",
        "test",
        "entity_1",
        device_id=device.id,
        suggested_object_id="device_sensor_a",
    )
    ent_reg.async_get_or_create(
        "sensor",
        "test",
        "entity_2",
        device_id=device.id,
        suggested_object_id="device_sensor_b",
    )
    ent_reg.async_get_or_create(
        "sensor",
        "test",
        "entity_3",
        suggested_object_id="unrelated_sensor",
    )
    await hass.async_block_till_done()

    # Entities already registered above with device_id; just write their states
    await set_state_at(
        hass, "sensor.device_sensor_a", "1", OLD_TIME, freezer, platform="test"
    )
    await set_state_at(
        hass, "sensor.device_sensor_b", "2", OLD_TIME, freezer, platform="test"
    )
    await set_state_at(
        hass, "sensor.unrelated_sensor", "3", OLD_TIME, freezer, platform="test"
    )

    await configure_rules(hass, [_rule(device_ids=[device.id])])
    await run_purge(hass)

    assert count_states(hass, "sensor.device_sensor_a") == 0
    assert count_states(hass, "sensor.device_sensor_b") == 0
    assert count_states(hass, "sensor.unrelated_sensor") > 0


# ---------------------------------------------------------------------------
# Regex include selector
# ---------------------------------------------------------------------------


async def test_regex_include_matches_pattern(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entities matching a regex include pattern are purged."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.frigate_cam1_fps", "30", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.frigate_cam1_skipped", "0", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.cpu_usage", "42", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [_rule(entity_regex_include=[r"frigate.*_(fps|skipped)$"])],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.frigate_cam1_fps") == 0
    assert count_states(hass, "sensor.frigate_cam1_skipped") == 0
    assert count_states(hass, "sensor.cpu_usage") > 0


# ---------------------------------------------------------------------------
# Regex exclude selector
# ---------------------------------------------------------------------------


async def test_regex_exclude_carves_out_exceptions(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Glob selects a broad set; regex_exclude removes a subset from purge."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.frigate_cam1_fps", "30", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.frigate_cam1_fps_debug", "30", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            _rule(
                entity_globs=["sensor.frigate_*"],
                entity_regex_exclude=["_debug$"],
            )
        ],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.frigate_cam1_fps") == 0
    assert count_states(hass, "sensor.frigate_cam1_fps_debug") > 0


# ---------------------------------------------------------------------------
# Disabled rule
# ---------------------------------------------------------------------------


async def test_disabled_rule_skips_purge(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with enabled=False does not purge anything."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.target_power", "100", OLD_TIME, freezer)
    assert count_states(hass, "sensor.target_power") > 0

    await configure_rules(
        hass,
        [_rule(entity_globs=["sensor.target_*"], enabled=False)],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.target_power") > 0


# ---------------------------------------------------------------------------
# Combined selectors
# ---------------------------------------------------------------------------


async def test_combined_glob_and_entity_id_union(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Glob and explicit entity_id together select the union of both sets."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.target_power", "100", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.extra_entity", "50", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.bystander", "0", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [_rule(entity_globs=["sensor.target_*"], entity_ids=["sensor.extra_entity"])],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.target_power") == 0
    assert count_states(hass, "sensor.extra_entity") == 0
    assert count_states(hass, "sensor.bystander") > 0


async def test_multiple_rules_run_independently(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Two rules each purge their own target group; unrelated entity is untouched."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.group_a_temp", "20", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.group_b_humidity", "50", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.untouched", "0", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [
            _rule(name="rule_a", entity_globs=["sensor.group_a_*"]),
            _rule(name="rule_b", entity_globs=["sensor.group_b_*"]),
        ],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.group_a_temp") == 0
    assert count_states(hass, "sensor.group_b_humidity") == 0
    assert count_states(hass, "sensor.untouched") > 0


async def test_partial_purge_mixed_state_ages(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entity with old and recent states: only the old states are purged."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.mixed_age", "old_val", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.mixed_age", "recent_val", RECENT_TIME, freezer)
    assert count_states(hass, "sensor.mixed_age") == 2

    await configure_rules(
        hass,
        [_rule(entity_globs=["sensor.mixed_age"], keep_days=KEEP_DAYS)],
    )
    await run_purge(hass)

    # OLD_TIME state (10d) is beyond KEEP_DAYS (4d) → purged
    # RECENT_TIME state (3d) is within KEEP_DAYS (4d) → kept
    assert count_states(hass, "sensor.mixed_age") == 1


async def test_rule_with_no_positive_selectors_skipped(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """A rule with all-empty selectors matches nothing and does not crash."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.important", "99", OLD_TIME, freezer)

    await configure_rules(hass, [_rule()])  # all selectors empty by default
    await run_purge(hass)

    assert count_states(hass, "sensor.important") > 0


async def test_large_batch_all_entities_purged(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """105 matching entities are all purged, exercising the 100-entity batch limit."""
    hass, _ = integration_entry
    ent_reg = er.async_get(hass)

    entity_ids = [f"sensor.bulk_{i:03d}" for i in range(105)]

    # Register all entities, then write their states in a single batch
    for eid in entity_ids:
        _, _, uid = eid.partition(".")
        ent_reg.async_get_or_create(
            "sensor", "test_integration", uid, suggested_object_id=uid
        )
    await hass.async_block_till_done()

    freezer.move_to(OLD_TIME)
    for eid in entity_ids:
        hass.states.async_set(eid, "1")
    await wait_for_recorder(hass)
    freezer.move_to(NOW)

    await configure_rules(hass, [_rule(entity_globs=["sensor.bulk_*"])])
    await run_purge(hass)

    for eid in entity_ids:
        assert count_states(hass, eid) == 0, f"{eid} was not purged"


async def test_integration_filter_and_regex_exclude_combined(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Integration filter selects a platform; regex_exclude removes debug variants."""
    hass, _ = integration_entry

    await set_state_at(
        hass, "sensor.cam_fps", "30", OLD_TIME, freezer, platform="fast_platform"
    )
    await set_state_at(
        hass, "sensor.cam_fps_debug", "0", OLD_TIME, freezer, platform="fast_platform"
    )
    await set_state_at(
        hass, "sensor.other_sensor", "1", OLD_TIME, freezer, platform="slow_platform"
    )

    await configure_rules(
        hass,
        [
            _rule(
                integration_filter=["fast_platform"],
                entity_regex_exclude=["_debug$"],
            )
        ],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.cam_fps") == 0
    assert count_states(hass, "sensor.cam_fps_debug") > 0
    assert count_states(hass, "sensor.other_sensor") > 0


async def test_multiple_regex_include_patterns_union(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Multiple regex_include patterns are OR-unioned: any match is sufficient."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.motion_zone_a", "on", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.motion_zone_b", "on", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.temperature_living", "21", OLD_TIME, freezer)
    await set_state_at(hass, "sensor.humidity_bedroom", "45", OLD_TIME, freezer)

    await configure_rules(
        hass,
        [_rule(entity_regex_include=[r"sensor\.motion_", r"sensor\.temperature_"])],
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.motion_zone_a") == 0
    assert count_states(hass, "sensor.motion_zone_b") == 0
    assert count_states(hass, "sensor.temperature_living") == 0
    assert count_states(hass, "sensor.humidity_bedroom") > 0


# ---------------------------------------------------------------------------
# Service API: add_rule / remove_rule
# ---------------------------------------------------------------------------


async def test_add_rule_service_creates_active_rule(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """add_rule service creates a rule that is immediately active on next purge."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.service_target", "42", OLD_TIME, freezer)

    await hass.services.async_call(
        DOMAIN,
        "add_rule",
        {
            CONF_RULE_NAME: "svc_rule",
            CONF_ENTITY_GLOBS: ["sensor.service_target"],
            CONF_KEEP_DAYS: KEEP_DAYS,
        },
        blocking=True,
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.service_target") == 0


async def test_remove_rule_service_deactivates_rule(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """remove_rule service removes the rule; subsequent purge leaves the entity intact."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.guarded_sensor", "1", OLD_TIME, freezer)

    await hass.services.async_call(
        DOMAIN,
        "add_rule",
        {
            CONF_RULE_NAME: "temp_rule",
            CONF_ENTITY_GLOBS: ["sensor.guarded_*"],
            CONF_KEEP_DAYS: KEEP_DAYS,
        },
        blocking=True,
    )
    await hass.services.async_call(
        DOMAIN, "remove_rule", {CONF_RULE_NAME: "temp_rule"}, blocking=True
    )
    await run_purge(hass)

    assert count_states(hass, "sensor.guarded_sensor") > 0


async def test_add_rule_service_updates_keep_days(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Re-adding a rule with the same name updates keep_days; new value takes effect."""
    hass, _ = integration_entry

    await set_state_at(hass, "sensor.updateable", "5", OLD_TIME, freezer)

    # First add: aggressive keep_days → entity is purgeable (state is 10d old)
    await hass.services.async_call(
        DOMAIN,
        "add_rule",
        {
            CONF_RULE_NAME: "update_test",
            CONF_ENTITY_GLOBS: ["sensor.updateable"],
            CONF_KEEP_DAYS: KEEP_DAYS,  # 4 days — OLD_TIME (10d) is beyond this
        },
        blocking=True,
    )
    # Update: conservative keep_days → entity is now within the window
    await hass.services.async_call(
        DOMAIN,
        "add_rule",
        {
            CONF_RULE_NAME: "update_test",
            CONF_ENTITY_GLOBS: ["sensor.updateable"],
            CONF_KEEP_DAYS: 15,  # 15 days — OLD_TIME (10d) is within this
        },
        blocking=True,
    )
    await run_purge(hass)

    # keep_days=15, state is 10d old → must survive
    assert count_states(hass, "sensor.updateable") > 0
