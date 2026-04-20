# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for short-term statistics retention.

These tests verify that the monkey-patch applied by the recorder_tuning
integration causes ``statistics_short_term`` rows to survive recorder purge
cycles for longer than the raw ``states`` table.

Time model
----------
NOW              = 2026-04-04 12:00 UTC
PURGE_KEEP_DAYS  = 5   (recorder global setting — raw states older than this are purged)
STATS_KEEP_DAYS  = 30  (our extension — short-term stats older than this are purged)

                   ← stats_keep_days (30d) →
  |----|----|----|----|----|----|----+
  T-35d                        T-5d NOW
       ↑                  ↑
  beyond 30d          beyond 5d but within 30d
  → stats purged      → states purged; stats survive
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.recorder_tuning.const import (
    CONF_ENTITY_IDS,
    CONF_KEEP_DAYS,
    CONF_RULE_NAME,
    DOMAIN,
)

from .conftest import (
    NOW,
    PURGE_KEEP_DAYS,
    STATS_KEEP_DAYS,
    configure_rules,
    count_long_term_stats,
    count_short_term_stats,
    count_states,
    insert_long_term_stat,
    insert_short_term_stat,
    set_state_at,
    wait_for_purge,
)


async def run_recorder_purge(hass: HomeAssistant) -> None:
    """Trigger the recorder's own purge cycle and wait for completion."""
    await hass.services.async_call(
        "recorder",
        "purge",
        {"keep_days": PURGE_KEEP_DAYS, "repack": False},
        blocking=True,
    )
    await wait_for_purge(hass)


# ---------------------------------------------------------------------------
# Core behaviour: stats outlive states
# ---------------------------------------------------------------------------


async def test_short_term_stats_survive_state_purge(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Short-term stats at T-10d survive a purge that removes states at T-10d.

    Without the patch:   stats at T-10d would be purged (10 > purge_keep_days=5).
    With the patch:      stats at T-10d survive  (10 < stats_keep_days=30).
    States at T-10d:     always purged            (10 > purge_keep_days=5).
    """
    hass, _ = integration_entry

    stat_id = "sensor.energy_meter"
    entity_id = "sensor.energy_meter"
    ten_days_ago = NOW - timedelta(days=10)

    # Write a state and a short-term stat both stamped 10 days ago
    await set_state_at(hass, entity_id, "42", ten_days_ago, freezer)
    insert_short_term_stat(hass, stat_id, ten_days_ago)

    assert count_states(hass, entity_id) > 0
    assert count_short_term_stats(hass, stat_id) > 0

    # Run the recorder purge (keep_days=5)
    await run_recorder_purge(hass)

    # States at T-10d must be gone (10 > purge_keep_days=5)
    assert count_states(hass, entity_id) == 0

    # Short-term stats must still be present (10 < stats_keep_days=30)
    assert count_short_term_stats(hass, stat_id) > 0


async def test_multiple_stat_ages_mixed_survival(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Stats within stats_keep_days survive; stats beyond it are purged.

    T-10d stat  → within 30d  → survives
    T-35d stat  → beyond 30d  → purged
    """
    hass, _ = integration_entry

    stat_id = "sensor.energy_meter"
    ten_days_ago = NOW - timedelta(days=10)
    thirty_five_days_ago = NOW - timedelta(days=35)

    insert_short_term_stat(hass, stat_id, ten_days_ago, value=10.0)
    insert_short_term_stat(hass, stat_id, thirty_five_days_ago, value=35.0)

    assert count_short_term_stats(hass, stat_id) == 2

    await run_recorder_purge(hass)

    # Only the T-10d stat should remain
    assert count_short_term_stats(hass, stat_id) == 1


# ---------------------------------------------------------------------------
# Boundary: stats older than stats_keep_days ARE purged
# ---------------------------------------------------------------------------


async def test_short_term_stats_purged_beyond_stats_keep_days(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Stats at T-35d are purged because 35 > stats_keep_days (30)."""
    hass, _ = integration_entry

    stat_id = "sensor.old_meter"
    thirty_five_days_ago = NOW - timedelta(days=35)

    insert_short_term_stat(hass, stat_id, thirty_five_days_ago)
    assert count_short_term_stats(hass, stat_id) > 0

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) == 0


# ---------------------------------------------------------------------------
# Safety: patch never purges MORE aggressively than the recorder would
# ---------------------------------------------------------------------------


async def test_patch_never_more_aggressive_than_recorder(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """When purge_keep_days > stats_keep_days would imply, recorder cutoff wins.

    Our integration is configured with stats_keep_days=30 and the recorder
    uses purge_keep_days=5.  A stat at T-3d (within both windows) must always
    survive — the patch must never shift the cutoff to be MORE recent than
    the recorder's own cutoff.
    """
    hass, _ = integration_entry

    stat_id = "sensor.safe_meter"
    three_days_ago = NOW - timedelta(days=3)

    insert_short_term_stat(hass, stat_id, three_days_ago)
    assert count_short_term_stats(hass, stat_id) > 0

    await run_recorder_purge(hass)

    # T-3d is within both purge_keep_days (5) and stats_keep_days (30)
    assert count_short_term_stats(hass, stat_id) > 0


# ---------------------------------------------------------------------------
# Multiple independent statistic IDs
# ---------------------------------------------------------------------------


async def test_multiple_statistic_ids_independent_retention(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Each statistic ID's retention is evaluated independently.

    within_id (T-10d): within stats_keep_days (30d)  → survives
    beyond_id (T-35d): beyond stats_keep_days (30d)  → purged
    """
    hass, _ = integration_entry

    within_id = "sensor.within_meter"
    beyond_id = "sensor.beyond_meter"
    ten_days_ago = NOW - timedelta(days=10)
    thirty_five_days_ago = NOW - timedelta(days=35)

    insert_short_term_stat(hass, within_id, ten_days_ago)
    insert_short_term_stat(hass, beyond_id, thirty_five_days_ago)

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, within_id) > 0
    assert count_short_term_stats(hass, beyond_id) == 0


# ---------------------------------------------------------------------------
# Repeated purge cycles
# ---------------------------------------------------------------------------


async def test_repeated_purge_cycles_stats_survive(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Stats within stats_keep_days survive multiple consecutive purge cycles."""
    hass, _ = integration_entry

    stat_id = "sensor.repeated_purge_meter"
    ten_days_ago = NOW - timedelta(days=10)
    insert_short_term_stat(hass, stat_id, ten_days_ago)

    for cycle in range(3):
        await run_recorder_purge(hass)
        assert count_short_term_stats(hass, stat_id) > 0, (
            f"Stat unexpectedly purged on cycle {cycle + 1}"
        )


# ---------------------------------------------------------------------------
# Boundary: stats_keep_days edge cases
# ---------------------------------------------------------------------------


async def test_stats_just_past_boundary_purged(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """A stat one second past stats_keep_days is purged."""
    hass, _ = integration_entry

    stat_id = "sensor.boundary_past_meter"
    just_past = NOW - timedelta(days=STATS_KEEP_DAYS, seconds=1)

    insert_short_term_stat(hass, stat_id, just_past)
    assert count_short_term_stats(hass, stat_id) > 0

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) == 0


async def test_stats_just_within_boundary_survive(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """A stat one day inside stats_keep_days is not purged."""
    hass, _ = integration_entry

    stat_id = "sensor.boundary_within_meter"
    just_within = NOW - timedelta(days=STATS_KEEP_DAYS - 1)

    insert_short_term_stat(hass, stat_id, just_within)
    assert count_short_term_stats(hass, stat_id) > 0

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) > 0


# ---------------------------------------------------------------------------
# Long-term statistics are never purged by recorder.purge
# ---------------------------------------------------------------------------


async def test_long_term_stats_never_purged(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """recorder.purge does not touch the Statistics (long-term) table at all.

    Long-term stats accumulate indefinitely — neither the recorder's purge
    cycle nor our patch should remove them.  We insert rows at three ages
    (T-3d, T-10d, T-35d) that span both keep_days windows and assert that
    all three survive.
    """
    hass, _ = integration_entry

    stat_id = "sensor.longterm_energy"
    three_days_ago = NOW - timedelta(days=3)
    ten_days_ago = NOW - timedelta(days=10)
    thirty_five_days_ago = NOW - timedelta(days=35)

    insert_long_term_stat(hass, stat_id, three_days_ago, value=3.0)
    insert_long_term_stat(hass, stat_id, ten_days_ago, value=10.0)
    insert_long_term_stat(hass, stat_id, thirty_five_days_ago, value=35.0)
    assert count_long_term_stats(hass, stat_id) == 3

    await run_recorder_purge(hass)

    # All three must still be present — recorder.purge never removes long-term stats
    assert count_long_term_stats(hass, stat_id) == 3


async def test_long_term_stats_survive_multiple_purge_cycles(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Long-term stats accumulate across repeated purge cycles."""
    hass, _ = integration_entry

    stat_id = "sensor.longterm_accumulation"
    timestamps = [NOW - timedelta(days=d) for d in (1, 8, 15, 22, 35)]

    for i, ts in enumerate(timestamps):
        insert_long_term_stat(hass, stat_id, ts, value=float(i))
    assert count_long_term_stats(hass, stat_id) == 5

    for _ in range(3):
        await run_recorder_purge(hass)

    assert count_long_term_stats(hass, stat_id) == 5


# ---------------------------------------------------------------------------
# Three-timestamp verification for short-term stats
# ---------------------------------------------------------------------------


async def test_short_term_stats_three_timestamp_exact_counts(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Insert stats at T-3d, T-10d, T-35d; after purge exactly 2 survive.

    T-3d  → within both windows              → survives  (count towards 2)
    T-10d → within stats_keep_days (30d)     → survives  (count towards 2)
    T-35d → beyond stats_keep_days (30d)     → purged
    """
    hass, _ = integration_entry

    stat_id = "sensor.three_ts_meter"
    three_days_ago = NOW - timedelta(days=3)
    ten_days_ago = NOW - timedelta(days=10)
    thirty_five_days_ago = NOW - timedelta(days=35)

    insert_short_term_stat(hass, stat_id, three_days_ago, value=3.0)
    insert_short_term_stat(hass, stat_id, ten_days_ago, value=10.0)
    insert_short_term_stat(hass, stat_id, thirty_five_days_ago, value=35.0)
    assert count_short_term_stats(hass, stat_id) == 3

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) == 2


# ---------------------------------------------------------------------------
# Entity purge rules do NOT remove that entity's short-term statistics
# ---------------------------------------------------------------------------


async def test_entity_purge_rule_leaves_short_term_stats_intact(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """recorder.purge_entities only removes states; short-term stats are unaffected.

    Our entity purge rules call recorder.purge_entities, which purges the
    States table but does NOT touch statistics_short_term.  A stat for the
    same entity_id at T-10d must therefore survive even though the entity's
    states at T-10d are removed.
    """
    hass, _ = integration_entry

    entity_id = "sensor.dual_table_entity"
    stat_id = entity_id
    ten_days_ago = NOW - timedelta(days=10)

    # Write state AND short-term stat at T-10d for the same entity
    await set_state_at(hass, entity_id, "42", ten_days_ago, freezer)
    insert_short_term_stat(hass, stat_id, ten_days_ago, value=42.0)

    assert count_states(hass, entity_id) > 0
    assert count_short_term_stats(hass, stat_id) > 0

    # Run entity purge rule with keep_days=4 → T-10d states are beyond the window
    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "dual_table_rule",
                "entity_globs": [],
                CONF_ENTITY_IDS: [entity_id],
                "device_ids": [],
                "integration_filter": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                CONF_KEEP_DAYS: 4,
                "enabled": True,
            }
        ],
    )
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await async_wait_recording_done(hass)

    # States at T-10d must be gone (purge_entities removed them)
    assert count_states(hass, entity_id) == 0

    # Short-term stats at T-10d must still be present (purge_entities never touches stats)
    assert count_short_term_stats(hass, stat_id) > 0


async def test_entity_purge_rule_leaves_long_term_stats_intact(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """recorder.purge_entities never touches Statistics (long-term) either."""
    hass, _ = integration_entry

    entity_id = "sensor.longterm_dual_entity"
    stat_id = entity_id
    ten_days_ago = NOW - timedelta(days=10)

    await set_state_at(hass, entity_id, "99", ten_days_ago, freezer)
    insert_long_term_stat(hass, stat_id, ten_days_ago, value=99.0)

    assert count_states(hass, entity_id) > 0
    assert count_long_term_stats(hass, stat_id) > 0

    await configure_rules(
        hass,
        [
            {
                CONF_RULE_NAME: "longterm_dual_rule",
                "entity_globs": [],
                CONF_ENTITY_IDS: [entity_id],
                "device_ids": [],
                "integration_filter": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                CONF_KEEP_DAYS: 4,
                "enabled": True,
            }
        ],
    )
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await async_wait_recording_done(hass)

    assert count_states(hass, entity_id) == 0
    assert count_long_term_stats(hass, stat_id) > 0


# ---------------------------------------------------------------------------
# Combined scenario: entity purge rule + global recorder.purge
#
# In real deployments both mechanisms run.  Our entity rules call
# recorder.purge_entities (removes states only), then the nightly
# recorder.purge removes short-term stats — unless our patch extends
# the cutoff.
# ---------------------------------------------------------------------------


def _entity_rule(entity_id: str, keep_days: int) -> dict:
    return {
        CONF_RULE_NAME: "combined_rule",
        "entity_globs": [],
        CONF_ENTITY_IDS: [entity_id],
        "device_ids": [],
        "integration_filter": [],
        "entity_regex_include": [],
        "entity_regex_exclude": [],
        CONF_KEEP_DAYS: keep_days,
        "enabled": True,
    }


async def test_combined_entity_rule_then_global_purge_stats_survive(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entity rule removes states; subsequent global purge leaves short-term stats.

    Timeline (purge_keep_days=5, stats_keep_days=30):
      T-10d state  → removed by entity rule (keep_days=4, 10 > 4)
      T-10d stat   → NOT removed by entity rule (purge_entities never touches stats)
                   → survives global purge because 10 < stats_keep_days (30) via patch
    """
    hass, _ = integration_entry

    entity_id = "sensor.combined_test_entity"
    stat_id = entity_id
    ten_days_ago = NOW - timedelta(days=10)

    await set_state_at(hass, entity_id, "42", ten_days_ago, freezer)
    insert_short_term_stat(hass, stat_id, ten_days_ago, value=42.0)

    assert count_states(hass, entity_id) > 0
    assert count_short_term_stats(hass, stat_id) > 0

    # Step 1: entity rule removes the state
    await configure_rules(hass, [_entity_rule(entity_id, keep_days=4)])
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await async_wait_recording_done(hass)
    assert count_states(hass, entity_id) == 0

    # Step 2: global purge runs — patch must protect the T-10d stat
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_id) > 0, (
        "Patch should keep T-10d stat alive (10 < stats_keep_days=30)"
    )


async def test_combined_entity_rule_then_global_purge_old_stats_removed(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entity rule removes states; global purge correctly removes stats beyond stats_keep_days.

    Timeline (purge_keep_days=5, stats_keep_days=30):
      T-35d state  → removed by entity rule (keep_days=4, 35 > 4)
      T-35d stat   → survives entity rule (purge_entities never touches stats)
                   → removed by global purge because 35 > stats_keep_days (30)
    """
    hass, _ = integration_entry

    entity_id = "sensor.combined_old_entity"
    stat_id = entity_id
    thirty_five_days_ago = NOW - timedelta(days=35)

    await set_state_at(hass, entity_id, "7", thirty_five_days_ago, freezer)
    insert_short_term_stat(hass, stat_id, thirty_five_days_ago, value=7.0)

    assert count_states(hass, entity_id) > 0
    assert count_short_term_stats(hass, stat_id) > 0

    # Step 1: entity rule removes the state
    await configure_rules(hass, [_entity_rule(entity_id, keep_days=4)])
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await async_wait_recording_done(hass)
    assert count_states(hass, entity_id) == 0

    # Step 2: global purge — T-35d stat is beyond stats_keep_days, must be purged
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_id) == 0, (
        "T-35d stat must be removed (35 > stats_keep_days=30)"
    )


async def test_combined_mixed_stat_ages_after_entity_rule(
    integration_entry: tuple[HomeAssistant, Any], freezer: Any
) -> None:
    """Entity rule removes all states; global purge then applies correct stat cutoff.

    Stats at T-10d and T-35d share the same entity.  After the entity rule
    strips the states, the global purge must remove only the T-35d stat.

    T-10d stat → survives  (10 < stats_keep_days=30)
    T-35d stat → purged    (35 > stats_keep_days=30)
    """
    hass, _ = integration_entry

    entity_id = "sensor.combined_mixed_entity"
    stat_id = entity_id
    ten_days_ago = NOW - timedelta(days=10)
    thirty_five_days_ago = NOW - timedelta(days=35)

    # Write states at both ages so the entity rule has something to purge
    await set_state_at(hass, entity_id, "old", thirty_five_days_ago, freezer)
    await set_state_at(hass, entity_id, "recent", ten_days_ago, freezer)
    insert_short_term_stat(hass, stat_id, ten_days_ago, value=10.0)
    insert_short_term_stat(hass, stat_id, thirty_five_days_ago, value=35.0)

    assert count_short_term_stats(hass, stat_id) == 2

    # Step 1: entity rule strips states (keep_days=4 → both ages beyond window)
    await configure_rules(hass, [_entity_rule(entity_id, keep_days=4)])
    await hass.services.async_call(DOMAIN, "run_purge_now", {}, blocking=True)
    await async_wait_recording_done(hass)
    assert count_states(hass, entity_id) == 0

    # Step 2: global purge applies stats_keep_days cutoff independently
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_id) == 1, (
        "Only T-10d stat should survive; T-35d must be purged"
    )
