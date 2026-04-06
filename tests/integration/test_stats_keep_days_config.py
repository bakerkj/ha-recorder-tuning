# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Integration tests for stats_keep_days configurability.

Verifies that the monkey-patch reads ``stats_keep_days`` live from the config
entry at purge time, so changing the setting takes effect on the very next
``recorder.purge`` call without reloading the integration.

Time model
----------
NOW             = 2026-04-04 12:00 UTC
PURGE_KEEP_DAYS = 5   (recorder global — raw states older than this are purged)
STATS_KEEP_DAYS = 30  (default configured in integration_entry fixture)

Stat ages used:
  T-3d   → within any reasonable window (3 < 5 < 30)
  T-10d  → beyond purge_keep_days (5) but within default stats_keep_days (30)
  T-20d  → beyond a reduced stats_keep_days (e.g. 15) but within default (30)
  T-35d  → beyond default stats_keep_days (30)
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant

from .conftest import (
    NOW,
    PURGE_KEEP_DAYS,
    STATS_KEEP_DAYS,
    count_short_term_stats,
    insert_short_term_stat,
    set_stats_keep_days,
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
# Baseline: default stats_keep_days (30d) works as expected
# ---------------------------------------------------------------------------


async def test_default_stats_keep_days_protects_ten_day_stat(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Baseline: with default stats_keep_days=30, a T-10d stat survives purge."""
    hass, _ = integration_entry

    stat_id = "sensor.baseline_meter"
    insert_short_term_stat(hass, stat_id, NOW - timedelta(days=10), value=10.0)

    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) > 0


# ---------------------------------------------------------------------------
# Lowering stats_keep_days takes effect immediately
# ---------------------------------------------------------------------------


async def test_lowering_stats_keep_days_causes_previously_safe_stat_to_be_purged(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Lowering stats_keep_days makes a previously-safe stat purgeable.

    T-20d stat:
      Before change: stats_keep_days=30  → 20 < 30  → would survive
      After  change: stats_keep_days=15  → 20 > 15  → purged
    """
    hass, _ = integration_entry

    stat_id = "sensor.lower_keep_days_meter"
    twenty_days_ago = NOW - timedelta(days=20)
    insert_short_term_stat(hass, stat_id, twenty_days_ago, value=20.0)

    # Confirm it would survive with the default
    assert count_short_term_stats(hass, stat_id) > 0

    # Lower stats_keep_days so the T-20d stat is now beyond the window
    set_stats_keep_days(hass, 15)
    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) == 0, (
        "T-20d stat must be purged after lowering stats_keep_days to 15"
    )


async def test_lowering_stats_keep_days_preserves_stats_within_new_window(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Lowering stats_keep_days only purges stats outside the new window.

    T-10d stat: still within reduced window (15d)  → survives
    T-20d stat: now outside reduced window (15d)   → purged
    """
    hass, _ = integration_entry

    stat_id_within = "sensor.within_new_window_meter"
    stat_id_outside = "sensor.outside_new_window_meter"

    insert_short_term_stat(hass, stat_id_within, NOW - timedelta(days=10), value=10.0)
    insert_short_term_stat(hass, stat_id_outside, NOW - timedelta(days=20), value=20.0)

    set_stats_keep_days(hass, 15)
    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id_within) > 0
    assert count_short_term_stats(hass, stat_id_outside) == 0


# ---------------------------------------------------------------------------
# Raising stats_keep_days takes effect immediately
# ---------------------------------------------------------------------------


async def test_raising_stats_keep_days_protects_previously_purgeable_stat(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """Raising stats_keep_days protects a stat that would otherwise be purged.

    T-20d stat:
      Default stats_keep_days=30 → 20 < 30 → survives (but let's start with a
      tight window and then widen it to make the protection clear)

    We start with stats_keep_days=15 (T-20d would be purged), raise it to 25
    (T-20d now survives), then purge.
    """
    hass, _ = integration_entry

    stat_id = "sensor.raise_keep_days_meter"
    twenty_days_ago = NOW - timedelta(days=20)
    insert_short_term_stat(hass, stat_id, twenty_days_ago, value=20.0)

    # Start with a window that would purge the T-20d stat
    set_stats_keep_days(hass, 15)

    # Raise it so the T-20d stat is now within the window
    set_stats_keep_days(hass, 25)
    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) > 0, (
        "T-20d stat must survive after raising stats_keep_days to 25"
    )


# ---------------------------------------------------------------------------
# Patch never more aggressive than recorder (stats_keep_days < purge_keep_days)
# ---------------------------------------------------------------------------


async def test_stats_keep_days_below_purge_keep_days_uses_recorder_cutoff(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """When stats_keep_days < purge_keep_days the recorder's cutoff wins.

    If stats_keep_days=2 but purge_keep_days=5, setting the cutoff to T-2d
    would be MORE aggressive than the recorder's T-5d.  The patch uses
    min(purge_before, stats_purge_before) so the recorder cutoff is honoured.

    T-3d stat is within purge_keep_days (5) → must survive regardless of
    stats_keep_days being set to an absurdly low value.
    """
    hass, _ = integration_entry

    stat_id = "sensor.min_cutoff_meter"
    three_days_ago = NOW - timedelta(days=3)
    insert_short_term_stat(hass, stat_id, three_days_ago, value=3.0)

    # Set stats_keep_days to 2 — would imply purging T-3d, but recorder says keep it
    set_stats_keep_days(hass, 2)
    await run_recorder_purge(hass)

    assert count_short_term_stats(hass, stat_id) > 0, (
        "Patch must never be more aggressive than recorder; T-3d must survive"
    )


# ---------------------------------------------------------------------------
# Change takes effect without reloading the integration
# ---------------------------------------------------------------------------


async def test_live_stats_keep_days_change_no_reload_required(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """stats_keep_days is read from the config entry at purge time, not at patch time.

    We change it twice between purges — both changes must be reflected
    immediately without unloading/reloading the integration.

    Round 1: stats_keep_days=15 → T-20d stat purged
    Round 2: stats_keep_days=25 → T-10d stat survives (already survived round 1 too)
    """
    hass, _ = integration_entry

    stat_10d = "sensor.live_change_10d"
    stat_20d = "sensor.live_change_20d"

    insert_short_term_stat(hass, stat_10d, NOW - timedelta(days=10), value=10.0)
    insert_short_term_stat(hass, stat_20d, NOW - timedelta(days=20), value=20.0)

    # Round 1: tighten window — T-20d goes away, T-10d stays
    set_stats_keep_days(hass, 15)
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_10d) > 0
    assert count_short_term_stats(hass, stat_20d) == 0

    # Round 2: widen window again — T-10d still present and still protected
    set_stats_keep_days(hass, 25)
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_10d) > 0


# ---------------------------------------------------------------------------
# Restore default and verify behaviour returns to normal
# ---------------------------------------------------------------------------


async def test_restoring_default_stats_keep_days_resumes_normal_behaviour(
    integration_entry: tuple[HomeAssistant, Any],
) -> None:
    """After lowering then restoring stats_keep_days the original behaviour returns.

    1. Lower to 15 → T-20d purged, T-10d survives.
    2. Restore to 30 → T-10d still survives (already confirmed); a fresh T-20d
       stat inserted after step 1 survives the next purge.
    """
    hass, _ = integration_entry

    stat_id = "sensor.restore_default_meter"

    insert_short_term_stat(hass, stat_id, NOW - timedelta(days=20), value=20.0)

    # Lower — T-20d stat is purged
    set_stats_keep_days(hass, 15)
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_id) == 0

    # Insert a fresh T-20d stat, restore the default, purge again — must survive
    insert_short_term_stat(hass, stat_id, NOW - timedelta(days=20), value=20.0)
    set_stats_keep_days(hass, STATS_KEEP_DAYS)
    await run_recorder_purge(hass)
    assert count_short_term_stats(hass, stat_id) > 0, (
        "After restoring stats_keep_days=30, T-20d stat must survive"
    )
