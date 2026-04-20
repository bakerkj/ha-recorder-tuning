# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for manager lifecycle behaviours:

- ``_schedule_purge`` must not cancel/reinstall the timer when the HH:MM
  hasn't changed (would lose a firing that's about to happen).
- ``_execute_all_rules`` must warn once per rule that matches zero
  entities, not on every run.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_manager(rules, purge_time="03:00"):
    """Return a manager configured with ``purge_time`` and the given rules."""
    from custom_components.recorder_tuning import RecorderTuningManager

    hass = MagicMock()
    config = {"purge_time": purge_time, "dry_run": False, "rules": list(rules)}
    return RecorderTuningManager(hass, config)


# ---------------------------------------------------------------------------
# _schedule_purge: no-op when HH:MM is unchanged
# ---------------------------------------------------------------------------


def test_schedule_purge_reinstalls_when_time_changes():
    manager = _make_manager([], purge_time="03:00")

    first_unsub = MagicMock()
    second_unsub = MagicMock()

    with patch(
        "custom_components.recorder_tuning.async_track_time_change",
        side_effect=[first_unsub, second_unsub],
    ) as mock_track:
        manager._schedule_purge()
        # Change purge_time — next call should cancel + reinstall
        manager.config = {"purge_time": "04:30"}
        manager._schedule_purge()

    assert mock_track.call_count == 2
    first_unsub.assert_called_once()
    assert manager._unsub_timer is second_unsub
    assert manager._scheduled_at == "04:30"


def test_schedule_purge_noop_when_time_unchanged():
    """A reload that doesn't touch purge_time must not disturb the timer."""
    manager = _make_manager([], purge_time="03:00")

    unsub = MagicMock()
    with patch(
        "custom_components.recorder_tuning.async_track_time_change",
        return_value=unsub,
    ) as mock_track:
        manager._schedule_purge()
        # Second call with the same time — timer must not be cancelled.
        manager._schedule_purge()

    mock_track.assert_called_once()
    unsub.assert_not_called()
    assert manager._unsub_timer is unsub


def test_unload_clears_scheduled_at():
    """After unload, a subsequent schedule_purge must re-install."""
    manager = _make_manager([], purge_time="03:00")

    unsub = MagicMock()
    with patch(
        "custom_components.recorder_tuning.async_track_time_change",
        return_value=unsub,
    ):
        manager._schedule_purge()
        manager.async_unload()

    assert manager._scheduled_at is None
    unsub.assert_called_once()


# ---------------------------------------------------------------------------
# Zero-match warn-once-per-rule-per-reload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zero_match_warns_once_then_debug(caplog):
    import logging

    from custom_components.recorder_tuning.const import (
        CONF_ENABLED,
        CONF_ENTITY_IDS,
        CONF_KEEP_DAYS,
        CONF_RULE_NAME,
    )

    rule = {
        CONF_RULE_NAME: "stale",
        CONF_ENTITY_IDS: ["sensor.does_not_exist"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    manager = _make_manager([rule])

    # Patch _resolve_entities to return empty (simulating a rule that matches
    # nothing). Use patch.object so we don't have to go through the registry.
    with patch.object(manager, "_resolve_entities", return_value=[]):
        with patch(
            "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
        ):
            caplog.set_level(logging.DEBUG)

            await manager._execute_all_rules(dry_run=True)
            # First run → WARNING
            warnings = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "stale" in r.message
            ]
            assert len(warnings) == 1
            assert "stale" in manager._warned_empty_rules

            caplog.clear()
            await manager._execute_all_rules(dry_run=True)
            # Second run → DEBUG only, no more WARNING
            warnings = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "stale" in r.message
            ]
            assert warnings == []
            debugs = [
                r
                for r in caplog.records
                if r.levelname == "DEBUG" and "stale" in r.message
            ]
            assert any("still matches no entities" in r.message for r in debugs)


@pytest.mark.asyncio
async def test_zero_match_suppression_clears_when_rule_recovers():
    """If a rule starts matching again, a later zero-match warns again."""
    from custom_components.recorder_tuning.const import (
        CONF_ENABLED,
        CONF_ENTITY_IDS,
        CONF_KEEP_DAYS,
        CONF_RULE_NAME,
    )

    rule = {
        CONF_RULE_NAME: "recoverable",
        CONF_ENTITY_IDS: ["sensor.x"],
        CONF_KEEP_DAYS: 7,
        CONF_ENABLED: True,
    }
    manager = _make_manager([rule])

    # First: zero match → warn + add to suppressed set.
    with patch.object(manager, "_resolve_entities", return_value=[]):
        with patch(
            "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
        ):
            await manager._execute_all_rules(dry_run=True)
    assert "recoverable" in manager._warned_empty_rules

    # Then: rule matches → discard from suppressed set.
    with patch.object(manager, "_resolve_entities", return_value=["sensor.x"]):
        with patch.object(manager, "_log_purge_plan"):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules(dry_run=True)
    assert "recoverable" not in manager._warned_empty_rules


def test_update_config_clears_zero_match_suppression():
    """Calling update_config resets the zero-match suppression set."""
    manager = _make_manager([])
    manager._warned_empty_rules.add("rule_a")
    manager._warned_empty_rules.add("rule_b")

    manager.update_config({"purge_time": "03:00", "dry_run": False, "rules": []})

    assert manager._warned_empty_rules == set()
