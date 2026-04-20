# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for manager lifecycle behaviours:

- ``_schedule_purge`` must not cancel/reinstall the timer when the HH:MM
  hasn't changed (would lose a firing that's about to happen).
- ``_execute_all_rules`` must warn once per rule that matches zero
  entities, not on every run.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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


# ---------------------------------------------------------------------------
# Trailing recorder.purge call
# ---------------------------------------------------------------------------


def _make_manager_with_config(
    *, enabled: bool = True, repack: str = "never", force_repack: bool = False, **extra
):
    """Build a manager with a validated-like ha_recorder_purge block.

    The trio of ha_recorder_purge fields are passed as keyword args to keep
    call sites short; ``extra`` overrides top-level fields like ``rules``.
    ``repack`` defaults to ``"never"`` so tests don't need to freeze the
    clock for a deterministic repack value — cadence-specific tests pass
    ``repack="weekly"`` explicitly.
    """
    from custom_components.recorder_tuning import RecorderTuningManager

    hass = MagicMock()
    hass.services.async_call = AsyncMock()
    config = {
        "purge_time": "03:00",
        "dry_run": False,
        "rules": [],
        "ha_recorder_purge": {
            "enabled": enabled,
            "repack": repack,
            "force_repack": force_repack,
        },
        **extra,
    }
    return hass, RecorderTuningManager(hass, config)


@pytest.mark.asyncio
async def test_recorder_purge_called_after_rules_when_enabled():
    """With enabled=true (default), recorder.purge is called after rules."""
    hass, manager = _make_manager_with_config()
    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=False)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(calls) == 1
    assert calls[0].args[2] == {"repack": False}
    assert calls[0].kwargs.get("blocking") is True


@pytest.mark.asyncio
async def test_recorder_purge_skipped_when_disabled():
    """With enabled=false, recorder.purge must not be called."""
    hass, manager = _make_manager_with_config(enabled=False)
    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=False)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert calls == []


@pytest.mark.asyncio
async def test_recorder_purge_skipped_in_dry_run():
    """In dry-run mode the recorder.purge call is logged but not executed."""
    hass, manager = _make_manager_with_config()
    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=True)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert calls == []


@pytest.mark.asyncio
async def test_recorder_purge_passes_repack_option():
    """force_repack=true is forwarded to the recorder.purge call."""
    hass, manager = _make_manager_with_config(force_repack=True)
    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=False)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(calls) == 1
    assert calls[0].args[2] == {"repack": True}


def test_should_repack_force_wins_over_cadence():
    """force_repack=true must repack every day regardless of repack cadence."""
    from datetime import datetime

    from custom_components.recorder_tuning import _should_repack_today

    weekday = datetime(2026, 4, 15)  # Wednesday — no "natural" repack day
    assert _should_repack_today(weekday, "never", force_repack=True) is True
    assert _should_repack_today(weekday, "monthly", force_repack=True) is True
    assert _should_repack_today(weekday, "weekly", force_repack=True) is True


def test_should_repack_never_is_always_false_without_force():
    from datetime import datetime

    from custom_components.recorder_tuning import _should_repack_today

    for day in (
        datetime(2026, 4, 5),  # 1st Sunday
        datetime(2026, 4, 12),  # 2nd Sunday
        datetime(2026, 4, 15),  # Wednesday
    ):
        assert _should_repack_today(day, "never", force_repack=False) is False


def test_should_repack_weekly_fires_only_on_sundays():
    from datetime import datetime

    from custom_components.recorder_tuning import _should_repack_today

    sunday = datetime(2026, 4, 12)
    monday = datetime(2026, 4, 13)
    saturday = datetime(2026, 4, 11)

    assert _should_repack_today(sunday, "weekly", force_repack=False) is True
    assert _should_repack_today(monday, "weekly", force_repack=False) is False
    assert _should_repack_today(saturday, "weekly", force_repack=False) is False


def test_should_repack_monthly_fires_on_second_sunday_only():
    from datetime import datetime

    from custom_components.recorder_tuning import _should_repack_today

    # April 2026: 1st Sun=5, 2nd Sun=12, 3rd Sun=19, 4th Sun=26
    assert _should_repack_today(datetime(2026, 4, 5), "monthly", False) is False
    assert _should_repack_today(datetime(2026, 4, 12), "monthly", False) is True
    assert _should_repack_today(datetime(2026, 4, 19), "monthly", False) is False
    assert _should_repack_today(datetime(2026, 4, 26), "monthly", False) is False
    # Not even a Sunday
    assert _should_repack_today(datetime(2026, 4, 15), "monthly", False) is False


@pytest.mark.asyncio
async def test_recorder_purge_cadence_on_matching_day_sends_repack_true(freezer):
    """weekly cadence on a Sunday must pass repack=True to recorder.purge."""
    from datetime import datetime as real_datetime

    hass, manager = _make_manager_with_config(repack="weekly", force_repack=False)
    freezer.move_to(real_datetime(2026, 4, 12))  # Sunday

    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=False)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(calls) == 1
    assert calls[0].args[2] == {"repack": True}


@pytest.mark.asyncio
async def test_recorder_purge_cadence_off_day_sends_repack_false(freezer):
    """weekly cadence on a non-Sunday must pass repack=False."""
    from datetime import datetime as real_datetime

    hass, manager = _make_manager_with_config(repack="weekly", force_repack=False)
    freezer.move_to(real_datetime(2026, 4, 15))  # Wednesday

    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager._execute_all_rules(dry_run=False)

    calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(calls) == 1
    assert calls[0].args[2] == {"repack": False}


# ---------------------------------------------------------------------------
# run_purge_now service: rule_names filtering
# ---------------------------------------------------------------------------


def _make_manager_with_rules(rules: list[dict], **config_overrides):
    """Like _make_manager_with_config but seeds the manager with actual rules."""
    from custom_components.recorder_tuning import RecorderTuningManager
    from custom_components.recorder_tuning.const import (
        CONF_ENTITY_IDS,
        CONF_KEEP_DAYS,
        CONF_RULE_NAME,
    )

    full_rules: list[dict] = []
    for r in rules:
        full_rules.append(
            {
                CONF_RULE_NAME: r["name"],
                CONF_ENTITY_IDS: r.get("entity_ids", []),
                CONF_KEEP_DAYS: r.get("keep_days", 7),
                "integration_filter": [],
                "device_ids": [],
                "entity_globs": [],
                "entity_regex_include": [],
                "entity_regex_exclude": [],
                "enabled": True,
                "match_mode": "all",
                "dry_run": None,
            }
        )

    hass = MagicMock()
    hass.services.async_call = AsyncMock()
    config = {
        "purge_time": "03:00",
        "dry_run": False,
        "rules": full_rules,
        "ha_recorder_purge": {
            "enabled": True,
            "repack": "never",
            "force_repack": False,
        },
        **config_overrides,
    }
    return hass, RecorderTuningManager(hass, config)


@pytest.mark.asyncio
async def test_run_purge_now_rule_names_filters_rules():
    """Only rules named in rule_names are executed."""
    hass, manager = _make_manager_with_rules(
        [
            {"name": "rule_a", "entity_ids": ["sensor.a"]},
            {"name": "rule_b", "entity_ids": ["sensor.b"]},
            {"name": "rule_c", "entity_ids": ["sensor.c"]},
        ]
    )

    call = MagicMock()
    call.data = {"rule_names": ["rule_a", "rule_c"]}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_entity_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]
    purged_entities = {
        eid for call in purge_entity_calls for eid in call.args[2]["entity_id"]
    }
    assert purged_entities == {"sensor.a", "sensor.c"}


@pytest.mark.asyncio
async def test_run_purge_now_rule_names_match_is_case_insensitive():
    """Rule names match regardless of case differences between call + config."""
    hass, manager = _make_manager_with_rules(
        [
            {"name": "Frigate camera metrics", "entity_ids": ["sensor.a"]},
            {"name": "ESPHome diagnostic sensors", "entity_ids": ["sensor.b"]},
        ]
    )

    call = MagicMock()
    # Mixed casing: one all-lower, one all-upper
    call.data = {"rule_names": ["frigate camera metrics", "ESPHOME DIAGNOSTIC SENSORS"]}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_entity_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]
    purged = {eid for call in purge_entity_calls for eid in call.args[2]["entity_id"]}
    assert purged == {"sensor.a", "sensor.b"}


@pytest.mark.asyncio
async def test_run_purge_now_rule_names_skips_trailing_global_purge():
    """Filtered runs must not trigger the trailing recorder.purge sweep."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    call = MagicMock()
    call.data = {"rule_names": ["rule_a"]}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert purge_calls == []


@pytest.mark.asyncio
async def test_run_purge_now_default_skips_trailing_purge():
    """Default (no args) must NOT trigger the global recorder.purge."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    call = MagicMock()
    call.data = {}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert purge_calls == []


@pytest.mark.asyncio
async def test_run_purge_now_explicit_opt_in_runs_trailing_purge():
    """ha_recorder_purge: true opts into the global sweep on a manual call."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    call = MagicMock()
    call.data = {"ha_recorder_purge": True}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(purge_calls) == 1


@pytest.mark.asyncio
async def test_run_purge_now_rule_names_overrides_explicit_opt_in():
    """rule_names forces-skip even if ha_recorder_purge: true is also passed."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    call = MagicMock()
    call.data = {"rule_names": ["rule_a"], "ha_recorder_purge": True}

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert purge_calls == []


@pytest.mark.asyncio
async def test_scheduled_run_still_triggers_trailing_purge():
    """The scheduled nightly path is unaffected by the service-call default flip."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                # Simulate the scheduled firing path — _async_run_purge.
                from datetime import datetime

                await manager._async_run_purge(datetime.now())

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge")
    ]
    assert len(purge_calls) == 1


@pytest.mark.asyncio
async def test_run_purge_now_unknown_rule_names_warn_not_abort(caplog):
    """Unknown names log a WARNING but the matching rules still run."""
    import logging

    hass, manager = _make_manager_with_rules(
        [
            {"name": "rule_a", "entity_ids": ["sensor.a"]},
            {"name": "rule_b", "entity_ids": ["sensor.b"]},
        ]
    )

    call = MagicMock()
    call.data = {"rule_names": ["rule_a", "typo_rule"]}
    caplog.set_level(logging.WARNING)

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    assert any("unknown rule name" in r.message for r in caplog.records)

    purge_entity_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]
    purged = {eid for call in purge_entity_calls for eid in call.args[2]["entity_id"]}
    assert purged == {"sensor.a"}


@pytest.mark.asyncio
async def test_run_purge_now_all_rule_names_unknown_is_noop(caplog):
    """If every requested name is unknown, the call is a no-op (logs warning)."""
    import logging

    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"]}]
    )

    call = MagicMock()
    call.data = {"rule_names": ["typo_a", "typo_b"]}
    caplog.set_level(logging.WARNING)

    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        await manager.async_run_purge_now(call)

    # No purge_entities, no global purge
    assert hass.services.async_call.call_args_list == []
    assert any("nothing to do" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_recorder_purge_failure_is_logged_not_raised(caplog):
    """If recorder.purge raises, log the error and continue (don't bubble up)."""
    import logging

    hass, manager = _make_manager_with_config()
    hass.services.async_call.side_effect = RuntimeError("recorder unavailable")
    caplog.set_level(logging.ERROR)

    with patch(
        "custom_components.recorder_tuning.er.async_get", return_value=MagicMock()
    ):
        # Must not raise
        await manager._execute_all_rules(dry_run=False)

    assert any(
        "recorder.purge failed" in r.message
        for r in caplog.records
        if r.levelname == "ERROR"
    )
