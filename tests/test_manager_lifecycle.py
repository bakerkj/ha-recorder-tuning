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

            await manager._execute_all_rules(service_dry_run=True)
            # First run → WARNING
            warnings = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "stale" in r.message
            ]
            assert len(warnings) == 1
            assert "stale" in manager._warned_empty_rules

            caplog.clear()
            await manager._execute_all_rules(service_dry_run=True)
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
            await manager._execute_all_rules(service_dry_run=True)
    assert "recoverable" in manager._warned_empty_rules

    # Then: rule matches → discard from suppressed set.
    with patch.object(manager, "_resolve_entities", return_value=["sensor.x"]):
        with patch.object(manager, "_log_purge_plan"):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules(service_dry_run=True)
    assert "recoverable" not in manager._warned_empty_rules


# ---------------------------------------------------------------------------
# _effective_dry_run precedence matrix (top-level safety lock; service wins
# over per-rule; per-rule wins over top-level; top-level is the fallback).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "top,service,rule,expected",
    [
        # Top-level lock beats everything.
        (True, None, None, True),
        (True, False, False, True),
        (True, True, True, True),
        (True, False, True, True),
        (True, True, False, True),
        # Top=false + service provided → service wins over per-rule.
        (False, True, None, True),
        (False, False, None, False),
        (False, True, False, True),
        (False, False, True, False),
        # Top=false + service unset → per-rule wins.
        (False, None, True, True),
        (False, None, False, False),
        # Top=false + service unset + rule unset → inherit top (false).
        (False, None, None, False),
    ],
)
def test_effective_dry_run_matrix(top, service, rule, expected):
    from custom_components.recorder_tuning import _effective_dry_run

    assert _effective_dry_run(top, service, rule) is expected


def _make_manager_with_full_config(rules_list, *, top_dry_run=False):
    """Build a manager with a specific top-level dry_run + rule set."""
    from custom_components.recorder_tuning import RecorderTuningManager

    hass = MagicMock()
    hass.services.async_call = AsyncMock()
    config = {
        "purge_time": "03:00",
        "dry_run": top_dry_run,
        "rules": rules_list,
        "ha_recorder_purge": {
            "enabled": False,
            "repack": "never",
            "force_repack": False,
        },
    }
    return hass, RecorderTuningManager(hass, config)


def _make_rule(name, *, dry_run=None, entity_ids=None):
    return {
        "name": name,
        "entity_ids": entity_ids or [f"sensor.{name}"],
        "keep_days": 7,
        "enabled": True,
        "dry_run": dry_run,
        "match_mode": "all",
    }


@pytest.mark.asyncio
async def test_top_level_dry_run_locks_even_when_rule_overrides_to_false():
    """Top-level dry_run=true must override per-rule dry_run=false."""
    hass, manager = _make_manager_with_full_config(
        [_make_rule("aggressive_rule", dry_run=False)], top_dry_run=True
    )
    with patch.object(
        manager, "_resolve_entities", side_effect=lambda r, reg: r["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules()

    assert not [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]


@pytest.mark.asyncio
async def test_service_dry_run_true_forces_dry_even_when_rule_says_false():
    """Service dry_run=true beats per-rule dry_run=false (top-level is false)."""
    hass, manager = _make_manager_with_full_config(
        [_make_rule("rule_a", dry_run=False)]
    )
    with patch.object(
        manager, "_resolve_entities", side_effect=lambda r, reg: r["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules(service_dry_run=True)

    assert not [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]


@pytest.mark.asyncio
async def test_service_dry_run_false_forces_live_even_when_rule_says_true():
    """Service dry_run=false beats per-rule dry_run=true (top-level is false)."""
    hass, manager = _make_manager_with_full_config([_make_rule("rule_a", dry_run=True)])
    with patch.object(
        manager, "_resolve_entities", side_effect=lambda r, reg: r["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules(service_dry_run=False)

    purge_calls = [
        c
        for c in hass.services.async_call.call_args_list
        if c.args[:2] == ("recorder", "purge_entities")
    ]
    assert len(purge_calls) == 1


@pytest.mark.asyncio
async def test_mixed_mode_log_when_rules_disagree(caplog):
    """Scheduled-style run with different per-rule dry_runs emits MIXED log line."""
    import logging

    rules = [
        _make_rule("dry_rule", dry_run=True),
        _make_rule("live_rule", dry_run=False),
    ]
    hass, manager = _make_manager_with_full_config(rules)
    caplog.set_level(logging.INFO)

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda r, reg: r["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", new_callable=AsyncMock):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager._execute_all_rules()

    msgs = [r.message for r in caplog.records]
    assert any(
        "[MIXED] starting" in m and "1 rule(s) LIVE" in m and "1 rule(s) DRY RUN" in m
        for m in msgs
    )
    assert any("[MIXED] complete" in m for m in msgs)


def test_update_config_clears_zero_match_suppression():
    """Calling update_config resets the zero-match suppression set."""
    manager = _make_manager([])
    manager._warned_empty_rules.add("rule_a")
    manager._warned_empty_rules.add("rule_b")

    manager.update_config({"purge_time": "03:00", "dry_run": False, "rules": []})

    assert manager._warned_empty_rules == set()


# ---------------------------------------------------------------------------
# Dry-run summary on setup/reload
# ---------------------------------------------------------------------------


def test_dry_run_summary_top_level_locked(caplog):
    """Top-level dry_run: true → safety-lock summary, no per-rule breakdown."""
    import logging

    _, manager = _make_manager_with_full_config(
        [_make_rule("a", dry_run=False), _make_rule("b", dry_run=True)],
        top_dry_run=True,
    )
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "top-level dry_run: true" in m and "all 2 enabled rule(s)" in m for m in msgs
    )
    # No aggregate LIVE/DRY breakdown when the lock is in effect
    assert not any("dry-run summary" in m for m in msgs)


def test_dry_run_summary_all_dry(caplog):
    """All rules dry (top=false, per-rule=true) → summary shows 0 LIVE, N DRY."""
    import logging

    _, manager = _make_manager_with_full_config(
        [_make_rule("a", dry_run=True), _make_rule("b", dry_run=True)]
    )
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "dry-run summary" in m and "0 rule(s) LIVE" in m and "2 rule(s) DRY RUN" in m
        for m in msgs
    )
    # No minority list when one side is empty
    assert not any(m.startswith("recorder_tuning:   [") for m in msgs)


def test_dry_run_summary_all_live(caplog):
    """All rules live (top=false, per-rule=false) → summary shows N LIVE, 0 DRY."""
    import logging

    _, manager = _make_manager_with_full_config(
        [_make_rule("a", dry_run=False), _make_rule("b", dry_run=False)]
    )
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "dry-run summary" in m and "2 rule(s) LIVE" in m and "0 rule(s) DRY RUN" in m
        for m in msgs
    )
    assert not any(m.startswith("recorder_tuning:   [") for m in msgs)


def test_dry_run_summary_mixed_lists_live_minority(caplog):
    """Mixed with fewer LIVE than DRY → minority list labelled [LIVE] by name."""
    import logging

    rules = [
        _make_rule("alpha", dry_run=True),
        _make_rule("beta", dry_run=True),
        _make_rule("gamma", dry_run=False),  # minority
    ]
    _, manager = _make_manager_with_full_config(rules)
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "dry-run summary" in m and "1 rule(s) LIVE" in m and "2 rule(s) DRY RUN" in m
        for m in msgs
    )
    assert any("[LIVE] gamma" in m for m in msgs)
    # Majority (DRY) members should not be listed
    assert not any("[DRY RUN] alpha" in m for m in msgs)
    assert not any("[DRY RUN] beta" in m for m in msgs)


def test_dry_run_summary_mixed_lists_dry_minority(caplog):
    """Mixed with fewer DRY than LIVE → minority list labelled [DRY RUN]."""
    import logging

    rules = [
        _make_rule("alpha", dry_run=False),
        _make_rule("beta", dry_run=False),
        _make_rule("gamma", dry_run=True),  # minority
    ]
    _, manager = _make_manager_with_full_config(rules)
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "dry-run summary" in m and "2 rule(s) LIVE" in m and "1 rule(s) DRY RUN" in m
        for m in msgs
    )
    assert any("[DRY RUN] gamma" in m for m in msgs)
    assert not any("[LIVE] alpha" in m for m in msgs)


def test_dry_run_summary_tie_prefers_live(caplog):
    """Equal split → LIVE list printed (documented tie-break)."""
    import logging

    rules = [
        _make_rule("alpha", dry_run=False),
        _make_rule("beta", dry_run=True),
    ]
    _, manager = _make_manager_with_full_config(rules)
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any("[LIVE] alpha" in m for m in msgs)
    assert not any("[DRY RUN] beta" in m for m in msgs)


def test_dry_run_summary_skips_disabled_rules(caplog):
    """Disabled rules must not appear in counts or minority list."""
    import logging

    rules = [
        _make_rule("enabled_live", dry_run=False),
        _make_rule("disabled_live", dry_run=False),
    ]
    rules[1]["enabled"] = False
    _, manager = _make_manager_with_full_config(rules)
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert any(
        "dry-run summary" in m and "1 rule(s) LIVE" in m and "0 rule(s) DRY RUN" in m
        for m in msgs
    )
    assert not any("disabled_live" in m for m in msgs)


def test_dry_run_summary_minority_list_capped(caplog):
    """Minority list longer than _DRY_RUN_LOG_CAP shows cap + overflow line."""
    import logging

    from custom_components.recorder_tuning import _DRY_RUN_LOG_CAP

    # Majority LIVE so DRY becomes the minority; make DRY bigger than the cap.
    minority_size = _DRY_RUN_LOG_CAP + 3
    dry_rules = [_make_rule(f"dry_{i:03d}", dry_run=True) for i in range(minority_size)]
    live_rules = [
        _make_rule(f"live_{i:03d}", dry_run=False)
        for i in range(minority_size + 10)  # clearly majority
    ]
    _, manager = _make_manager_with_full_config(dry_rules + live_rules)
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    dry_lines = [m for m in msgs if "[DRY RUN]" in m and "dry_" in m]
    assert len(dry_lines) == _DRY_RUN_LOG_CAP
    assert any(f"…and {minority_size - _DRY_RUN_LOG_CAP} more" in m for m in msgs)


def test_dry_run_summary_empty_ruleset_logs_nothing(caplog):
    """Empty rule list (top=false) → no summary emitted."""
    import logging

    _, manager = _make_manager_with_full_config([])
    caplog.set_level(logging.INFO)
    manager._log_dry_run_summary()

    msgs = [r.message for r in caplog.records]
    assert not any("dry-run summary" in m for m in msgs)
    assert not any("locked to DRY RUN" in m for m in msgs)


# ---------------------------------------------------------------------------
# Per-rule config logging
# ---------------------------------------------------------------------------


def test_rule_config_lines_strips_empties_and_none_dry_run():
    """Empty lists and dry_run=None must not appear among the emitted lines."""
    from custom_components.recorder_tuning import _rule_config_lines

    rule = {
        "name": "r1",
        "integration_filter": [],
        "device_ids": [],
        "entity_ids": ["sensor.foo"],
        "entity_globs": [],
        "entity_regex_include": [],
        "entity_regex_exclude": [],
        "keep_days": 7,
        "enabled": True,
        "match_mode": "all",
        "dry_run": None,
    }
    lines = _rule_config_lines(rule)

    assert "entity_ids: ['sensor.foo']" in lines
    assert "keep_days: 7" in lines
    assert "match_mode: all" in lines
    # stripped
    joined = "\n".join(lines)
    assert "integration_filter" not in joined
    assert "device_ids" not in joined
    assert "entity_globs" not in joined
    assert "regex_include" not in joined
    assert "regex_exclude" not in joined
    assert "dry_run" not in joined
    # `enabled` is omitted: disabled rules never reach this log
    assert "enabled" not in joined
    # `name` is omitted: it's already in the preceding summary line
    assert "name" not in joined


def test_rule_config_lines_key_order_matches_schema():
    """Lines appear in the documented _RULE_CONFIG_LOG_KEYS order, not insertion order."""
    from custom_components.recorder_tuning import _rule_config_lines

    # Build the rule with keys shuffled; output must still be in schema order.
    rule = {
        "dry_run": True,
        "match_mode": "any",
        "enabled": True,
        "keep_days": 30,
        "entity_globs": ["sensor.zz_*"],
        "entity_ids": ["sensor.foo"],
        "name": "zr",
    }
    lines = _rule_config_lines(rule)
    keys_in_order = [line.split(":", 1)[0] for line in lines]

    assert keys_in_order == [
        "entity_ids",
        "entity_globs",
        "keep_days",
        "match_mode",
        "dry_run",
    ]


@pytest.mark.asyncio
async def test_log_purge_plan_shape_with_matches(caplog):
    """Log order: summary → config lines → entity lines; no 'config:' header."""
    import logging

    _, manager = _make_manager_with_full_config(
        [_make_rule("special_rule", dry_run=False, entity_ids=["sensor.special"])]
    )
    rule = manager.rules[0]
    caplog.set_level(logging.INFO)

    with patch(
        "custom_components.recorder_tuning._query_row_counts",
        new_callable=AsyncMock,
        return_value={"sensor.special": (42, 1700000000.0)},
    ):
        await manager._log_purge_plan(rule, ["sensor.special"], dry_run=False)

    msgs = [r.message for r in caplog.records]
    # No explicit "config:" header anymore — summary line leads
    assert not any("config:" in m for m in msgs)
    # Summary comes first
    summary_idx = next(
        i for i, m in enumerate(msgs) if "rule 'special_rule' (keep 7d)" in m
    )
    # Config field line exists and is AFTER the summary
    config_idx = next(i for i, m in enumerate(msgs) if "[PURGE]   keep_days: 7" in m)
    assert config_idx > summary_idx
    # Entity line (identified by the cutoff arrow) exists and is last
    entity_idx = next(i for i, m in enumerate(msgs) if "→" in m)
    assert entity_idx > config_idx
    # `name:` is NOT emitted (already in summary)
    assert not any("[PURGE]   name:" in m for m in msgs)


@pytest.mark.asyncio
async def test_log_purge_plan_shape_with_nothing_to_purge(caplog):
    """'nothing to purge' path still emits config lines after the summary."""
    import logging

    _, manager = _make_manager_with_full_config(
        [_make_rule("fresh_rule", dry_run=True, entity_ids=["sensor.fresh"])]
    )
    rule = manager.rules[0]
    caplog.set_level(logging.INFO)

    with patch(
        "custom_components.recorder_tuning._query_row_counts",
        new_callable=AsyncMock,
        return_value={},
    ):
        await manager._log_purge_plan(rule, ["sensor.fresh"], dry_run=True)

    msgs = [r.message for r in caplog.records]
    summary_idx = next(i for i, m in enumerate(msgs) if "nothing to purge" in m)
    config_idx = next(i for i, m in enumerate(msgs) if "[DRY RUN]   keep_days: 7" in m)
    assert config_idx > summary_idx


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
        await manager._execute_all_rules(service_dry_run=False)

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
        await manager._execute_all_rules(service_dry_run=False)

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
        await manager._execute_all_rules(service_dry_run=True)

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
        await manager._execute_all_rules(service_dry_run=False)

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
        await manager._execute_all_rules(service_dry_run=False)

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
        await manager._execute_all_rules(service_dry_run=False)

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
async def test_run_purge_now_keep_days_override_applied_to_rule():
    """keep_days service param overrides the rule's configured keep_days."""
    hass, manager = _make_manager_with_rules(
        [{"name": "rule_a", "entity_ids": ["sensor.a"], "keep_days": 14}]
    )

    call = MagicMock()
    call.data = {"rule_names": ["rule_a"], "keep_days": 1}

    seen_keep_days = []

    async def fake_log_plan(rule, entity_ids, dry_run=False):
        seen_keep_days.append(rule["keep_days"])

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", side_effect=fake_log_plan):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    assert seen_keep_days == [1]
    # Verify the service-call didn't mutate the manager's rules in place
    assert manager.rules[0]["keep_days"] == 14


@pytest.mark.asyncio
async def test_run_purge_now_keep_days_override_applies_to_all_rules_when_no_filter():
    """Without rule_names, the override applies to every rule in the run."""
    hass, manager = _make_manager_with_rules(
        [
            {"name": "rule_a", "entity_ids": ["sensor.a"], "keep_days": 14},
            {"name": "rule_b", "entity_ids": ["sensor.b"], "keep_days": 30},
        ]
    )

    call = MagicMock()
    call.data = {"keep_days": 3}

    seen_keep_days: list[int] = []

    async def fake_log_plan(rule, entity_ids, dry_run=False):
        seen_keep_days.append(rule["keep_days"])

    with patch.object(
        manager, "_resolve_entities", side_effect=lambda rule, reg: rule["entity_ids"]
    ):
        with patch.object(manager, "_log_purge_plan", side_effect=fake_log_plan):
            with patch(
                "custom_components.recorder_tuning.er.async_get",
                return_value=MagicMock(),
            ):
                await manager.async_run_purge_now(call)

    assert seen_keep_days == [3, 3]
    # Both original rules still have their configured keep_days
    assert manager.rules[0]["keep_days"] == 14
    assert manager.rules[1]["keep_days"] == 30


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
        await manager._execute_all_rules(service_dry_run=False)

    assert any(
        "recorder.purge failed" in r.message
        for r in caplog.records
        if r.levelname == "ERROR"
    )
