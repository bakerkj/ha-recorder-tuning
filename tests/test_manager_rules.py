# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for RecorderTuningManager rule-persistence helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_manager():
    """Build a manager with an AsyncMock store so persistence calls are awaitable."""
    from custom_components.recorder_tuning import RecorderTuningManager

    hass = MagicMock()
    entry = MagicMock()
    store = MagicMock()
    store.async_save = AsyncMock()
    return RecorderTuningManager(hass, entry, store, {"rules": []})


@pytest.mark.asyncio
async def test_async_replace_rules_updates_memory_and_persists():
    manager = _make_manager()

    new_rules = [
        {"name": "rule_a", "keep_days": 7, "enabled": True},
        {"name": "rule_b", "keep_days": 3, "enabled": False},
    ]
    await manager.async_replace_rules(new_rules)

    # In-memory state reflects the new rules
    assert manager.rules == new_rules
    # And they were persisted exactly once with the right payload
    manager.store.async_save.assert_awaited_once_with({"rules": new_rules})


@pytest.mark.asyncio
async def test_async_replace_rules_copies_input_list():
    """Caller mutations to the passed list must not leak into the manager."""
    manager = _make_manager()
    rules = [{"name": "rule_a", "keep_days": 7, "enabled": True}]
    await manager.async_replace_rules(rules)

    rules.append({"name": "rule_b", "keep_days": 3, "enabled": True})

    assert len(manager.rules) == 1
    assert manager.rules[0]["name"] == "rule_a"


@pytest.mark.asyncio
async def test_async_replace_rules_with_empty_clears():
    manager = _make_manager()
    manager.rules = [{"name": "stale", "keep_days": 7, "enabled": True}]

    await manager.async_replace_rules([])

    assert manager.rules == []
    manager.store.async_save.assert_awaited_once_with({"rules": []})
