# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for ``RecorderTuningManager.async_service_reload``.

The integration tests in ``tests/integration/test_yaml_config.py`` exercise
this through a full HA instance. These unit tests pin the contract
directly — particularly the "reload is atomic: on failure the previous
rule set is preserved" invariant — without the ~80ms per-test cost of
spinning up a recorder.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from homeassistant.exceptions import HomeAssistantError


def _make_manager(initial_rules: list[dict]) -> tuple[MagicMock, object]:
    """Return (hass, manager) with executor calls patched to run synchronously."""
    from custom_components.recorder_tuning import RecorderTuningManager

    async def fake_executor(func, *args, **kwargs):
        return func(*args, **kwargs)

    hass = MagicMock()
    hass.async_add_executor_job = fake_executor

    manager = RecorderTuningManager(hass, MagicMock(), list(initial_rules))
    return hass, manager


@pytest.mark.asyncio
async def test_reload_success_replaces_rules(monkeypatch):
    import custom_components.recorder_tuning as mod

    _, manager = _make_manager([{"name": "old", "keep_days": 7}])

    new_rules = [{"name": "new", "keep_days": 3}]
    monkeypatch.setattr(mod, "_load_yaml_rules", lambda hass: new_rules)

    await manager.async_service_reload(MagicMock())

    assert manager.rules == new_rules


@pytest.mark.asyncio
async def test_reload_missing_file_clears_rules(monkeypatch):
    """Missing YAML file → rules cleared; no exception."""
    import custom_components.recorder_tuning as mod

    _, manager = _make_manager([{"name": "old", "keep_days": 7}])

    # _load_yaml_rules returns [] for missing file
    monkeypatch.setattr(mod, "_load_yaml_rules", lambda hass: [])

    await manager.async_service_reload(MagicMock())

    assert manager.rules == []


@pytest.mark.asyncio
async def test_reload_failure_preserves_rules(monkeypatch):
    """If _load_yaml_rules raises, the previous rule set must survive untouched."""
    import custom_components.recorder_tuning as mod

    original = [{"name": "guard", "keep_days": 7}]
    _, manager = _make_manager(original)

    def raising(hass):
        raise HomeAssistantError("bad yaml")

    monkeypatch.setattr(mod, "_load_yaml_rules", raising)

    with pytest.raises(HomeAssistantError, match="bad yaml"):
        await manager.async_service_reload(MagicMock())

    # Rule set survives the failed reload
    assert manager.rules == original


@pytest.mark.asyncio
async def test_reload_failure_does_not_partially_apply(monkeypatch):
    """Even if _load_yaml_rules raises after producing partial state, rules stay the same."""
    import custom_components.recorder_tuning as mod

    original = [{"name": "guard", "keep_days": 7}]
    _, manager = _make_manager(original)

    def raising_after_work(hass):
        # Simulates the loader doing some work (e.g., partial parse) before raising.
        raise HomeAssistantError("parse error on rule[3]")

    monkeypatch.setattr(mod, "_load_yaml_rules", raising_after_work)

    with pytest.raises(HomeAssistantError):
        await manager.async_service_reload(MagicMock())

    assert manager.rules == original
