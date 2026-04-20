# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for the reload service handler.

The integration tests in ``tests/integration/test_yaml_config.py`` exercise
this through a full HA instance. These unit tests pin the contract directly
— particularly the "reload is atomic: on failure the previous rule set is
preserved" invariant — without the ~80ms per-test cost of spinning up a
recorder.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from homeassistant.exceptions import HomeAssistantError


def _make_manager(initial_rules):
    """Return a manager whose rules start as ``initial_rules``."""
    from custom_components.recorder_tuning import RecorderTuningManager

    hass = MagicMock()
    config = {"purge_time": "03:00", "dry_run": False, "rules": list(initial_rules)}
    manager = RecorderTuningManager(hass, config)
    return hass, manager


@pytest.mark.asyncio
async def test_reload_success_replaces_rules(monkeypatch):
    """A successful reload swaps in the new rule set."""
    import custom_components.recorder_tuning as mod

    hass, manager = _make_manager([{"name": "old", "keep_days": 7}])

    new_rules = [
        {
            "name": "new",
            "keep_days": 3,
            "entity_ids": ["sensor.x"],
        }
    ]
    new_conf = {
        "recorder_tuning": {
            "purge_time": "03:00",
            "stats_keep_days": 30,
            "dry_run": False,
            "rules": new_rules,
        }
    }

    monkeypatch.setattr(mod, "async_hass_config_yaml", AsyncMock(return_value=new_conf))
    monkeypatch.setattr(mod, "_apply_stats_patch", MagicMock())

    handler = mod._make_reload_handler(hass, manager)
    await handler(MagicMock())

    # CONFIG_SCHEMA fills in default fields on each rule; compare by name/keep_days
    assert [r["name"] for r in manager.rules] == ["new"]
    assert manager.rules[0]["keep_days"] == 3


@pytest.mark.asyncio
async def test_reload_without_domain_key_raises_and_preserves_rules(monkeypatch):
    """Reload when recorder_tuning: is not in the reloaded YAML raises."""
    import custom_components.recorder_tuning as mod

    hass, manager = _make_manager([{"name": "guard", "keep_days": 7}])

    monkeypatch.setattr(mod, "async_hass_config_yaml", AsyncMock(return_value={}))
    monkeypatch.setattr(mod, "_apply_stats_patch", MagicMock())

    handler = mod._make_reload_handler(hass, manager)

    with pytest.raises(HomeAssistantError, match="no recorder_tuning:"):
        await handler(MagicMock())

    # Previous rule set must survive
    assert manager.rules == [{"name": "guard", "keep_days": 7}]


@pytest.mark.asyncio
async def test_reload_yaml_error_propagates_and_preserves_rules(monkeypatch):
    """A YAML parse error surfaces as HomeAssistantError; rules are untouched."""
    import custom_components.recorder_tuning as mod

    hass, manager = _make_manager([{"name": "guard", "keep_days": 7}])

    async def raising(hass_arg):
        raise HomeAssistantError("bad yaml")

    monkeypatch.setattr(mod, "async_hass_config_yaml", raising)

    handler = mod._make_reload_handler(hass, manager)

    with pytest.raises(HomeAssistantError, match="bad yaml"):
        await handler(MagicMock())

    # Rule set survives the failed reload
    assert manager.rules == [{"name": "guard", "keep_days": 7}]


@pytest.mark.asyncio
async def test_reload_schema_error_raises_and_preserves_rules(monkeypatch):
    """A schema violation in the reloaded config raises and preserves rules."""
    import custom_components.recorder_tuning as mod

    hass, manager = _make_manager([{"name": "guard", "keep_days": 7}])

    bad_conf = {
        "recorder_tuning": {
            "rules": [{"name": "oops", "keep_days": 999999}],  # out of range
        }
    }
    monkeypatch.setattr(mod, "async_hass_config_yaml", AsyncMock(return_value=bad_conf))
    monkeypatch.setattr(mod, "_apply_stats_patch", MagicMock())

    handler = mod._make_reload_handler(hass, manager)

    with pytest.raises(HomeAssistantError, match="invalid configuration"):
        await handler(MagicMock())

    # Rule set survives the failed reload
    assert manager.rules == [{"name": "guard", "keep_days": 7}]
