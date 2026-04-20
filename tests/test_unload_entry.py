# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Unit tests for ``async_unload_entry`` — specifically the wrapper-tag
branches that decide whether to restore the original recorder purge
function or leave it alone.

Integration tests cover the happy path (our wrapper is in place → we
restore). These unit tests pin the less-happy paths that are hard to
construct in a live HA instance.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _install_fake_recorder_purge(monkeypatch, current_fn):
    """Put a fake recorder.purge module on the real HA module's attribute.

    ``async_unload_entry`` does ``from homeassistant.components.recorder
    import purge as recorder_purge`` — Python resolves that via the real
    parent package's ``purge`` attribute. ``monkeypatch.setattr`` on the
    real module's ``find_short_term_statistics_to_purge`` is what matters.
    """
    from homeassistant.components.recorder import purge as recorder_purge

    monkeypatch.setattr(
        recorder_purge, "find_short_term_statistics_to_purge", current_fn
    )
    return recorder_purge


def _hass_with_domain_data(data: dict) -> MagicMock:
    hass = MagicMock()
    hass.data = {"recorder_tuning": dict(data)}
    hass.services.async_remove = MagicMock()
    return hass


@pytest.mark.asyncio
async def test_unload_restores_when_our_wrapper_is_installed(monkeypatch):
    """Happy path: our tagged wrapper is still in place → restore original."""
    from custom_components.recorder_tuning import (
        _ORIG_PURGE_FN_KEY,
        _STATS_KEEP_DAYS_KEY,
        _WRAPPER_TAG,
        async_unload_entry,
    )

    def original(purge_before, max_bind_vars):
        return None

    def our_wrapper(purge_before, max_bind_vars):
        return None

    our_wrapper.__dict__[_WRAPPER_TAG] = True

    recorder_purge = _install_fake_recorder_purge(monkeypatch, our_wrapper)

    entry = MagicMock()
    entry.entry_id = "entry_1"
    hass = _hass_with_domain_data(
        {
            _ORIG_PURGE_FN_KEY: original,
            _STATS_KEEP_DAYS_KEY: 30,
            "entry_1": MagicMock(async_unload=MagicMock()),
        }
    )

    result = await async_unload_entry(hass, entry)
    assert result is True
    # Restored to the real original
    assert recorder_purge.find_short_term_statistics_to_purge is original
    # Both cached keys cleaned up
    assert _ORIG_PURGE_FN_KEY not in hass.data["recorder_tuning"]
    assert _STATS_KEEP_DAYS_KEY not in hass.data["recorder_tuning"]


@pytest.mark.asyncio
async def test_unload_leaves_alone_when_something_else_rewrapped(monkeypatch, caplog):
    """If a third party rewrapped on top of ours, restoring would drop their layer."""
    import logging

    from custom_components.recorder_tuning import (
        _ORIG_PURGE_FN_KEY,
        _STATS_KEEP_DAYS_KEY,
        async_unload_entry,
    )

    def original(purge_before, max_bind_vars):
        return None

    def not_ours(purge_before, max_bind_vars):
        """Tag-less wrapper installed by a hypothetical other integration."""
        return None

    recorder_purge = _install_fake_recorder_purge(monkeypatch, not_ours)

    entry = MagicMock()
    entry.entry_id = "entry_1"
    hass = _hass_with_domain_data(
        {
            _ORIG_PURGE_FN_KEY: original,
            _STATS_KEEP_DAYS_KEY: 30,
            "entry_1": MagicMock(async_unload=MagicMock()),
        }
    )

    caplog.set_level(logging.WARNING)
    result = await async_unload_entry(hass, entry)

    assert result is True
    # Did NOT restore — leave the tag-less wrapper in place
    assert recorder_purge.find_short_term_statistics_to_purge is not_ours
    # Still cleaned up our cached keys
    assert _ORIG_PURGE_FN_KEY not in hass.data["recorder_tuning"]
    assert _STATS_KEEP_DAYS_KEY not in hass.data["recorder_tuning"]
    assert "re-wrapped by something else" in caplog.text


@pytest.mark.asyncio
async def test_unload_noop_when_no_original_stored(monkeypatch):
    """If we never successfully applied the patch, unload touches nothing."""
    from custom_components.recorder_tuning import async_unload_entry

    def untouched(purge_before, max_bind_vars):
        return None

    recorder_purge = _install_fake_recorder_purge(monkeypatch, untouched)

    entry = MagicMock()
    entry.entry_id = "entry_1"
    hass = _hass_with_domain_data({"entry_1": MagicMock(async_unload=MagicMock())})

    result = await async_unload_entry(hass, entry)

    assert result is True
    assert recorder_purge.find_short_term_statistics_to_purge is untouched
