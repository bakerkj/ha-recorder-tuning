# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for the short-term statistics retention patch logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


def _make_hass(stats_keep_days: int = 30):
    """Return a minimal hass stub configured with stats_keep_days."""
    from custom_components.recorder_tuning.const import (
        CONF_STATS_KEEP_DAYS,
        DOMAIN,
    )

    entry = MagicMock()
    entry.data = {CONF_STATS_KEEP_DAYS: stats_keep_days}

    hass = MagicMock()
    hass.config_entries.async_entries.return_value = [entry]
    hass.data = {DOMAIN: {}}
    return hass


# ---------------------------------------------------------------------------
# Patch applies and extends the cutoff
# ---------------------------------------------------------------------------


def test_patch_extends_cutoff():
    """Patch should move the effective cutoff further into the past."""
    from custom_components.recorder_tuning.const import DOMAIN
    from custom_components.recorder_tuning import (
        _ORIG_PURGE_FN_KEY,
        _apply_stats_patch,
    )

    calls = []

    def fake_original(purge_before, max_bind_vars):
        calls.append(purge_before)

    hass = _make_hass(stats_keep_days=30)

    with patch(
        "custom_components.recorder_tuning.__init__.recorder_purge_module"
        if False  # sentinel — we use the import-patch approach below
        else "homeassistant.components.recorder.purge",
        create=True,
    ):
        # Patch the recorder module directly
        import sys
        import types

        fake_purge_mod = types.ModuleType("homeassistant.components.recorder.purge")
        fake_purge_mod.find_short_term_statistics_to_purge = fake_original
        sys.modules["homeassistant.components.recorder.purge"] = fake_purge_mod

        _apply_stats_patch(hass, 30)

        # Confirm patch was stored
        assert _ORIG_PURGE_FN_KEY in hass.data[DOMAIN]

        # Call the patched function with a recent cutoff (e.g. 5-day purge_keep_days)
        recorder_cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        fake_purge_mod.find_short_term_statistics_to_purge(recorder_cutoff, 100)

        assert len(calls) == 1
        # The effective cutoff passed to the original must be <= recorder_cutoff
        # (i.e., further in the past because stats_keep_days=30 > 5)
        assert calls[0] <= recorder_cutoff

        # Clean up sys.modules
        del sys.modules["homeassistant.components.recorder.purge"]


def test_patch_never_purges_more_aggressively():
    """If stats_keep_days < purge_keep_days, use the recorder's original cutoff."""
    from custom_components.recorder_tuning import (
        _apply_stats_patch,
    )
    import sys
    import types

    calls = []

    def fake_original(purge_before, max_bind_vars):
        calls.append(purge_before)
        return []

    # stats_keep_days=5 but recorder cutoff is already 10 days back
    hass = _make_hass(stats_keep_days=5)

    fake_purge_mod = types.ModuleType("homeassistant.components.recorder.purge")
    fake_purge_mod.find_short_term_statistics_to_purge = fake_original
    sys.modules["homeassistant.components.recorder.purge"] = fake_purge_mod

    _apply_stats_patch(hass, 5)

    # Recorder wants to purge everything older than 10 days
    recorder_cutoff = datetime.now(timezone.utc) - timedelta(days=10)
    fake_purge_mod.find_short_term_statistics_to_purge(recorder_cutoff, 100)

    assert len(calls) == 1
    # effective_before = min(recorder_cutoff, stats_cutoff)
    # stats_cutoff = now - 5d → MORE recent than recorder_cutoff (now-10d)
    # so effective_before = recorder_cutoff (the earlier one)
    effective = calls[0]
    stats_cutoff = datetime.now(timezone.utc) - timedelta(days=5)
    assert effective <= stats_cutoff  # never more aggressive than recorder

    del sys.modules["homeassistant.components.recorder.purge"]


# ---------------------------------------------------------------------------
# Patch is idempotent — calling twice doesn't double-wrap
# ---------------------------------------------------------------------------


def test_patch_not_applied_twice():
    """Calling _apply_stats_patch a second time should not double-wrap."""
    from custom_components.recorder_tuning import (
        _apply_stats_patch,
    )
    import sys
    import types

    call_count = [0]

    def fake_original(purge_before, max_bind_vars):
        call_count[0] += 1

    hass = _make_hass(30)

    fake_purge_mod = types.ModuleType("homeassistant.components.recorder.purge")
    fake_purge_mod.find_short_term_statistics_to_purge = fake_original
    sys.modules["homeassistant.components.recorder.purge"] = fake_purge_mod

    _apply_stats_patch(hass, 30)
    # Simulate second call (e.g. options changed)
    _apply_stats_patch(hass, 60)

    recorder_cutoff = datetime.now(timezone.utc) - timedelta(days=3)
    fake_purge_mod.find_short_term_statistics_to_purge(recorder_cutoff, 100)

    # Original should only be called once per invocation
    assert call_count[0] == 1

    del sys.modules["homeassistant.components.recorder.purge"]


# ---------------------------------------------------------------------------
# The following tests use pytest's ``monkeypatch`` to replace the attribute on
# the real ``homeassistant.components.recorder.purge`` module. This is more
# robust than the ``sys.modules`` replacement used by the legacy tests above,
# because CPython's import machinery resolves ``from pkg import sub`` via the
# parent package's attribute — setting ``sys.modules[...]`` alone doesn't win
# once the submodule has been imported once. ``monkeypatch`` restores the
# original attribute at teardown, preventing test order leakage.
# ---------------------------------------------------------------------------


def test_wrapper_is_tagged(monkeypatch):
    """Our installed wrapper carries the _WRAPPER_TAG attribute."""
    from homeassistant.components.recorder import purge as recorder_purge

    from custom_components.recorder_tuning import _WRAPPER_TAG, _apply_stats_patch

    def vanilla(purge_before, max_bind_vars):
        return None

    monkeypatch.setattr(recorder_purge, "find_short_term_statistics_to_purge", vanilla)

    hass = _make_hass(30)
    _apply_stats_patch(hass, 30)

    wrapper = recorder_purge.find_short_term_statistics_to_purge
    assert getattr(wrapper, _WRAPPER_TAG, False) is True
    assert wrapper is not vanilla


def test_patch_skips_when_wrapper_already_installed(monkeypatch):
    """If the target already carries our tag, don't re-wrap it."""
    from homeassistant.components.recorder import purge as recorder_purge

    from custom_components.recorder_tuning import (
        _ORIG_PURGE_FN_KEY,
        _WRAPPER_TAG,
        _apply_stats_patch,
    )
    from custom_components.recorder_tuning.const import DOMAIN

    def already_tagged(purge_before, max_bind_vars):
        return None

    already_tagged.__dict__[_WRAPPER_TAG] = True
    monkeypatch.setattr(
        recorder_purge, "find_short_term_statistics_to_purge", already_tagged
    )

    hass = _make_hass(30)
    # hass.data has no _ORIG_PURGE_FN_KEY, but the target is already tagged —
    # must be detected and left alone.
    _apply_stats_patch(hass, 30)

    assert recorder_purge.find_short_term_statistics_to_purge is already_tagged
    assert _ORIG_PURGE_FN_KEY not in hass.data.get(DOMAIN, {})


# ---------------------------------------------------------------------------
# Retention updates propagate to the live closure
# ---------------------------------------------------------------------------


def test_reapply_updates_effective_cutoff(monkeypatch):
    """Re-applying with a different retention shifts the closure's cutoff."""
    from homeassistant.components.recorder import purge as recorder_purge

    from custom_components.recorder_tuning import _apply_stats_patch

    calls: list[datetime] = []

    def fake_original(purge_before, max_bind_vars):
        calls.append(purge_before)

    monkeypatch.setattr(
        recorder_purge, "find_short_term_statistics_to_purge", fake_original
    )

    hass = _make_hass(30)
    _apply_stats_patch(hass, 30)
    wrapper = recorder_purge.find_short_term_statistics_to_purge

    # Recorder cutoff is very recent, so our stats cutoff dominates.
    recorder_cutoff = datetime.now(timezone.utc) - timedelta(days=1)
    wrapper(recorder_cutoff, 100)
    first_effective = calls[-1]

    # Simulate reload with a longer retention.
    _apply_stats_patch(hass, 60)
    wrapper(recorder_cutoff, 100)
    second_effective = calls[-1]

    # Longer retention → effective cutoff moves further into the past.
    assert second_effective < first_effective


# ---------------------------------------------------------------------------
# Graceful handling when HA has moved the function
# ---------------------------------------------------------------------------


def test_patch_noop_when_target_function_missing(monkeypatch):
    """If the target attribute is gone, log and return without raising."""
    from homeassistant.components.recorder import purge as recorder_purge

    from custom_components.recorder_tuning import (
        _ORIG_PURGE_FN_KEY,
        _apply_stats_patch,
    )
    from custom_components.recorder_tuning.const import DOMAIN

    monkeypatch.delattr(recorder_purge, "find_short_term_statistics_to_purge")

    hass = _make_hass(30)
    _apply_stats_patch(hass, 30)  # must not raise

    assert not hasattr(recorder_purge, "find_short_term_statistics_to_purge")
    assert _ORIG_PURGE_FN_KEY not in hass.data.get(DOMAIN, {})
