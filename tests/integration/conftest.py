# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Shared fixtures and helpers for integration tests.

These tests spin up a real in-process Home Assistant instance with a
file-backed SQLite recorder.  No network connections are made — HA runs
entirely in the test event loop without DNS, mDNS, Bluetooth, or any
external service.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import pytest
from homeassistant.helpers import entity_registry as er
from homeassistant.components.recorder.db_schema import (
    Statistics,
    StatisticsMeta,
    StatisticsShortTerm,
    States,
    StatesMeta,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import session_scope
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_purge_done,
    async_wait_recording_done,
)

from custom_components.recorder_tuning.const import (
    CONF_DRY_RUN,
    CONF_PURGE_TIME,
    CONF_STATS_KEEP_DAYS,
    DOMAIN,
    STORAGE_KEY,
    STORAGE_VERSION,
)

# ---------------------------------------------------------------------------
# Fixed time reference — all tests are anchored relative to this instant.
# ---------------------------------------------------------------------------

NOW = datetime(2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc)

# Recorder global purge window (days of raw-state history to keep)
PURGE_KEEP_DAYS = 5
# Our extension — short-term statistics retention window
STATS_KEEP_DAYS = 30


# ---------------------------------------------------------------------------
# Fixture: override recorder_config so recorder_mock uses our settings
# ---------------------------------------------------------------------------


@pytest.fixture
def recorder_config() -> dict[str, Any]:
    """Use a short purge window and no commit delay for deterministic tests."""
    return {
        "purge_keep_days": PURGE_KEEP_DAYS,
        "commit_interval": 0,
    }


@pytest.fixture
def mock_recorder_before_hass(async_test_recorder: Any) -> Any:
    """Start the recorder before the hass fixture initializes.

    The framework asserts that ``recorder_db_url`` is set up before ``hass``
    marks itself as initialized.  Overriding ``mock_recorder_before_hass``
    (which is a dependency of ``hass``) ensures the recorder's DB URL and
    engine are prepared in the right order.
    """
    return async_test_recorder


# ---------------------------------------------------------------------------
# Fixture: HA instance with recorder, time frozen at NOW
# ---------------------------------------------------------------------------


@pytest.fixture
async def recorder_hass(
    hass: HomeAssistant,
    recorder_mock: Any,
    enable_custom_integrations: None,
    freezer: Any,
) -> AsyncGenerator[HomeAssistant, None]:
    """HA instance with recorder running, clock frozen at NOW.

    ``recorder_mock`` starts the recorder using ``recorder_config`` above.
    ``enable_custom_integrations`` allows loading our custom component.
    No outbound network calls are made — the recorder uses a temporary
    SQLite file and no network-dependent components are loaded.
    """
    freezer.move_to(NOW)
    await async_wait_recording_done(hass)
    yield hass


# ---------------------------------------------------------------------------
# Fixture: recorder_hass with recorder_tuning integration loaded
# ---------------------------------------------------------------------------


@pytest.fixture
async def integration_entry(
    recorder_hass: HomeAssistant,
) -> AsyncGenerator[tuple[HomeAssistant, MockConfigEntry], None]:
    """Load the recorder_tuning integration on top of recorder_hass."""
    hass = recorder_hass
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_PURGE_TIME: "03:00",
            CONF_STATS_KEEP_DAYS: STATS_KEEP_DAYS,
            CONF_DRY_RUN: False,  # tests that call run_purge_now expect live purges
        },
    )
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    yield hass, entry


# ---------------------------------------------------------------------------
# Helpers: recorder wait
# ---------------------------------------------------------------------------


async def wait_for_recorder(hass: HomeAssistant) -> None:
    """Wait for the recorder thread to fully process and commit."""
    await async_wait_recording_done(hass)


async def wait_for_purge(hass: HomeAssistant) -> None:
    """Wait for a recorder purge cycle to complete."""
    await async_wait_purge_done(hass)


# ---------------------------------------------------------------------------
# Helpers: rule management
# ---------------------------------------------------------------------------


async def configure_rules(hass: HomeAssistant, rules: list[dict]) -> None:
    """Write rules to the storage store and push them to the live manager."""
    from homeassistant.helpers import storage  # noqa: PLC0415

    store = storage.Store(hass, STORAGE_VERSION, STORAGE_KEY)
    await store.async_save({"rules": rules})
    for obj in hass.data.get(DOMAIN, {}).values():
        if hasattr(obj, "rules"):
            obj.rules = list(rules)


def set_dry_run(hass: HomeAssistant, enabled: bool) -> None:
    """Update dry_run on the live config entry."""
    entries = hass.config_entries.async_entries(DOMAIN)
    if not entries:
        raise RuntimeError("recorder_tuning config entry not found")
    entry = entries[0]
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_DRY_RUN: enabled}
    )


def set_stats_keep_days(hass: HomeAssistant, days: int) -> None:
    """Update stats_keep_days on the live config entry.

    The patch closure reads this value from the config entry at call time,
    so the new value takes effect on the very next purge without reloading.
    """
    entries = hass.config_entries.async_entries(DOMAIN)
    if not entries:
        raise RuntimeError("recorder_tuning config entry not found")
    entry = entries[0]
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_STATS_KEEP_DAYS: days}
    )


# ---------------------------------------------------------------------------
# Helpers: write a state at a specific past time
# ---------------------------------------------------------------------------


async def set_state_at(
    hass: HomeAssistant,
    entity_id: str,
    state: str,
    ts: datetime,
    freezer: Any,
    platform: str = "test_integration",
) -> None:
    """Register an entity and set its state, timestamped at ``ts``.

    Entities must be in the entity registry for glob/integration-filter
    resolution to find them.  The platform defaults to ``test_integration``
    so integration-filter tests can override it.
    """
    ent_reg = er.async_get(hass)
    domain, _, unique_id = entity_id.partition(".")
    if not ent_reg.async_get(entity_id):
        ent_reg.async_get_or_create(
            domain,
            platform,
            unique_id,
            suggested_object_id=unique_id,
        )
        await hass.async_block_till_done()

    freezer.move_to(ts)
    hass.states.async_set(entity_id, state)
    await wait_for_recorder(hass)
    freezer.move_to(NOW)


# ---------------------------------------------------------------------------
# Helpers: DB query utilities
# ---------------------------------------------------------------------------


def count_states(hass: HomeAssistant, entity_id: str) -> int:
    """Return the number of ``States`` rows for *entity_id*."""
    with session_scope(hass=hass) as session:
        meta = (
            session.query(StatesMeta).filter(StatesMeta.entity_id == entity_id).first()
        )
        if meta is None:
            return 0
        return (
            session.query(States).filter(States.metadata_id == meta.metadata_id).count()
        )


def count_short_term_stats(hass: HomeAssistant, statistic_id: str) -> int:
    """Return the number of ``StatisticsShortTerm`` rows for *statistic_id*."""
    with session_scope(hass=hass) as session:
        meta = (
            session.query(StatisticsMeta)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .first()
        )
        if meta is None:
            return 0
        return (
            session.query(StatisticsShortTerm)
            .filter(StatisticsShortTerm.metadata_id == meta.id)
            .count()
        )


def insert_short_term_stat(
    hass: HomeAssistant,
    statistic_id: str,
    ts: datetime,
    value: float = 1.0,
) -> None:
    """Insert a ``StatisticsShortTerm`` row directly at a specific timestamp.

    Also creates the ``StatisticsMeta`` entry if it does not exist.
    """
    ts_float = ts.timestamp()
    with session_scope(hass=hass) as session:
        meta = (
            session.query(StatisticsMeta)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .first()
        )
        if meta is None:
            meta = StatisticsMeta(
                statistic_id=statistic_id,
                source="recorder",
                unit_of_measurement="W",
                has_mean=True,
                has_sum=False,
                name=statistic_id,
            )
            session.add(meta)
            session.flush()

        session.add(
            StatisticsShortTerm(
                metadata_id=meta.id,
                created_ts=ts_float,
                start_ts=ts_float,
                mean=value,
            )
        )


def insert_long_term_stat(
    hass: HomeAssistant,
    statistic_id: str,
    ts: datetime,
    value: float = 1.0,
) -> None:
    """Insert a ``Statistics`` (long-term, hourly) row directly at a specific timestamp.

    Also creates the ``StatisticsMeta`` entry if it does not exist.
    """
    ts_float = ts.timestamp()
    with session_scope(hass=hass) as session:
        meta = (
            session.query(StatisticsMeta)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .first()
        )
        if meta is None:
            meta = StatisticsMeta(
                statistic_id=statistic_id,
                source="recorder",
                unit_of_measurement="W",
                has_mean=True,
                has_sum=False,
                name=statistic_id,
            )
            session.add(meta)
            session.flush()

        session.add(
            Statistics(
                metadata_id=meta.id,
                created_ts=ts_float,
                start_ts=ts_float,
                mean=value,
            )
        )


def count_long_term_stats(hass: HomeAssistant, statistic_id: str) -> int:
    """Return the number of ``Statistics`` (long-term) rows for *statistic_id*."""
    with session_scope(hass=hass) as session:
        meta = (
            session.query(StatisticsMeta)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .first()
        )
        if meta is None:
            return 0
        return (
            session.query(Statistics).filter(Statistics.metadata_id == meta.id).count()
        )
