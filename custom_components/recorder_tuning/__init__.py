# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Recorder Tuning - per-entity purge rules and short-term statistics retention."""

from __future__ import annotations

import fnmatch
import logging
import os
import re
from datetime import datetime, time, timedelta, timezone
from typing import Any

import voluptuous as vol
import yaml

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_time_change

from .const import (
    CONF_DEVICE_IDS,
    CONF_DRY_RUN,
    DEFAULT_DRY_RUN,
    CONF_ENABLED,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_INTEGRATION_FILTER,
    CONF_KEEP_DAYS,
    CONF_PURGE_TIME,
    CONF_RULE_NAME,
    CONF_RULES,
    CONF_STATS_KEEP_DAYS,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
    YAML_CONFIG_FILE,
)

_LOGGER = logging.getLogger(__name__)

# Key used to stash the original purge function so we can restore it on unload
_ORIG_PURGE_FN_KEY = f"{DOMAIN}_original_purge_fn"
# Cached stats_keep_days — written from the event loop (setup/reload), read
# from the recorder executor thread by the patched closure. Dict reads are
# atomic under the CPython GIL, so no lock is required.
_STATS_KEEP_DAYS_KEY = f"{DOMAIN}_stats_keep_days"
# Attribute tag on our wrapper so we can recognise it on unload/reload and
# avoid wrapping ourselves twice, or unwrapping someone else's patch.
_WRAPPER_TAG = f"__{DOMAIN}_wrapped__"
# Batch size for recorder.purge_entities service calls and the matching
# per-entity row-count query used in dry-run / pre-purge logging. Kept well
# under SQLite's default max-variables limit (999).
_PURGE_BATCH_SIZE = 100
# Maximum number of per-entity log lines emitted at INFO in dry-run mode.
# Extra entities are summarised as "…and N more" to keep the log readable on
# large installations. The full list is always available at DEBUG.
_DRY_RUN_LOG_CAP = 25

# Voluptuous schema for a single rule loaded from YAML
_RULE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_RULE_NAME): str,
        vol.Optional(CONF_INTEGRATION_FILTER, default=[]): [str],
        vol.Optional(CONF_DEVICE_IDS, default=[]): [str],
        vol.Optional(CONF_ENTITY_IDS, default=[]): [str],
        vol.Optional(CONF_ENTITY_GLOBS, default=[]): [str],
        vol.Optional(CONF_ENTITY_REGEX_INCLUDE, default=[]): [str],
        vol.Optional(CONF_ENTITY_REGEX_EXCLUDE, default=[]): [str],
        vol.Required(CONF_KEEP_DAYS): vol.All(int, vol.Range(min=1, max=365)),
        vol.Optional(CONF_ENABLED, default=True): bool,
    }
)


def _parse_hhmm(value: str) -> time:
    """Parse a ``HH:MM`` string into a ``time`` object.

    Raises ``ValueError`` (or ``TypeError`` if ``value`` isn't a string) on
    malformed input — callers decide whether to treat that as a validation
    failure (config flow) or a warning plus default (scheduler).
    """
    return datetime.strptime(value, "%H:%M").time()


def _load_yaml_rules(hass: HomeAssistant) -> list[dict]:
    """Load and validate rules from recorder_tuning.yaml.

    Returns:
        - ``[]`` if the file does not exist (legitimate "no rules configured"
          state — the integration still sets up and is ready for a reload
          once the file is created).
        - The list of valid rules if the file parses. Individual rules that
          fail schema validation are skipped with a WARNING; other valid
          rules in the same file are still returned.

    Raises:
        HomeAssistantError — the file exists but is unreadable, contains
        invalid YAML, or has the wrong top-level shape. The reload service
        surfaces this to the caller so typos don't silently wipe the rule
        set; setup catches it so a broken file doesn't block the
        integration from loading.
    """
    yaml_path = hass.config.path(YAML_CONFIG_FILE)
    if not os.path.isfile(yaml_path):
        return []

    try:
        with open(yaml_path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except OSError as err:
        raise HomeAssistantError(
            f"recorder_tuning: could not read {yaml_path}: {err}"
        ) from err
    except yaml.YAMLError as err:
        raise HomeAssistantError(
            f"recorder_tuning: YAML parse error in {yaml_path}: {err}"
        ) from err

    if not isinstance(raw, dict) or CONF_RULES not in raw:
        raise HomeAssistantError(
            f"recorder_tuning: {yaml_path} must contain a top-level 'rules:' list"
        )

    if not isinstance(raw[CONF_RULES], list):
        raise HomeAssistantError(
            f"recorder_tuning: {yaml_path} — 'rules:' must be a YAML list"
        )

    rules: list[dict] = []
    for i, rule_raw in enumerate(raw[CONF_RULES]):
        try:
            rules.append(_RULE_SCHEMA(rule_raw))
        except vol.Invalid as err:
            _LOGGER.warning(
                "recorder_tuning: skipping rule[%d] in %s — validation error: %s",
                i,
                yaml_path,
                err,
            )

    # Callers (async_setup_entry, async_service_reload) emit their own summary
    # log with schedule/context — don't double-log the rule count here.
    return rules


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Recorder Tuning from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    stats_keep_days = entry.data.get(CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS)
    _apply_stats_patch(hass, stats_keep_days)

    try:
        yaml_rules = await hass.async_add_executor_job(_load_yaml_rules, hass)
    except HomeAssistantError as err:
        # Broken YAML at startup must not prevent the integration from loading —
        # the user can fix the file and call recorder_tuning.reload.
        _LOGGER.error(
            "recorder_tuning: YAML config is invalid at setup, starting with "
            "no rules active: %s",
            err,
        )
        yaml_rules = []
    if not yaml_rules:
        _LOGGER.info(
            "recorder_tuning: no active rules. Edit %s in the config dir and "
            "call recorder_tuning.reload to enable.",
            YAML_CONFIG_FILE,
        )

    manager = RecorderTuningManager(hass, entry, yaml_rules)
    hass.data[DOMAIN][entry.entry_id] = manager

    await manager.async_setup()

    hass.services.async_register(
        DOMAIN,
        "run_purge_now",
        manager.async_run_purge_now,
        schema=vol.Schema({vol.Optional(CONF_DRY_RUN): bool}),
    )
    hass.services.async_register(
        DOMAIN,
        "reload",
        manager.async_service_reload,
        schema=vol.Schema({}),
    )

    entry.async_on_unload(entry.add_update_listener(manager.async_reload))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    domain_data: dict = hass.data.get(DOMAIN, {})
    manager: RecorderTuningManager | None = domain_data.pop(entry.entry_id, None)
    if manager:
        manager.async_unload()

    # Restore the original purge function only if our wrapper is still the
    # one installed. If something else has wrapped on top, leave it in place
    # — unwrapping would drop their layer on the floor.
    original_fn = domain_data.pop(_ORIG_PURGE_FN_KEY, None)
    domain_data.pop(_STATS_KEEP_DAYS_KEY, None)
    if original_fn is not None:
        try:
            from homeassistant.components.recorder import purge as recorder_purge  # noqa: PLC0415
        except ImportError as err:
            _LOGGER.warning(
                "recorder_tuning: could not restore purge function: %s", err
            )
        else:
            current = getattr(
                recorder_purge, "find_short_term_statistics_to_purge", None
            )
            if current is not None and getattr(current, _WRAPPER_TAG, False):
                recorder_purge.find_short_term_statistics_to_purge = original_fn
                _LOGGER.info("recorder_tuning: short-term stats patch removed")
            else:
                _LOGGER.warning(
                    "recorder_tuning: find_short_term_statistics_to_purge has "
                    "been re-wrapped by something else — leaving it alone"
                )

    for service in ("run_purge_now", "reload"):
        hass.services.async_remove(DOMAIN, service)

    return True


async def _query_row_counts(
    hass: HomeAssistant, entity_ids: list[str], cutoff_ts: float
) -> dict[str, tuple[int, float]]:
    """Return ``{entity_id: (row_count, oldest_ts)}`` for rows older than cutoff.

    Runs the grouped count query on the recorder's executor thread in batches
    of ``_PURGE_BATCH_SIZE`` to stay under SQLite's bind-var limit. Entities
    with zero matching rows are omitted from the result.
    """
    # Deferred: homeassistant.components.recorder is not available at module
    # load time — the recorder component must be fully initialised first.
    from homeassistant.components.recorder import get_instance  # noqa: PLC0415
    from homeassistant.components.recorder.db_schema import States, StatesMeta  # noqa: PLC0415
    from homeassistant.helpers.recorder import session_scope  # noqa: PLC0415
    from sqlalchemy import func, select  # noqa: PLC0415

    instance = get_instance(hass)

    def _query() -> dict[str, tuple[int, float]]:
        results: dict[str, tuple[int, float]] = {}
        with session_scope(session=instance.get_session()) as session:
            for i in range(0, len(entity_ids), _PURGE_BATCH_SIZE):
                batch = entity_ids[i : i + _PURGE_BATCH_SIZE]
                rows = session.execute(
                    select(
                        StatesMeta.entity_id,
                        func.count(States.state_id).label("cnt"),
                        func.min(States.last_updated_ts).label("oldest_ts"),
                    )
                    .join(StatesMeta, States.metadata_id == StatesMeta.metadata_id)
                    .where(StatesMeta.entity_id.in_(batch))
                    .where(States.last_updated_ts < cutoff_ts)
                    .group_by(StatesMeta.entity_id)
                ).fetchall()
                for row in rows:
                    if row.cnt > 0:
                        results[row.entity_id] = (row.cnt, row.oldest_ts)
        return results

    return await hass.async_add_executor_job(_query)


def _apply_stats_patch(hass: HomeAssistant, stats_keep_days: int) -> None:
    """Monkey-patch recorder purge to use a longer cutoff for short-term statistics.

    Must be called from the event loop (on setup and on config-entry reload).
    The replacement closure reads the current retention from
    ``hass.data[DOMAIN][_STATS_KEEP_DAYS_KEY]``, so config changes take effect
    on the next recorder purge without rewrapping.
    """
    domain_data = hass.data.setdefault(DOMAIN, {})
    # Update the cached retention unconditionally — covers both first-apply
    # and reload paths.
    domain_data[_STATS_KEEP_DAYS_KEY] = stats_keep_days

    if _ORIG_PURGE_FN_KEY in domain_data:
        _LOGGER.debug(
            "recorder_tuning: stats retention updated to %d days", stats_keep_days
        )
        return

    try:
        from homeassistant.components.recorder import purge as recorder_purge  # noqa: PLC0415
    except ImportError as err:
        _LOGGER.error(
            "recorder_tuning: recorder.purge unavailable, stats patch not applied: %s",
            err,
        )
        return

    current_fn = getattr(recorder_purge, "find_short_term_statistics_to_purge", None)
    if current_fn is None:
        _LOGGER.error(
            "recorder_tuning: HA has removed find_short_term_statistics_to_purge — "
            "stats patch not applied. Check tests/test_ha_signature_compat.py."
        )
        return

    if getattr(current_fn, _WRAPPER_TAG, False):
        # Our wrapper is already installed (stale hass.data or re-setup after
        # a partial teardown). Don't wrap it a second time.
        _LOGGER.debug(
            "recorder_tuning: wrapper already present, reusing it (retention %d days)",
            stats_keep_days,
        )
        return

    domain_data[_ORIG_PURGE_FN_KEY] = current_fn

    def patched_find_short_term_statistics_to_purge(
        purge_before: datetime, max_bind_vars: int
    ) -> Any:
        keep_days = domain_data.get(_STATS_KEEP_DAYS_KEY, DEFAULT_STATS_KEEP_DAYS)
        stats_purge_before = datetime.now(timezone.utc) - timedelta(days=keep_days)
        # Never purge more aggressively than the recorder wants
        effective_before = min(purge_before, stats_purge_before)
        _LOGGER.debug(
            "recorder_tuning: short-term stats cutoff %s → %s (%d days)",
            purge_before.isoformat(),
            effective_before.isoformat(),
            keep_days,
        )
        return current_fn(effective_before, max_bind_vars)

    patched_find_short_term_statistics_to_purge.__dict__[_WRAPPER_TAG] = True
    recorder_purge.find_short_term_statistics_to_purge = (
        patched_find_short_term_statistics_to_purge
    )
    _LOGGER.info(
        "recorder_tuning: short-term stats patch applied (%d days)", stats_keep_days
    )


class RecorderTuningManager:
    """Manages scheduled entity purge rules for HA recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        rules: list[dict],
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.rules: list[dict] = rules
        self._unsub_timer: Any = None

    async def async_setup(self) -> None:
        """Schedule the daily purge."""
        self._schedule_purge()
        _LOGGER.info(
            "recorder_tuning: loaded %d rule(s), scheduled at %s",
            len(self.rules),
            self.entry.data.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME),
        )

    def _schedule_purge(self) -> None:
        """Set up a daily time-based trigger."""
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None

        purge_time_str = self.entry.data.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME)
        try:
            purge_time = _parse_hhmm(purge_time_str)
        except (TypeError, ValueError):
            _LOGGER.warning(
                "recorder_tuning: invalid purge_time '%s', defaulting to %s",
                purge_time_str,
                DEFAULT_PURGE_TIME,
            )
            purge_time = _parse_hhmm(DEFAULT_PURGE_TIME)

        self._unsub_timer = async_track_time_change(
            self.hass,
            self._async_run_purge,
            hour=purge_time.hour,
            minute=purge_time.minute,
            second=0,
        )

    async def _async_run_purge(self, now: datetime) -> None:
        """Run all enabled purge rules."""
        dry_run = self.entry.data.get(CONF_DRY_RUN, DEFAULT_DRY_RUN)
        _LOGGER.info(
            "recorder_tuning: starting scheduled purge run%s",
            " [DRY RUN]" if dry_run else "",
        )
        await self._execute_all_rules(dry_run=dry_run)

    async def async_run_purge_now(self, call: ServiceCall) -> None:
        """Service handler: run purge immediately.

        If dry_run is explicitly provided in the service call it overrides the
        config entry setting.  If omitted, the config entry value is used so
        that the service call behaves the same as the scheduled nightly run.
        """
        if CONF_DRY_RUN in call.data:
            dry_run = call.data[CONF_DRY_RUN]
        else:
            dry_run = self.entry.data.get(CONF_DRY_RUN, DEFAULT_DRY_RUN)
        if dry_run:
            _LOGGER.info(
                "recorder_tuning: dry-run triggered via service — no data will be deleted"
            )
        else:
            _LOGGER.info("recorder_tuning: manual purge triggered via service")
        await self._execute_all_rules(dry_run=dry_run)

    async def _execute_all_rules(self, dry_run: bool = False) -> None:
        """Resolve entities for each rule and call recorder.purge_entities."""
        ent_reg = er.async_get(self.hass)

        if dry_run:
            _LOGGER.info(
                "recorder_tuning: [DRY RUN] starting — no data will be deleted"
            )
        else:
            _LOGGER.info("recorder_tuning: [PURGE] starting")

        for rule in self.rules:
            if not rule.get(CONF_ENABLED, True):
                _LOGGER.debug(
                    "recorder_tuning: skipping disabled rule '%s'", rule[CONF_RULE_NAME]
                )
                continue

            entity_ids = self._resolve_entities(rule, ent_reg)

            if not entity_ids:
                _LOGGER.warning(
                    "recorder_tuning: rule '%s' matched no entities, skipping",
                    rule[CONF_RULE_NAME],
                )
                continue

            keep_days = rule[CONF_KEEP_DAYS]  # required by _RULE_SCHEMA

            # Always log what will be (or would be) purged before acting
            await self._log_purge_plan(
                rule[CONF_RULE_NAME], entity_ids, keep_days, dry_run=dry_run
            )

            if not dry_run:
                for i in range(0, len(entity_ids), _PURGE_BATCH_SIZE):
                    batch = entity_ids[i : i + _PURGE_BATCH_SIZE]
                    await self.hass.services.async_call(
                        "recorder",
                        "purge_entities",
                        {"entity_id": batch, "keep_days": keep_days},
                        blocking=True,
                    )

        if dry_run:
            _LOGGER.info("recorder_tuning: [DRY RUN] complete")
        else:
            _LOGGER.info("recorder_tuning: purge run complete")

    async def _log_purge_plan(
        self,
        rule_name: str,
        entity_ids: list[str],
        keep_days: int,
        dry_run: bool = True,
    ) -> None:
        """Query and log which rows will be (or would be) removed for a rule.

        Called before every purge, regardless of dry-run mode.  The log prefix
        is ``[DRY RUN]`` or ``[PURGE]`` so lines are easy to grep.

        In dry-run mode the per-entity row details are logged at INFO so they
        are visible by default.  In live mode they are logged at DEBUG to avoid
        flooding the log on large instances — the INFO summary line (total rows
        across all matched entities) is always emitted in both modes.

        Note on accuracy: the query and the subsequent ``purge_entities`` call
        are not in a single transaction, so new rows can land between them.
        Counts below are a snapshot at query time; the actual delete count
        may differ slightly on a busy instance.
        """
        prefix = "[DRY RUN]" if dry_run else "[PURGE]"
        cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

        try:
            results = await _query_row_counts(self.hass, entity_ids, cutoff.timestamp())
        except Exception as err:  # noqa: BLE001
            # Broad catch is intentional: SQLAlchemy / recorder can raise a
            # wide range of errors (OperationalError, InterfaceError, driver
            # exceptions, schema drift). A failed pre-purge log must never
            # break the rest of the purge run — log and move on.
            _LOGGER.error(
                "recorder_tuning: %s rule '%s' — DB query failed: %s",
                prefix,
                rule_name,
                err,
            )
            return

        total_rows = sum(cnt for cnt, _ in results.values())
        _LOGGER.info(
            "recorder_tuning: %s rule '%s' — %d of %d matched entities have "
            "data older than %s (%d rows total)",
            prefix,
            rule_name,
            len(results),
            len(entity_ids),
            cutoff.strftime("%Y-%m-%d %H:%M UTC"),
            total_rows,
        )
        # In dry-run mode each per-entity line is emitted at INFO. Cap the
        # visible lines so the log stays readable on large installations;
        # the full list is still available at DEBUG.
        sorted_results = sorted(results.items())
        if dry_run and len(sorted_results) > _DRY_RUN_LOG_CAP:
            visible = sorted_results[:_DRY_RUN_LOG_CAP]
            hidden = sorted_results[_DRY_RUN_LOG_CAP:]
        else:
            visible = sorted_results
            hidden = []

        log_entity = _LOGGER.info if dry_run else _LOGGER.debug
        for entity_id, (cnt, oldest_ts) in visible:
            oldest = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
            log_entity(
                "recorder_tuning: %s   %-60s  %6d rows  %s → %s",
                prefix,
                entity_id,
                cnt,
                oldest.strftime("%Y-%m-%d %H:%M UTC"),
                cutoff.strftime("%Y-%m-%d %H:%M UTC"),
            )
        if hidden:
            hidden_rows = sum(cnt for _, (cnt, _) in hidden)
            _LOGGER.info(
                "recorder_tuning: %s   …and %d more entities (%d rows) — see DEBUG log",
                prefix,
                len(hidden),
                hidden_rows,
            )
            for entity_id, (cnt, oldest_ts) in hidden:
                oldest = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
                _LOGGER.debug(
                    "recorder_tuning: %s   %-60s  %6d rows  %s → %s",
                    prefix,
                    entity_id,
                    cnt,
                    oldest.strftime("%Y-%m-%d %H:%M UTC"),
                    cutoff.strftime("%Y-%m-%d %H:%M UTC"),
                )
        if not results:
            _LOGGER.info(
                "recorder_tuning: %s rule '%s' — nothing to purge", prefix, rule_name
            )

    def _resolve_entities(
        self,
        rule: dict,
        ent_reg: er.EntityRegistry,
    ) -> list[str]:
        """Build a deduplicated list of entity_ids matching the rule.

        Disabled entities are included in every selector path. A disabled
        entity does not record new states, but it may still have recorded
        history from before it was disabled — and that history is exactly
        what purge rules need to reach.
        """
        all_entries: list[er.RegistryEntry] = list(ent_reg.entities.values())

        # --- Positive selectors: build candidate set ---
        candidates: set[str] = set()

        # Explicit entity IDs
        for eid in rule.get(CONF_ENTITY_IDS, []):
            candidates.add(eid)

        # Integration/platform filter
        for integration in rule.get(CONF_INTEGRATION_FILTER, []):
            for entry in all_entries:
                if entry.platform == integration:
                    candidates.add(entry.entity_id)

        # Device IDs → all entities under that device, including disabled
        # ones (they may have pre-disable recorder history to purge).
        for device_id in rule.get(CONF_DEVICE_IDS, []):
            for entry in er.async_entries_for_device(
                ent_reg, device_id, include_disabled_entities=True
            ):
                candidates.add(entry.entity_id)

        # Glob patterns and regex selectors both need the full entity ID list;
        # build it lazily so rules using only entity_ids/integration/device skip it.
        _all_entity_ids: list[str] | None = None

        def all_entity_ids() -> list[str]:
            nonlocal _all_entity_ids
            if _all_entity_ids is None:
                _all_entity_ids = [e.entity_id for e in all_entries]
            return _all_entity_ids

        for pattern in rule.get(CONF_ENTITY_GLOBS, []):
            candidates.update(fnmatch.filter(all_entity_ids(), pattern))

        # Regex include — union with candidates
        for pattern in rule.get(CONF_ENTITY_REGEX_INCLUDE, []):
            try:
                compiled = re.compile(pattern)
                candidates.update(
                    eid for eid in all_entity_ids() if compiled.search(eid)
                )
            except re.error as err:
                _LOGGER.warning(
                    "recorder_tuning: rule '%s' invalid regex_include '%s': %s",
                    rule[CONF_RULE_NAME],
                    pattern,
                    err,
                )

        # --- Negative selector: regex exclude applied to candidate set ---
        excluded: set[str] = set()
        for pattern in rule.get(CONF_ENTITY_REGEX_EXCLUDE, []):
            try:
                compiled = re.compile(pattern)
                excluded.update(eid for eid in candidates if compiled.search(eid))
            except re.error as err:
                _LOGGER.warning(
                    "recorder_tuning: rule '%s' invalid regex_exclude '%s': %s",
                    rule[CONF_RULE_NAME],
                    pattern,
                    err,
                )

        resolved = candidates - excluded

        # Log all resolved entities that are absent from the state machine
        for eid in resolved:
            if not self.hass.states.get(eid):
                _LOGGER.debug(
                    "recorder_tuning: rule '%s': entity '%s' not in state machine",
                    rule[CONF_RULE_NAME],
                    eid,
                )

        # Sort for deterministic batch order, log order, and dry-run diffs.
        return sorted(resolved)

    async def async_service_reload(self, call: ServiceCall) -> None:
        """Service handler: reload rules from the YAML file.

        Raises ``HomeAssistantError`` if the file is present but unreadable,
        malformed, or has the wrong shape — so automations that call reload
        can detect the failure. On error the existing rule set is preserved
        (the reload is atomic: all-or-nothing).
        """
        yaml_rules = await self.hass.async_add_executor_job(_load_yaml_rules, self.hass)
        self.rules = yaml_rules
        _LOGGER.info(
            "recorder_tuning: reloaded %d rule(s) from %s",
            len(self.rules),
            YAML_CONFIG_FILE,
        )

    async def async_reload(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Handle config entry updates (schedule change, stats_keep_days change).

        ``hass`` and ``entry`` are required by HA's update-listener contract
        (``entry.add_update_listener``). HA mutates ``entry`` in place, so
        it *is* ``self.entry`` — reading either gives the same values — but
        we keep the args rather than referencing ``self`` to match the
        listener signature HA documents.
        """
        self._schedule_purge()
        _apply_stats_patch(
            hass, entry.data.get(CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS)
        )
        _LOGGER.info("recorder_tuning: reloaded with updated config")

    def async_unload(self) -> None:
        """Cancel the scheduled timer."""
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None
