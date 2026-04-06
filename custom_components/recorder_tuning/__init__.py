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
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import storage
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
    DEFAULT_KEEP_DAYS,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
    STORAGE_KEY,
    STORAGE_VERSION,
    YAML_CONFIG_FILE,
)

_LOGGER = logging.getLogger(__name__)

# Key used to stash the original purge function so we can restore it on unload
_ORIG_PURGE_FN_KEY = f"{DOMAIN}_original_purge_fn"

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


def _load_yaml_rules(hass: HomeAssistant) -> list[dict] | None:
    """Load and validate rules from recorder_tuning.yaml if it exists.

    Returns the validated rule list, or None if the file does not exist.
    On parse/validation errors, logs a warning and returns None so the
    caller can fall back to stored rules.
    """
    yaml_path = hass.config.path(YAML_CONFIG_FILE)
    if not os.path.isfile(yaml_path):
        return None

    try:
        with open(yaml_path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except OSError as err:
        _LOGGER.error("recorder_tuning: could not read %s: %s", yaml_path, err)
        return None
    except yaml.YAMLError as err:
        _LOGGER.error("recorder_tuning: YAML parse error in %s: %s", yaml_path, err)
        return None

    if not isinstance(raw, dict) or CONF_RULES not in raw:
        _LOGGER.error(
            "recorder_tuning: %s must contain a top-level 'rules:' list", yaml_path
        )
        return None

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

    _LOGGER.info("recorder_tuning: loaded %d rule(s) from %s", len(rules), yaml_path)
    return rules


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Handle migration of a config entry to a newer schema version.

    Version 1 is the only version so far.  Add migration logic here when
    ``RecorderTuningConfigFlow.VERSION`` is bumped, following the pattern:

        if entry.version < 2:
            new_data = {**entry.data, "new_field": default_value}
            hass.config_entries.async_update_entry(entry, data=new_data, version=2)
    """
    _LOGGER.debug(
        "recorder_tuning: config entry is at version %d, no migration needed",
        entry.version,
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Recorder Tuning from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    stats_keep_days = entry.data.get(CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS)
    _apply_stats_patch(hass, stats_keep_days)

    store = storage.Store(hass, STORAGE_VERSION, STORAGE_KEY)

    # YAML file takes precedence over stored rules when present
    yaml_rules = await hass.async_add_executor_job(_load_yaml_rules, hass)
    if yaml_rules is not None:
        rules_data = {CONF_RULES: yaml_rules}
    else:
        rules_data = await store.async_load() or {CONF_RULES: []}

    manager = RecorderTuningManager(
        hass, entry, store, rules_data, yaml_active=yaml_rules is not None
    )
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
        "add_rule",
        manager.async_service_add_rule,
        schema=_RULE_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        "remove_rule",
        manager.async_service_remove_rule,
        schema=vol.Schema({vol.Required(CONF_RULE_NAME): str}),
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
    manager: RecorderTuningManager | None = hass.data[DOMAIN].pop(entry.entry_id, None)
    if manager:
        manager.async_unload()

    # Restore the original purge function if we patched it
    original_fn = hass.data[DOMAIN].pop(_ORIG_PURGE_FN_KEY, None)
    if original_fn is not None:
        try:
            from homeassistant.components.recorder import purge as recorder_purge  # noqa: PLC0415

            recorder_purge.find_short_term_statistics_to_purge = original_fn
            _LOGGER.info("recorder_tuning: short-term stats patch removed")
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning(
                "recorder_tuning: could not restore purge function: %s", err
            )

    for service in ("run_purge_now", "add_rule", "remove_rule", "reload"):
        hass.services.async_remove(DOMAIN, service)

    return True


def _apply_stats_patch(hass: HomeAssistant, stats_keep_days: int) -> None:
    """Monkey-patch recorder purge to use a longer cutoff for short-term statistics."""
    try:
        from homeassistant.components.recorder import purge as recorder_purge  # noqa: PLC0415

        original_fn = recorder_purge.find_short_term_statistics_to_purge

        # Only patch once; if already patched, update the closure variable instead
        if _ORIG_PURGE_FN_KEY in hass.data.get(DOMAIN, {}):
            _LOGGER.debug(
                "recorder_tuning: stats patch already applied, updating to %d days",
                stats_keep_days,
            )
            # The closure reads stats_keep_days from the config entry at call time
            return

        hass.data.setdefault(DOMAIN, {})[_ORIG_PURGE_FN_KEY] = original_fn

        def patched_find_short_term_statistics_to_purge(
            purge_before: datetime, max_bind_vars: int
        ) -> Any:
            entries = hass.config_entries.async_entries(DOMAIN)
            if _ORIG_PURGE_FN_KEY in hass.data.get(DOMAIN, {}) and entries:
                keep_days = entries[0].data.get(
                    CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS
                )
            else:
                keep_days = DEFAULT_STATS_KEEP_DAYS
            stats_purge_before = datetime.now(timezone.utc) - timedelta(days=keep_days)
            # Never purge more aggressively than the recorder wants
            effective_before = min(purge_before, stats_purge_before)
            _LOGGER.debug(
                "recorder_tuning: short-term stats cutoff %s → %s (%d days)",
                purge_before.isoformat(),
                effective_before.isoformat(),
                keep_days,
            )
            return original_fn(effective_before, max_bind_vars)

        recorder_purge.find_short_term_statistics_to_purge = (
            patched_find_short_term_statistics_to_purge
        )
        _LOGGER.info(
            "recorder_tuning: short-term stats patch applied (%d days)", stats_keep_days
        )

    except Exception as err:  # noqa: BLE001
        _LOGGER.error("recorder_tuning: failed to patch recorder.purge: %s", err)


class RecorderTuningManager:
    """Manages scheduled entity purge rules for HA recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        store: storage.Store,
        rules_data: dict,
        yaml_active: bool = False,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.store = store
        self.rules: list[dict] = rules_data.get(CONF_RULES, [])
        self.yaml_active = yaml_active
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
            h, m = purge_time_str.split(":")
            purge_time = time(int(h), int(m), 0)
        except (ValueError, AttributeError):
            _LOGGER.warning(
                "recorder_tuning: invalid purge_time '%s', defaulting to 03:00",
                purge_time_str,
            )
            purge_time = time(3, 0, 0)

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

            keep_days = rule.get(CONF_KEEP_DAYS, DEFAULT_KEEP_DAYS)

            # Always log what will be (or would be) purged before acting
            await self._log_purge_plan(
                rule[CONF_RULE_NAME], entity_ids, keep_days, dry_run=dry_run
            )

            if not dry_run:
                batch_size = 100
                for i in range(0, len(entity_ids), batch_size):
                    batch = entity_ids[i : i + batch_size]
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
        """
        prefix = "[DRY RUN]" if dry_run else "[PURGE]"
        # Deferred: homeassistant.components.recorder is not available at module
        # load time — the recorder component must be fully initialised first.
        from homeassistant.components.recorder import get_instance  # noqa: PLC0415
        from homeassistant.components.recorder.db_schema import States, StatesMeta  # noqa: PLC0415
        from homeassistant.components.recorder.util import session_scope  # noqa: PLC0415
        from sqlalchemy import func, select  # noqa: PLC0415

        cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
        cutoff_ts = cutoff.timestamp()
        instance = get_instance(self.hass)

        def _query() -> dict[str, tuple[int, float]]:
            results: dict[str, tuple[int, float]] = {}
            with session_scope(session=instance.get_session()) as session:
                batch_size = 100
                for i in range(0, len(entity_ids), batch_size):
                    batch = entity_ids[i : i + batch_size]
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

        try:
            results = await self.hass.async_add_executor_job(_query)
        except Exception as err:  # noqa: BLE001
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
        log_entity = _LOGGER.info if dry_run else _LOGGER.debug
        for entity_id, (cnt, oldest_ts) in sorted(results.items()):
            oldest = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
            log_entity(
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
        """Build a deduplicated list of entity_ids matching the rule."""
        # Start with the universe of all registered entities
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

        # Device IDs → all non-disabled entities under that device
        for device_id in rule.get(CONF_DEVICE_IDS, []):
            for entry in er.async_entries_for_device(
                ent_reg, device_id, include_disabled_entities=False
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

        return list(resolved)

    async def async_service_reload(self, call: ServiceCall) -> None:
        """Service handler: reload rules from YAML file (or fall back to stored rules)."""
        yaml_rules = await self.hass.async_add_executor_job(_load_yaml_rules, self.hass)
        if yaml_rules is not None:
            self.rules = yaml_rules
            self.yaml_active = True
            _LOGGER.info(
                "recorder_tuning: reloaded %d rule(s) from %s",
                len(self.rules),
                YAML_CONFIG_FILE,
            )
        else:
            # No YAML file — fall back to stored rules
            self.yaml_active = False
            stored = await self.store.async_load() or {CONF_RULES: []}
            self.rules = stored.get(CONF_RULES, [])
            _LOGGER.info(
                "recorder_tuning: YAML file gone, reverted to %d stored rule(s)",
                len(self.rules),
            )

    async def async_service_add_rule(self, call: ServiceCall) -> None:
        """Service handler: add or update a rule."""
        if self.yaml_active:
            _LOGGER.warning(
                "recorder_tuning: add_rule ignored — rules are managed by %s; "
                "edit the file and call recorder_tuning.reload",
                YAML_CONFIG_FILE,
            )
            return
        # call.data is already validated and defaulted by _RULE_SCHEMA
        name = call.data[CONF_RULE_NAME]
        new_rule = dict(call.data)
        existing = next(
            (i for i, r in enumerate(self.rules) if r[CONF_RULE_NAME] == name), None
        )
        if existing is not None:
            self.rules[existing] = new_rule
            _LOGGER.info("recorder_tuning: updated rule '%s'", name)
        else:
            self.rules.append(new_rule)
            _LOGGER.info("recorder_tuning: added rule '%s'", name)
        await self._save_rules()

    async def async_service_remove_rule(self, call: ServiceCall) -> None:
        """Service handler: remove a rule by name."""
        if self.yaml_active:
            _LOGGER.warning(
                "recorder_tuning: remove_rule ignored — rules are managed by %s; "
                "edit the file and call recorder_tuning.reload",
                YAML_CONFIG_FILE,
            )
            return
        name = call.data[CONF_RULE_NAME]
        before = len(self.rules)
        self.rules = [r for r in self.rules if r[CONF_RULE_NAME] != name]
        if len(self.rules) < before:
            _LOGGER.info("recorder_tuning: removed rule '%s'", name)
        else:
            _LOGGER.warning(
                "recorder_tuning: rule '%s' not found, nothing removed", name
            )
        await self._save_rules()

    async def _save_rules(self) -> None:
        """Persist rules to storage."""
        await self.store.async_save({CONF_RULES: self.rules})

    async def async_reload(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Handle config entry updates (schedule change, stats_keep_days change).

        Only the purge schedule needs explicit rescheduling here.  The stats
        patch closure reads ``CONF_STATS_KEEP_DAYS`` directly from the config
        entry at every call, so stats retention changes take effect on the very
        next recorder purge without re-patching.
        """
        self._schedule_purge()
        _LOGGER.info("recorder_tuning: reloaded with updated config")

    def async_unload(self) -> None:
        """Cancel the scheduled timer."""
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None
