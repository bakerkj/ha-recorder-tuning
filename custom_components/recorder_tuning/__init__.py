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
    CONF_MATCH_MODE,
    CONF_PURGE_TIME,
    CONF_RULE_NAME,
    CONF_RULES,
    CONF_STATS_KEEP_DAYS,
    DEFAULT_MATCH_MODE,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
    MATCH_MODE_ALL,
    MATCH_MODE_ANY,
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


def _regex_pattern(value: str) -> str:
    """Voluptuous validator: raise ``vol.Invalid`` if ``value`` isn't a valid regex.

    Running this at YAML load time means a bad pattern surfaces as a rule-level
    validation error (via ``_load_yaml_rules``'s "skipping rule[i]" path) and
    is visible to the user when they call ``recorder_tuning.reload``, not
    hours later when a scheduled purge runs.
    """
    if not isinstance(value, str):
        raise vol.Invalid(f"regex must be a string, got {type(value).__name__}")
    try:
        re.compile(value)
    except re.error as err:
        raise vol.Invalid(f"invalid regex {value!r}: {err}") from err
    return value


# Voluptuous schema for a single rule loaded from YAML
_RULE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_RULE_NAME): str,
        vol.Optional(CONF_INTEGRATION_FILTER, default=[]): [str],
        vol.Optional(CONF_DEVICE_IDS, default=[]): [str],
        vol.Optional(CONF_ENTITY_IDS, default=[]): [str],
        vol.Optional(CONF_ENTITY_GLOBS, default=[]): [str],
        vol.Optional(CONF_ENTITY_REGEX_INCLUDE, default=[]): [_regex_pattern],
        vol.Optional(CONF_ENTITY_REGEX_EXCLUDE, default=[]): [_regex_pattern],
        vol.Required(CONF_KEEP_DAYS): vol.All(int, vol.Range(min=1, max=365)),
        vol.Optional(CONF_ENABLED, default=True): bool,
        vol.Optional(CONF_MATCH_MODE, default=DEFAULT_MATCH_MODE): vol.In(
            [MATCH_MODE_ALL, MATCH_MODE_ANY]
        ),
    }
)


def parse_hhmm(value: str) -> time:
    """Parse a ``HH:MM`` string into a ``time`` object.

    Shared between the config flow (wraps ``ValueError`` into ``vol.Invalid``
    for form validation) and the scheduler (logs a warning and falls back to
    the default). No leading underscore because this is consumed across
    module boundaries within the package.
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

    # Warn on duplicate rule names. Duplicates still run — the rule engine
    # supports multiple rules matching the same entity — but identical names
    # make log output ambiguous (which rule ran first? which failed?).
    seen: set[str] = set()
    duplicates: set[str] = set()
    for rule in rules:
        name = rule[CONF_RULE_NAME]
        if name in seen:
            duplicates.add(name)
        else:
            seen.add(name)
    for name in sorted(duplicates):
        _LOGGER.warning(
            "recorder_tuning: rule name '%s' appears more than once in %s — "
            "each instance runs, but logs will be ambiguous",
            name,
            yaml_path,
        )

    # Callers (async_setup_entry, async_service_reload) emit their own summary
    # log with schedule/context — don't double-log the rule count here.
    return rules


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Recorder Tuning from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    stats_keep_days = entry.data.get(CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS)
    _apply_stats_patch(hass, stats_keep_days)

    yaml_path = hass.config.path(YAML_CONFIG_FILE)
    file_exists = await hass.async_add_executor_job(os.path.isfile, yaml_path)
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
    # Only nudge the user about the file when it actually doesn't exist. An
    # explicit ``rules: []`` is a legitimate "stats retention only" setup and
    # shouldn't get the "create the file" hint.
    if not file_exists:
        _LOGGER.info(
            "recorder_tuning: no %s in config dir — no purge rules active. "
            "Create the file and call recorder_tuning.reload to enable.",
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
        # HH:MM string the timer is currently scheduled for. Lets us skip the
        # cancel/reinstall cycle when an options change doesn't touch the
        # schedule — important when the change lands right before a firing.
        self._scheduled_at: str | None = None
        # Names of rules that warned about matching zero entities since the
        # last reload. Further zero-match runs for the same rule log at DEBUG
        # to avoid spamming the log every purge; the set is cleared on reload
        # and also when a rule recovers (matches ≥1 entity on a later run).
        self._warned_empty_rules: set[str] = set()

    async def async_setup(self) -> None:
        """Schedule the daily purge."""
        self._schedule_purge()
        _LOGGER.info(
            "recorder_tuning: loaded %d rule(s), scheduled at %s",
            len(self.rules),
            self.entry.data.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME),
        )

    def _schedule_purge(self) -> None:
        """Install (or update) the daily time-based trigger.

        If the scheduled HH:MM hasn't changed, do nothing — an options change
        that touches only dry_run or stats_keep_days must not cancel a pending
        firing. That matters when an update lands within a minute of the
        scheduled time: cancelling the timer would lose the day's purge.
        """
        purge_time_str = self.entry.data.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME)

        if self._unsub_timer is not None and self._scheduled_at == purge_time_str:
            return

        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None

        try:
            purge_time = parse_hhmm(purge_time_str)
        except (TypeError, ValueError):
            _LOGGER.warning(
                "recorder_tuning: invalid purge_time '%s', defaulting to %s",
                purge_time_str,
                DEFAULT_PURGE_TIME,
            )
            purge_time = parse_hhmm(DEFAULT_PURGE_TIME)

        self._unsub_timer = async_track_time_change(
            self.hass,
            self._async_run_purge,
            hour=purge_time.hour,
            minute=purge_time.minute,
            second=0,
        )
        self._scheduled_at = purge_time_str

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

            rule_name = rule[CONF_RULE_NAME]
            entity_ids = self._resolve_entities(rule, ent_reg)

            if not entity_ids:
                # Warn the first time a rule matches nothing (e.g., the user's
                # integration hasn't loaded yet at HA startup, or a selector
                # is misconfigured). Suppress subsequent zero-match runs until
                # reload — or until the rule recovers — so we don't spam.
                if rule_name not in self._warned_empty_rules:
                    _LOGGER.warning(
                        "recorder_tuning: rule '%s' matched no entities, skipping "
                        "(further zero-match runs will log at DEBUG until reload)",
                        rule_name,
                    )
                    self._warned_empty_rules.add(rule_name)
                else:
                    _LOGGER.debug(
                        "recorder_tuning: rule '%s' still matches no entities",
                        rule_name,
                    )
                continue

            # Rule matched at least once — clear any prior suppression so a
            # future zero-match (e.g., integration unloaded) warns again.
            self._warned_empty_rules.discard(rule_name)

            keep_days = rule[CONF_KEEP_DAYS]  # required by _RULE_SCHEMA

            # Always log what will be (or would be) purged before acting
            await self._log_purge_plan(
                rule_name, entity_ids, keep_days, dry_run=dry_run
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

        if not results:
            _LOGGER.info(
                "recorder_tuning: %s rule '%s' (keep %dd) — nothing to purge (checked %d entities, cutoff %s)",
                prefix,
                rule_name,
                keep_days,
                len(entity_ids),
                cutoff.strftime("%Y-%m-%d %H:%M UTC"),
            )
            return

        total_rows = sum(cnt for cnt, _ in results.values())
        _LOGGER.info(
            "recorder_tuning: %s rule '%s' (keep %dd) — %d of %d matched entities have "
            "data older than %s (%d rows total)",
            prefix,
            rule_name,
            keep_days,
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

    def _resolve_entities(
        self,
        rule: dict,
        ent_reg: er.EntityRegistry,
    ) -> list[str]:
        """Build a deduplicated list of entity_ids matching the rule.

        Each *present* positive selector produces an entity-id set. Sets are
        combined by ``match_mode``:

        - ``"all"`` (default): intersection — the entity must satisfy every
          present selector. Adding a selector narrows the rule.
        - ``"any"``: union — legacy behaviour; the entity matches if any
          selector matches.

        Within a single selector, list items still OR together
        (e.g. ``integration_filter: [a, b]`` means platform is ``a`` or ``b``;
        ``entity_regex_include: [p1, p2]`` means either pattern matches).

        ``entity_regex_exclude`` is always subtracted from the final set,
        independent of mode.

        Disabled entities are included in every selector path. A disabled
        entity does not record new states, but it may still have recorded
        history from before it was disabled — and that history is exactly
        what purge rules need to reach.
        """
        match_mode = rule.get(CONF_MATCH_MODE, DEFAULT_MATCH_MODE)
        all_entries: list[er.RegistryEntry] = list(ent_reg.entities.values())

        # Glob and regex selectors both walk the full entity-id list. Build it
        # lazily so rules that don't use those selectors skip the allocation.
        _all_entity_ids_cache: list[str] | None = None

        def all_entity_ids() -> list[str]:
            nonlocal _all_entity_ids_cache
            if _all_entity_ids_cache is None:
                _all_entity_ids_cache = [e.entity_id for e in all_entries]
            return _all_entity_ids_cache

        # --- Positive selectors: one set per *present* selector ---
        selector_sets: list[set[str]] = []

        # Explicit entity IDs
        entity_ids = rule.get(CONF_ENTITY_IDS) or []
        if entity_ids:
            selector_sets.append(set(entity_ids))

        # Integration/platform filter
        integrations = rule.get(CONF_INTEGRATION_FILTER) or []
        if integrations:
            wanted = set(integrations)
            selector_sets.append(
                {e.entity_id for e in all_entries if e.platform in wanted}
            )

        # Device IDs → all entities under that device, including disabled
        # ones (they may have pre-disable recorder history to purge).
        device_ids = rule.get(CONF_DEVICE_IDS) or []
        if device_ids:
            device_set: set[str] = set()
            for device_id in device_ids:
                for entry in er.async_entries_for_device(
                    ent_reg, device_id, include_disabled_entities=True
                ):
                    device_set.add(entry.entity_id)
            selector_sets.append(device_set)

        # Glob patterns
        globs = rule.get(CONF_ENTITY_GLOBS) or []
        if globs:
            glob_set: set[str] = set()
            for pattern in globs:
                glob_set.update(fnmatch.filter(all_entity_ids(), pattern))
            selector_sets.append(glob_set)

        # Regex include — patterns validated by _RULE_SCHEMA → _regex_pattern at
        # load time; re.compile here cannot raise. re's internal cache makes
        # repeated compilation essentially free.
        regex_includes = rule.get(CONF_ENTITY_REGEX_INCLUDE) or []
        if regex_includes:
            regex_set: set[str] = set()
            for pattern in regex_includes:
                compiled = re.compile(pattern)
                regex_set.update(
                    eid for eid in all_entity_ids() if compiled.search(eid)
                )
            selector_sets.append(regex_set)

        if not selector_sets:
            return []

        if match_mode == MATCH_MODE_ALL:
            candidates: set[str] = set.intersection(*selector_sets)
        else:
            candidates = set.union(*selector_sets)

        # --- Negative selector: regex exclude applied to candidate set ---
        excluded: set[str] = set()
        for pattern in rule.get(CONF_ENTITY_REGEX_EXCLUDE, []):
            compiled = re.compile(pattern)
            excluded.update(eid for eid in candidates if compiled.search(eid))

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
        # Rule set replaced → reset zero-match warning suppression so each
        # rule gets a fresh chance to warn once.
        self._warned_empty_rules.clear()
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
        self._scheduled_at = None
