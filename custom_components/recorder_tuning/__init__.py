# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Recorder Tuning - per-entity purge rules and short-term statistics retention."""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import re
from datetime import datetime, time, timedelta, timezone
from typing import Any

import voluptuous as vol

from homeassistant.config import async_hass_config_yaml
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.typing import ConfigType

from .const import (
    CONF_DEVICE_IDS,
    CONF_DRY_RUN,
    DEFAULT_DRY_RUN,
    CONF_ENABLED,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_HA_RECORDER_PURGE,
    CONF_HA_RECORDER_PURGE_ENABLED,
    CONF_HA_RECORDER_PURGE_FORCE_REPACK,
    CONF_HA_RECORDER_PURGE_REPACK,
    CONF_INTEGRATION_FILTER,
    CONF_KEEP_DAYS,
    CONF_MATCH_MODE,
    CONF_PURGE_TIME,
    CONF_RULE_NAME,
    CONF_RULE_NAMES,
    CONF_RULES,
    CONF_STATS_KEEP_DAYS,
    DEFAULT_HA_RECORDER_PURGE_ENABLED,
    DEFAULT_HA_RECORDER_PURGE_FORCE_REPACK,
    DEFAULT_HA_RECORDER_PURGE_REPACK,
    DEFAULT_MATCH_MODE,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
    MATCH_MODE_ALL,
    MATCH_MODE_ANY,
    REPACK_MONTHLY,
    REPACK_NEVER,
    REPACK_WEEKLY,
)

_LOGGER = logging.getLogger(__name__)

# Key used to stash the original purge function (kept in hass.data for
# symmetry with prior versions; no unload hook consumes it today).
_ORIG_PURGE_FN_KEY = f"{DOMAIN}_original_purge_fn"
# Module-level cache of the current short-term stats retention. Written by
# _apply_stats_patch (event loop) and read by the patched closure on the
# recorder executor thread. A plain int read/assign is atomic under the
# CPython GIL, so no lock is required.  Module-level (rather than
# hass.data-keyed) so the wrapper closure survives test-instance changes:
# the monkey-patch on the recorder module persists across pytest tests, but
# each test creates a fresh hass — stashing the cache on hass.data would
# leave the wrapper reading stale data in the next test.
_STATS_KEEP_DAYS_CURRENT: int = DEFAULT_STATS_KEEP_DAYS
# Attribute tag on our wrapper so we can recognise it on unload/reload and
# avoid wrapping ourselves twice, or unwrapping someone else's patch.
_WRAPPER_TAG = f"__{DOMAIN}_wrapped__"
# Batch size for recorder.purge_entities service calls and the matching
# per-entity row-count query used in dry-run / pre-purge logging. Kept well
# under SQLite's default max-variables limit (999).
_PURGE_BATCH_SIZE = 100
# Ceiling on how long the per-rule recorder-queue drain can take. Generous
# because a busy DB can spend minutes on a large purge; we'd rather warn
# and move on than park the scheduled run forever on a wedged recorder.
_PURGE_DRAIN_TIMEOUT = 600
# Cap on the minority-list length in the dry-run summary logged on setup /
# reload (_log_dry_run_summary). Beyond this the list is truncated with
# "…and N more" so the startup log stays readable on large installs.
_DRY_RUN_LOG_CAP = 25


def _regex_pattern(value: str) -> str:
    """Voluptuous validator: raise ``vol.Invalid`` if ``value`` isn't a valid regex."""
    if not isinstance(value, str):
        raise vol.Invalid(f"regex must be a string, got {type(value).__name__}")
    try:
        re.compile(value)
    except re.error as err:
        raise vol.Invalid(f"invalid regex {value!r}: {err}") from err
    return value


def parse_hhmm(value: str) -> time:
    """Parse a ``HH:MM`` string into a ``time`` object."""
    return datetime.strptime(value, "%H:%M").time()


def _should_repack_today(
    now: datetime, repack_cadence: str, force_repack: bool
) -> bool:
    """Return True if this run should pass ``repack=True`` to ``recorder.purge``.

    ``force_repack`` (``ha_recorder_purge.force_repack: true``) is the explicit
    override and wins over the cadence. Otherwise the cadence is one of:

    - ``never``    → no scheduled repack
    - ``weekly``   → every Sunday
    - ``monthly``  → second Sunday of the month (matches HA's native
      ``auto_repack`` cadence)
    """
    if force_repack:
        return True
    if repack_cadence == REPACK_NEVER:
        return False
    if repack_cadence == REPACK_WEEKLY:
        # weekday(): Monday=0 .. Sunday=6
        return now.weekday() == 6
    if repack_cadence == REPACK_MONTHLY:
        # Reuse HA's own predicate so cadence changes upstream carry over.
        from homeassistant.components.recorder.util import is_second_sunday  # noqa: PLC0415

        return is_second_sunday(now)
    return False


def _effective_dry_run(
    top_level: bool, service: bool | None, rule: bool | None
) -> bool:
    """Resolve dry-run for one rule on one run per the documented precedence.

    1. Top-level ``dry_run: true`` is a safety lock that overrides everything
       — no service call or per-rule override can turn it off.
    2. When the top-level is false, an explicit service-call ``dry_run`` wins
       over the per-rule ``dry_run`` setting.
    3. When the service call doesn't specify, the per-rule setting applies.
    4. When neither is set, the top-level value (false at this point) applies.

    Separated out as a pure function for direct matrix tests; the live call
    sites in ``_execute_all_rules`` reach for this to stay consistent.
    """
    if top_level:
        return True
    if service is not None:
        return service
    if rule is not None:
        return rule
    return top_level  # False here


# `name` is intentionally absent: it already appears in the summary line
# that precedes these config lines. `enabled` is absent because disabled
# rules are filtered out before this log fires.
_RULE_CONFIG_LOG_KEYS = (
    CONF_INTEGRATION_FILTER,
    CONF_DEVICE_IDS,
    CONF_ENTITY_IDS,
    CONF_ENTITY_GLOBS,
    CONF_ENTITY_REGEX_INCLUDE,
    CONF_ENTITY_REGEX_EXCLUDE,
    CONF_KEEP_DAYS,
    CONF_MATCH_MODE,
    CONF_DRY_RUN,
)


def _rule_config_lines(rule: dict) -> list[str]:
    """Return one ``key: value`` line per non-empty rule field, in schema order.

    Drops empty selector lists and ``dry_run: None`` so the pre-purge log
    shows only the fields the user actually set (or the schema defaulted
    to something meaningful). Key order matches ``_RULE_SCHEMA`` so logs
    are diff-friendly run over run.
    """
    lines: list[str] = []
    for key in _RULE_CONFIG_LOG_KEYS:
        if key not in rule:
            continue
        value = rule[key]
        if value in (None, [], ""):
            continue
        lines.append(f"{key}: {value}")
    return lines


def _purge_time_validator(value: Any) -> str:
    """Voluptuous validator: accept ``HH:MM`` strings."""
    if not isinstance(value, str):
        raise vol.Invalid(f"purge_time must be a string, got {type(value).__name__}")
    try:
        parse_hhmm(value)
    except ValueError as err:
        raise vol.Invalid(f"purge_time must be HH:MM (got {value!r}): {err}") from err
    return value


# Voluptuous schema for a single rule
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
        # Per-rule dry-run override. Absent (None) means the rule inherits the
        # top-level dry_run setting.
        vol.Optional(CONF_DRY_RUN, default=None): vol.Any(None, bool),
    }
)


# Sub-schema for the ha_recorder_purge: block (see const.py for field docs).
_HA_RECORDER_PURGE_SCHEMA = vol.Schema(
    {
        vol.Optional(
            CONF_HA_RECORDER_PURGE_ENABLED,
            default=DEFAULT_HA_RECORDER_PURGE_ENABLED,
        ): bool,
        vol.Optional(
            CONF_HA_RECORDER_PURGE_REPACK,
            default=DEFAULT_HA_RECORDER_PURGE_REPACK,
        ): vol.In([REPACK_NEVER, REPACK_WEEKLY, REPACK_MONTHLY]),
        vol.Optional(
            CONF_HA_RECORDER_PURGE_FORCE_REPACK,
            default=DEFAULT_HA_RECORDER_PURGE_FORCE_REPACK,
        ): bool,
    }
)

# Default that gets filled when the user omits ``ha_recorder_purge:`` entirely.
_DEFAULT_HA_RECORDER_PURGE = _HA_RECORDER_PURGE_SCHEMA({})


# Top-level schema: recorder_tuning: block in configuration.yaml. Rules can
# be supplied inline or via !include — HA's YAML loader resolves !include
# before we ever see the dict.
CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(
                    CONF_PURGE_TIME, default=DEFAULT_PURGE_TIME
                ): _purge_time_validator,
                vol.Optional(
                    CONF_STATS_KEEP_DAYS, default=DEFAULT_STATS_KEEP_DAYS
                ): vol.All(int, vol.Range(min=1, max=365)),
                vol.Optional(CONF_DRY_RUN, default=DEFAULT_DRY_RUN): bool,
                vol.Optional(
                    CONF_HA_RECORDER_PURGE, default=_DEFAULT_HA_RECORDER_PURGE
                ): _HA_RECORDER_PURGE_SCHEMA,
                vol.Optional(CONF_RULES, default=[]): [_RULE_SCHEMA],
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Recorder Tuning from configuration.yaml.

    The integration is YAML-only — there is no UI config flow. Users declare
    all integration settings + purge rules under a top-level ``recorder_tuning:``
    key in ``configuration.yaml``; rules are typically pulled in via
    ``rules: !include recorder_tuning_rules.yaml``.

    Returns True even when the integration key is absent so HA does not treat
    the module as broken.
    """
    domain_config = config.get(DOMAIN)
    if domain_config is None:
        # Integration not configured — nothing to do.
        return True

    _apply_stats_patch(hass, domain_config[CONF_STATS_KEEP_DAYS])

    manager = RecorderTuningManager(hass, domain_config)
    hass.data.setdefault(DOMAIN, {})["manager"] = manager
    await manager.async_setup()

    hass.services.async_register(
        DOMAIN,
        "run_purge_now",
        manager.async_run_purge_now,
        schema=vol.Schema(
            {
                vol.Optional(CONF_DRY_RUN): bool,
                vol.Optional(CONF_RULE_NAMES): vol.All([str], vol.Length(min=1)),
                vol.Optional(CONF_HA_RECORDER_PURGE): bool,
                vol.Optional(CONF_KEEP_DAYS): vol.All(int, vol.Range(min=1, max=365)),
            }
        ),
    )
    hass.services.async_register(
        DOMAIN,
        "reload",
        _make_reload_handler(hass, manager),
        schema=vol.Schema({}),
    )

    return True


def _make_reload_handler(hass: HomeAssistant, manager: RecorderTuningManager):
    """Build a reload service handler closed over ``hass`` and ``manager``.

    Reload re-reads configuration.yaml (including any ``!include`` rules file),
    re-runs the integration's CONFIG_SCHEMA, and applies the validated config.
    Any parse/schema error propagates as a ``HomeAssistantError`` so automations
    calling reload can detect the failure. On error the previous rule set and
    settings survive unchanged (reload is atomic).
    """

    async def _reload(call: ServiceCall) -> None:
        try:
            raw = await async_hass_config_yaml(hass)
        except HomeAssistantError:
            raise
        except Exception as err:
            raise HomeAssistantError(
                f"recorder_tuning: failed to read configuration.yaml: {err}"
            ) from err

        if DOMAIN not in raw:
            raise HomeAssistantError(
                f"recorder_tuning: reload found no {DOMAIN}: block in configuration.yaml"
            )

        try:
            validated = CONFIG_SCHEMA(raw)
        except vol.Invalid as err:
            raise HomeAssistantError(
                f"recorder_tuning: invalid configuration: {err}"
            ) from err

        new_domain_config = validated[DOMAIN]

        # Reapply the short-term stats retention first — the cached value is
        # read by the recorder executor on the next purge.
        _apply_stats_patch(hass, new_domain_config[CONF_STATS_KEEP_DAYS])
        manager.update_config(new_domain_config)
        _LOGGER.info(
            "reloaded from configuration.yaml — %d rule(s)",
            len(manager.rules),
        )
        manager._log_dry_run_summary()

    return _reload


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

    Must be called from the event loop (on setup and on reload). The
    replacement closure reads the current retention from the module-level
    ``_STATS_KEEP_DAYS_CURRENT`` variable, so config changes take effect on
    the next recorder purge without rewrapping.
    """
    global _STATS_KEEP_DAYS_CURRENT  # noqa: PLW0603
    # Update the cached retention unconditionally — covers first-apply,
    # reload, and the "wrapper already installed from a previous run" path.
    _STATS_KEEP_DAYS_CURRENT = stats_keep_days

    domain_data = hass.data.setdefault(DOMAIN, {})

    try:
        from homeassistant.components.recorder import purge as recorder_purge  # noqa: PLC0415
    except ImportError as err:
        _LOGGER.error(
            "recorder.purge unavailable, stats patch not applied: %s",
            err,
        )
        return

    current_fn = getattr(recorder_purge, "find_short_term_statistics_to_purge", None)
    if current_fn is None:
        _LOGGER.error(
            "HA has removed find_short_term_statistics_to_purge — "
            "stats patch not applied. Check tests/test_ha_signature_compat.py."
        )
        return

    if getattr(current_fn, _WRAPPER_TAG, False):
        # Our wrapper is already installed (e.g., after a test suite reuses
        # the recorder module across tests, or after a reload). The module
        # variable updated above is enough — don't wrap it a second time.
        _LOGGER.debug(
            "wrapper already present, reusing it (retention %d days)",
            stats_keep_days,
        )
        return

    domain_data[_ORIG_PURGE_FN_KEY] = current_fn

    def patched_find_short_term_statistics_to_purge(
        purge_before: datetime, max_bind_vars: int
    ) -> Any:
        keep_days = _STATS_KEEP_DAYS_CURRENT
        stats_purge_before = datetime.now(timezone.utc) - timedelta(days=keep_days)
        # Never purge more aggressively than the recorder wants
        effective_before = min(purge_before, stats_purge_before)
        # WARNING level so it's visible in the default HA log without any
        # logger config — proves the monkey-patch is firing on each purge.
        _LOGGER.warning(
            "short-term stats cutoff %s → %s (%d days)",
            purge_before.isoformat(),
            effective_before.isoformat(),
            keep_days,
        )
        return current_fn(effective_before, max_bind_vars)

    patched_find_short_term_statistics_to_purge.__dict__[_WRAPPER_TAG] = True
    recorder_purge.find_short_term_statistics_to_purge = (
        patched_find_short_term_statistics_to_purge
    )
    _LOGGER.info("short-term stats patch applied (%d days)", stats_keep_days)


class RecorderTuningManager:
    """Manages scheduled entity purge rules for HA recorder."""

    def __init__(
        self,
        hass: HomeAssistant,
        config: dict,
    ) -> None:
        self.hass = hass
        self.config: dict = config
        self.rules: list[dict] = list(config.get(CONF_RULES, []))
        self._unsub_timer: Any = None
        # HH:MM string the timer is currently scheduled for. Lets us skip the
        # cancel/reinstall cycle when a reload doesn't touch the schedule —
        # important when the change lands right before a firing.
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
            "loaded %d rule(s), scheduled at %s",
            len(self.rules),
            self.config.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME),
        )
        self._log_dry_run_summary()

    def _log_dry_run_summary(self) -> None:
        """Log which enabled rules will run LIVE vs DRY RUN on the next firing.

        Called on setup and on reload so the user can verify rollout state at
        a glance. Applies the same ``_effective_dry_run`` precedence the
        scheduled run uses (service call = None), so the summary predicts what
        the next nightly firing will do.

        The minority set is listed by name (LIVE ties go to LIVE) so you can
        see exactly which rules are in the non-majority mode — most useful
        during rollout when only a few rules have been flipped to LIVE.
        Capped at ``_DRY_RUN_LOG_CAP`` to stay readable on large installs.
        """
        top_level = self.config.get(CONF_DRY_RUN, DEFAULT_DRY_RUN)
        enabled = [r for r in self.rules if r.get(CONF_ENABLED, True)]

        if top_level:
            _LOGGER.info(
                "top-level dry_run: true — all %d enabled rule(s) locked to DRY RUN",
                len(enabled),
            )
            return

        if not enabled:
            return

        live: list[str] = []
        dry: list[str] = []
        for rule in enabled:
            eff = _effective_dry_run(
                top_level=top_level, service=None, rule=rule.get(CONF_DRY_RUN)
            )
            (dry if eff else live).append(rule[CONF_RULE_NAME])

        _LOGGER.info(
            "dry-run summary — %d rule(s) LIVE, %d rule(s) DRY RUN",
            len(live),
            len(dry),
        )

        if not live or not dry:
            return

        minority, label = (live, "LIVE") if len(live) <= len(dry) else (dry, "DRY RUN")
        for name in sorted(minority)[:_DRY_RUN_LOG_CAP]:
            _LOGGER.info("  [%s] %s", label, name)
        if len(minority) > _DRY_RUN_LOG_CAP:
            _LOGGER.info(
                "  [%s] …and %d more",
                label,
                len(minority) - _DRY_RUN_LOG_CAP,
            )

    def update_config(self, new_config: dict) -> None:
        """Swap in a new validated config block (called by the reload handler).

        Replaces rules, updates schedule (no-op if HH:MM unchanged), and
        clears the zero-match warn-suppression set so each rule gets a
        fresh chance to warn once.
        """
        self.config = new_config
        self.rules = list(new_config.get(CONF_RULES, []))
        self._warned_empty_rules.clear()
        self._schedule_purge()

    def _schedule_purge(self) -> None:
        """Install (or update) the daily time-based trigger.

        If the scheduled HH:MM hasn't changed, do nothing — a reload that
        doesn't touch purge_time must not cancel a pending firing. That
        matters when a reload lands within a minute of the scheduled time:
        cancelling the timer would lose the day's purge.
        """
        purge_time_str = self.config.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME)

        if self._unsub_timer is not None and self._scheduled_at == purge_time_str:
            return

        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None

        try:
            purge_time = parse_hhmm(purge_time_str)
        except (TypeError, ValueError):
            _LOGGER.warning(
                "invalid purge_time '%s', defaulting to %s",
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
        """Run all enabled purge rules (scheduled nightly firing)."""
        _LOGGER.info("starting scheduled purge run")
        # Scheduled runs have no service-call context; fall through to the
        # per-rule + top-level precedence inside _execute_all_rules.
        await self._execute_all_rules(service_dry_run=None)

    async def async_run_purge_now(self, call: ServiceCall) -> None:
        """Service handler: run purge immediately.

        Precedence (see ``_effective_dry_run``):

        1. Top-level ``dry_run: true`` in configuration.yaml is a safety lock
           — nothing below can override it.
        2. When the top-level is false, an explicit ``dry_run`` in the service
           call wins over per-rule ``dry_run`` settings.
        3. Otherwise the per-rule ``dry_run`` (if set) applies.
        4. Otherwise the top-level value (false here) applies.

        ``rule_names`` (optional list of strings) restricts the run to rules
        with matching ``name``. Unknown names are logged as a warning but do
        not abort the call.

        ``keep_days`` (optional int 1-365) overrides the ``keep_days`` on
        every rule that runs in this call. Does not persist.

        Unlike the scheduled nightly run, the trailing global ``recorder.purge``
        call is **skipped by default** on a manual invocation. Pass
        ``ha_recorder_purge: true`` to opt into the full nightly flow.
        ``rule_names`` always implies-skip regardless.
        """
        service_dry_run: bool | None = call.data.get(CONF_DRY_RUN)
        keep_days_override = call.data.get(CONF_KEEP_DAYS)

        requested_names = call.data.get(CONF_RULE_NAMES)
        rules_arg: list[dict] | None = None
        # Manual-run default: skip the global recorder.purge. rule_names and
        # the explicit opt-in ``ha_recorder_purge`` both flow through this
        # single variable so _execute_all_rules sees exactly what to do.
        trailing_arg: bool = False
        override_suffix = (
            f" (keep_days override: {keep_days_override})"
            if keep_days_override is not None
            else ""
        )

        if requested_names:
            # Case-insensitive match: preserve the user's original casing for
            # the "unknown" warning but compare in lowercase against rule names.
            requested_lower_to_orig = {name.lower(): name for name in requested_names}
            requested_lower = set(requested_lower_to_orig.keys())
            known_lower = {r[CONF_RULE_NAME].lower() for r in self.rules}
            unknown = {
                requested_lower_to_orig[lo] for lo in requested_lower - known_lower
            }
            if unknown:
                _LOGGER.warning(
                    "run_purge_now: unknown rule name(s): %s",
                    sorted(unknown),
                )
            rules_arg = [
                r for r in self.rules if r[CONF_RULE_NAME].lower() in requested_lower
            ]
            if not rules_arg:
                _LOGGER.warning(
                    "run_purge_now: no rules matched %s — nothing to do",
                    sorted(requested_names),
                )
                return
            _LOGGER.info(
                "run_purge_now service triggered for rule(s): %s%s",
                sorted(r[CONF_RULE_NAME] for r in rules_arg),
                override_suffix,
            )
        else:
            # Explicit opt-in to the global sweep — only honoured when no
            # rule_names filter is present.
            trailing_arg = bool(call.data.get(CONF_HA_RECORDER_PURGE, False))
            _LOGGER.info(
                "run_purge_now service triggered%s%s",
                " (includes global recorder.purge)" if trailing_arg else "",
                override_suffix,
            )

        if keep_days_override is not None:
            # Shallow-copy each rule with the override applied. We use
            # self.rules when rules_arg is None so the override propagates to
            # whichever rules actually execute.
            source = rules_arg if rules_arg is not None else self.rules
            rules_arg = [
                {**rule, CONF_KEEP_DAYS: keep_days_override} for rule in source
            ]

        await self._execute_all_rules(
            service_dry_run=service_dry_run,
            rules=rules_arg,
            run_trailing_purge=trailing_arg,
        )

    async def _drain_recorder_queue(self, rule_name: str) -> None:
        """Wait for the recorder's task queue to drain, bounded by a timeout.

        recorder.purge_entities returns as soon as a PurgeEntitiesTask is
        queued on the recorder thread — not after the delete runs. Submitting
        a sentinel via async_block_till_done and awaiting it forces control
        to return only once the prior purges have actually completed (the
        recorder worker is FIFO and single-threaded). Bounded by
        _PURGE_DRAIN_TIMEOUT so a wedged recorder doesn't hang the run.
        """
        # Deferred: recorder may not be fully initialised at module load.
        from homeassistant.components.recorder import get_instance  # noqa: PLC0415

        try:
            await asyncio.wait_for(
                get_instance(self.hass).async_block_till_done(),
                timeout=_PURGE_DRAIN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "rule '%s': recorder queue did not drain within %ds — continuing",
                rule_name,
                _PURGE_DRAIN_TIMEOUT,
            )

    async def _execute_all_rules(
        self,
        service_dry_run: bool | None = None,
        rules: list[dict] | None = None,
        run_trailing_purge: bool | None = None,
    ) -> None:
        """Resolve entities for each rule and call recorder.purge_entities.

        ``service_dry_run`` — value from an explicit ``dry_run`` service
        parameter if the caller is a service handler. ``None`` for scheduled
        runs. Feeds ``_effective_dry_run`` along with the top-level config
        value and each rule's own ``dry_run`` setting.

        ``rules`` — if provided, iterate this list instead of ``self.rules``.
        The service handler uses this for ``rule_names``-filtered runs and
        ``keep_days`` overrides.

        ``run_trailing_purge`` — if None, defer to
        ``ha_recorder_purge.enabled`` in config. If explicitly False, skip the
        trailing global ``recorder.purge`` call (used by ``rule_names``-filtered
        runs so targeted debugging doesn't trigger the global sweep).
        """
        ent_reg = er.async_get(self.hass)
        active_rules = self.rules if rules is None else rules
        top_level_dry_run = self.config.get(CONF_DRY_RUN, DEFAULT_DRY_RUN)

        # Resolve the effective dry_run for every enabled rule up front so
        # the aggregate start/complete log can name the mode unambiguously.
        plan: list[tuple[dict, bool]] = []
        for rule in active_rules:
            if not rule.get(CONF_ENABLED, True):
                _LOGGER.debug("skipping disabled rule '%s'", rule[CONF_RULE_NAME])
                continue
            eff = _effective_dry_run(
                top_level=top_level_dry_run,
                service=service_dry_run,
                rule=rule.get(CONF_DRY_RUN),
            )
            plan.append((rule, eff))

        dry_count = sum(1 for _, eff in plan if eff)
        live_count = len(plan) - dry_count
        trailing_effective_dry = _effective_dry_run(
            top_level=top_level_dry_run, service=service_dry_run, rule=None
        )

        if not plan:
            _LOGGER.info("no enabled rules to run")
            mode_label = "[DRY RUN]" if trailing_effective_dry else "[PURGE]"
        elif live_count == 0:
            _LOGGER.info("[DRY RUN] starting — no data will be deleted")
            mode_label = "[DRY RUN]"
        elif dry_count == 0:
            _LOGGER.info("[PURGE] starting")
            mode_label = "[PURGE]"
        else:
            _LOGGER.info(
                "[MIXED] starting — %d rule(s) LIVE, %d rule(s) DRY RUN",
                live_count,
                dry_count,
            )
            mode_label = "[MIXED]"

        for rule, rule_dry_run in plan:
            rule_name = rule[CONF_RULE_NAME]
            entity_ids = self._resolve_entities(rule, ent_reg)

            if not entity_ids:
                # Warn the first time a rule matches nothing (e.g., the user's
                # integration hasn't loaded yet at HA startup, or a selector
                # is misconfigured). Suppress subsequent zero-match runs until
                # reload — or until the rule recovers — so we don't spam.
                if rule_name not in self._warned_empty_rules:
                    _LOGGER.warning(
                        "rule '%s' matched no entities, skipping "
                        "(further zero-match runs will log at DEBUG until reload)",
                        rule_name,
                    )
                    self._warned_empty_rules.add(rule_name)
                else:
                    _LOGGER.debug(
                        "rule '%s' still matches no entities",
                        rule_name,
                    )
                continue

            # Rule matched at least once — clear any prior suppression so a
            # future zero-match (e.g., integration unloaded) warns again.
            self._warned_empty_rules.discard(rule_name)

            keep_days = rule[CONF_KEEP_DAYS]  # required by _RULE_SCHEMA

            # Always log what will be (or would be) purged before acting.
            # _log_purge_plan emits the summary, per-rule config dump, and
            # per-entity lines together so the shape stays consistent.
            await self._log_purge_plan(rule, entity_ids, dry_run=rule_dry_run)

            if not rule_dry_run:
                for i in range(0, len(entity_ids), _PURGE_BATCH_SIZE):
                    batch = entity_ids[i : i + _PURGE_BATCH_SIZE]
                    await self.hass.services.async_call(
                        "recorder",
                        "purge_entities",
                        {"entity_id": batch, "keep_days": keep_days},
                        blocking=True,
                    )
                # Drain the recorder queue once per rule so the "rule
                # complete" log below fires only after the PurgeEntitiesTasks
                # have actually run (recorder.purge_entities returns as soon
                # as the task is queued, not after the delete).
                await self._drain_recorder_queue(rule_name)
                _LOGGER.info(
                    "rule '%s' complete — %d entities",
                    rule_name,
                    len(entity_ids),
                )

        # After per-entity rules, optionally call HA's own recorder.purge so
        # the global purge_keep_days sweeps everything rules don't cover AND
        # the short-term stats monkey-patch fires. Intended to replace HA's
        # auto_purge (set auto_purge: false on the recorder).  Caller can
        # force-skip via run_trailing_purge=False (used by rule_names runs).
        ha_purge_cfg = self.config.get(
            CONF_HA_RECORDER_PURGE, _DEFAULT_HA_RECORDER_PURGE
        )
        do_trailing = (
            ha_purge_cfg.get(
                CONF_HA_RECORDER_PURGE_ENABLED, DEFAULT_HA_RECORDER_PURGE_ENABLED
            )
            if run_trailing_purge is None
            else run_trailing_purge
        )
        if do_trailing:
            repack = _should_repack_today(
                datetime.now(),
                ha_purge_cfg.get(
                    CONF_HA_RECORDER_PURGE_REPACK, DEFAULT_HA_RECORDER_PURGE_REPACK
                ),
                ha_purge_cfg.get(
                    CONF_HA_RECORDER_PURGE_FORCE_REPACK,
                    DEFAULT_HA_RECORDER_PURGE_FORCE_REPACK,
                ),
            )
            if trailing_effective_dry:
                _LOGGER.info(
                    "[DRY RUN] would call recorder.purge (repack=%s)",
                    repack,
                )
            else:
                _LOGGER.info(
                    "calling recorder.purge (repack=%s) — "
                    "this can take minutes on a large DB",
                    repack,
                )
                try:
                    await self.hass.services.async_call(
                        "recorder",
                        "purge",
                        {"repack": repack},
                        blocking=True,
                    )
                except Exception as err:  # noqa: BLE001
                    # Broad catch: recorder may be unavailable, mid-restart,
                    # or the service call may time out. We've already done
                    # the per-entity work, so log and move on rather than
                    # raising to the scheduler.
                    _LOGGER.error("recorder.purge failed: %s", err)

        _LOGGER.info("%s complete", mode_label)

    async def _log_purge_plan(
        self,
        rule: dict,
        entity_ids: list[str],
        dry_run: bool = True,
    ) -> None:
        """Query and log which rows will be (or would be) removed for a rule.

        Emits in this order:

        1. Summary line — ``rule 'X' (keep 7d) — M of K matched entities ...``
           (or ``nothing to purge`` when no rows are old enough)
        2. Per-rule config dump — one indented ``key: value`` line per
           non-empty field (see ``_rule_config_lines``)
        3. Per-entity rows — one indented line per entity with a row count
           and the oldest timestamp being trimmed

        Called before every purge, regardless of dry-run mode. The log prefix
        is ``[DRY RUN]`` or ``[PURGE]`` so lines are easy to grep.

        Every matched entity is emitted at INFO — no cap, no DEBUG fallback.
        The user wants full visibility of what the purge touches in the
        default HA log. Operators on very large installs can raise the
        ``recorder_tuning`` logger to WARNING if the noise is a problem.

        Note on accuracy: the query and the subsequent ``purge_entities`` call
        are not in a single transaction, so new rows can land between them.
        Counts below are a snapshot at query time; the actual delete count
        may differ slightly on a busy instance.
        """
        rule_name = rule[CONF_RULE_NAME]
        keep_days = rule[CONF_KEEP_DAYS]
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
                "%s rule '%s' — DB query failed: %s",
                prefix,
                rule_name,
                err,
            )
            return

        if not results:
            _LOGGER.info(
                "%s rule '%s' (keep %dd) — nothing to purge (checked %d entities, cutoff %s)",
                prefix,
                rule_name,
                keep_days,
                len(entity_ids),
                cutoff.strftime("%Y-%m-%d %H:%M UTC"),
            )
            for line in _rule_config_lines(rule):
                _LOGGER.info("%s   %s", prefix, line)
            return

        total_rows = sum(cnt for cnt, _ in results.values())
        _LOGGER.info(
            "%s rule '%s' (keep %dd) — %d of %d matched entities have "
            "data older than %s (%d rows total)",
            prefix,
            rule_name,
            keep_days,
            len(results),
            len(entity_ids),
            cutoff.strftime("%Y-%m-%d %H:%M UTC"),
            total_rows,
        )
        for line in _rule_config_lines(rule):
            _LOGGER.info("%s   %s", prefix, line)
        for entity_id, (cnt, oldest_ts) in sorted(results.items()):
            oldest = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
            _LOGGER.info(
                "%s   %-60s  %6d rows  %s → %s",
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

        Glob and regex selectors see both registered entities AND entities
        that exist only in the state machine (e.g., old-style yaml-defined
        MQTT sensors that bypass the entity registry). ``integration_filter``
        and ``device_ids`` remain registry-only because the state machine
        carries no integration/device metadata to match against.
        """
        match_mode = rule.get(CONF_MATCH_MODE, DEFAULT_MATCH_MODE)
        all_entries: list[er.RegistryEntry] = list(ent_reg.entities.values())

        # Glob and regex selectors both walk the full entity-id list. Build it
        # lazily so rules that don't use those selectors skip the allocation.
        # The list is the union of registry-known entities and whatever is
        # currently in the state machine; the latter captures yaml-defined
        # entities that never made it into the registry.
        _all_entity_ids_cache: list[str] | None = None

        def all_entity_ids() -> list[str]:
            nonlocal _all_entity_ids_cache
            if _all_entity_ids_cache is None:
                ids: set[str] = {e.entity_id for e in all_entries}
                ids.update(s.entity_id for s in self.hass.states.async_all())
                _all_entity_ids_cache = sorted(ids)
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
                    "rule '%s': entity '%s' not in state machine",
                    rule[CONF_RULE_NAME],
                    eid,
                )

        # Sort for deterministic batch order, log order, and dry-run diffs.
        return sorted(resolved)

    def async_unload(self) -> None:
        """Cancel the scheduled timer."""
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None
        self._scheduled_at = None
