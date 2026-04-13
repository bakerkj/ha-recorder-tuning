# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Config flow for Recorder Tuning."""

from __future__ import annotations

import re

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import storage

from .const import (
    CONF_DEVICE_IDS,
    CONF_DRY_RUN,
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
    DEFAULT_DRY_RUN,
    DEFAULT_KEEP_DAYS,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
    STORAGE_KEY,
    STORAGE_VERSION,
)


def _valid_time(value: str) -> str:
    """Validate HH:MM time string and return it zero-padded."""
    if not re.match(r"^\d{1,2}:\d{2}$", value):
        raise vol.Invalid("Time must be in HH:MM format, e.g. 03:00")
    h, m = value.split(":")
    if not (0 <= int(h) <= 23 and 0 <= int(m) <= 59):
        raise vol.Invalid("Invalid time value")
    return f"{int(h):02d}:{m}"


def _split_csv(val: str) -> list[str]:
    """Split a comma-separated string into a stripped list, skipping blanks."""
    return [x.strip() for x in val.split(",") if x.strip()] if val else []


class RecorderTuningConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):  # type: ignore[call-arg]
    """Handle the initial setup config flow."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Initial setup: ask for daily purge time and stats retention."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                _valid_time(user_input[CONF_PURGE_TIME])
            except vol.Invalid:
                errors[CONF_PURGE_TIME] = "invalid_time"

            if not errors:
                await self.async_set_unique_id(DOMAIN)
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title="Recorder Tuning",
                    data={
                        CONF_PURGE_TIME: user_input[CONF_PURGE_TIME],
                        CONF_STATS_KEEP_DAYS: user_input[CONF_STATS_KEEP_DAYS],
                        CONF_DRY_RUN: user_input.get(CONF_DRY_RUN, DEFAULT_DRY_RUN),
                    },
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_PURGE_TIME, default=DEFAULT_PURGE_TIME): str,
                    # min=1 is safe: the patch uses min(recorder_cutoff, stats_cutoff)
                    # so it can never purge more aggressively than the recorder would.
                    vol.Required(
                        CONF_STATS_KEEP_DAYS, default=DEFAULT_STATS_KEEP_DAYS
                    ): vol.All(int, vol.Range(min=1, max=365)),
                    vol.Optional(CONF_DRY_RUN, default=DEFAULT_DRY_RUN): bool,
                }
            ),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> RecorderTuningOptionsFlow:
        """Return the options flow handler."""
        return RecorderTuningOptionsFlow(config_entry)


class RecorderTuningOptionsFlow(config_entries.OptionsFlow):
    """Options flow for managing purge rules and schedule."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self.config_entry = config_entry
        self._rules: list[dict] = []
        self._editing_index: int | None = None
        self._yaml_active: bool = False

    async def async_step_init(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Load rules from the live manager (or storage if manager isn't available)."""
        from .const import DOMAIN as _DOMAIN  # noqa: PLC0415

        manager = self.hass.data.get(_DOMAIN, {}).get(self.config_entry.entry_id)
        if manager is not None:
            # Use the manager's in-memory state so the UI always reflects what is
            # actually active (YAML rules when yaml_active=True, stored rules otherwise).
            self._rules = list(manager.rules)
            self._yaml_active = manager.yaml_active
        else:
            store = storage.Store(self.hass, STORAGE_VERSION, STORAGE_KEY)
            data = await store.async_load() or {CONF_RULES: []}
            self._rules = data.get(CONF_RULES, [])
            self._yaml_active = False
        return await self.async_step_menu()

    async def async_step_menu(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Show menu of current rules and actions."""
        rule_lines = []
        for i, rule in enumerate(self._rules):
            status = "on" if rule.get(CONF_ENABLED, True) else "off"
            rule_lines.append(
                f"[{status}] {i + 1}. {rule[CONF_RULE_NAME]} — keep {rule[CONF_KEEP_DAYS]}d"
            )
        rules_text = "\n".join(rule_lines) if rule_lines else "(no rules defined)"

        dry_run_status = (
            "ON" if self.config_entry.data.get(CONF_DRY_RUN, DEFAULT_DRY_RUN) else "OFF"
        )
        yaml_status = (
            "ON (edit recorder_tuning.yaml and call reload)"
            if self._yaml_active
            else "OFF"
        )
        return self.async_show_menu(
            step_id="menu",
            menu_options=[
                "set_schedule",
                "set_stats_retention",
                "set_dry_run",
                "add_rule",
                "edit_rule",
                "remove_rule",
                "done",
            ],
            description_placeholders={
                "rules": rules_text,
                "dry_run": dry_run_status,
                "yaml_active": yaml_status,
            },
        )

    # ------------------------------------------------------------------ #
    # Schedule and stats retention                                         #
    # ------------------------------------------------------------------ #

    async def async_step_set_schedule(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Change the daily purge time."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                _valid_time(user_input[CONF_PURGE_TIME])
            except vol.Invalid:
                errors[CONF_PURGE_TIME] = "invalid_time"
            else:
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    data={
                        **self.config_entry.data,
                        CONF_PURGE_TIME: user_input[CONF_PURGE_TIME],
                    },
                )
                return await self.async_step_menu()

        current = self.config_entry.data.get(CONF_PURGE_TIME, DEFAULT_PURGE_TIME)
        return self.async_show_form(
            step_id="set_schedule",
            data_schema=vol.Schema(
                {vol.Required(CONF_PURGE_TIME, default=current): str}
            ),
            errors=errors,
        )

    async def async_step_set_dry_run(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Toggle dry-run mode on or off."""
        if user_input is not None:
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data={
                    **self.config_entry.data,
                    CONF_DRY_RUN: user_input[CONF_DRY_RUN],
                },
            )
            return await self.async_step_menu()

        current = self.config_entry.data.get(CONF_DRY_RUN, DEFAULT_DRY_RUN)
        return self.async_show_form(
            step_id="set_dry_run",
            data_schema=vol.Schema({vol.Required(CONF_DRY_RUN, default=current): bool}),
        )

    async def async_step_set_stats_retention(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Change the short-term statistics retention period."""
        if user_input is not None:
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data={
                    **self.config_entry.data,
                    CONF_STATS_KEEP_DAYS: user_input[CONF_STATS_KEEP_DAYS],
                },
            )
            return await self.async_step_menu()

        current = self.config_entry.data.get(
            CONF_STATS_KEEP_DAYS, DEFAULT_STATS_KEEP_DAYS
        )
        return self.async_show_form(
            step_id="set_stats_retention",
            data_schema=vol.Schema(
                {
                    # min=1 is safe: the patch uses min(recorder_cutoff, stats_cutoff)
                    # so it can never purge more aggressively than the recorder would.
                    vol.Required(CONF_STATS_KEEP_DAYS, default=current): vol.All(
                        int, vol.Range(min=1, max=365)
                    )
                }
            ),
        )

    # ------------------------------------------------------------------ #
    # Rule management                                                      #
    # ------------------------------------------------------------------ #

    async def async_step_add_rule(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Add a new purge rule."""
        if self._yaml_active:
            return await self.async_step_menu()

        errors: dict[str, str] = {}

        if user_input is not None:
            errors = _validate_rule_input(user_input)
            if not errors:
                self._rules.append(_build_rule(user_input))
                await self._save_rules()
                return await self.async_step_menu()

        return self.async_show_form(
            step_id="add_rule",
            data_schema=_rule_schema(),
            errors=errors,
        )

    async def async_step_edit_rule(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Select a rule to edit."""
        if self._yaml_active or not self._rules:
            return await self.async_step_menu()

        if user_input is not None:
            self._editing_index = int(user_input["rule_index"])
            return await self.async_step_edit_rule_detail()

        options = {
            str(i): f"{i + 1}. {r[CONF_RULE_NAME]}" for i, r in enumerate(self._rules)
        }
        return self.async_show_form(
            step_id="edit_rule",
            data_schema=vol.Schema({vol.Required("rule_index"): vol.In(options)}),
        )

    async def async_step_edit_rule_detail(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Edit the selected rule."""
        errors: dict[str, str] = {}
        idx = self._editing_index
        rule = self._rules[idx]  # type: ignore[index]

        if user_input is not None:
            errors = _validate_rule_input(user_input)
            if not errors:
                self._rules[idx] = _build_rule(user_input)  # type: ignore[index]
                await self._save_rules()
                self._editing_index = None
                return await self.async_step_menu()

        defaults = {
            CONF_RULE_NAME: rule[CONF_RULE_NAME],
            CONF_INTEGRATION_FILTER: ", ".join(rule.get(CONF_INTEGRATION_FILTER, [])),
            CONF_DEVICE_IDS: ", ".join(rule.get(CONF_DEVICE_IDS, [])),
            CONF_ENTITY_IDS: ", ".join(rule.get(CONF_ENTITY_IDS, [])),
            CONF_ENTITY_GLOBS: ", ".join(rule.get(CONF_ENTITY_GLOBS, [])),
            CONF_ENTITY_REGEX_INCLUDE: ", ".join(
                rule.get(CONF_ENTITY_REGEX_INCLUDE, [])
            ),
            CONF_ENTITY_REGEX_EXCLUDE: ", ".join(
                rule.get(CONF_ENTITY_REGEX_EXCLUDE, [])
            ),
            CONF_KEEP_DAYS: rule.get(CONF_KEEP_DAYS, DEFAULT_KEEP_DAYS),
            CONF_ENABLED: rule.get(CONF_ENABLED, True),
        }
        return self.async_show_form(
            step_id="edit_rule_detail",
            data_schema=_rule_schema(defaults),
            errors=errors,
        )

    async def async_step_remove_rule(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Select a rule to remove."""
        if self._yaml_active or not self._rules:
            return await self.async_step_menu()

        if user_input is not None:
            self._rules.pop(int(user_input["rule_index"]))
            await self._save_rules()
            return await self.async_step_menu()

        options = {
            str(i): f"{i + 1}. {r[CONF_RULE_NAME]}" for i, r in enumerate(self._rules)
        }
        return self.async_show_form(
            step_id="remove_rule",
            data_schema=vol.Schema({vol.Required("rule_index"): vol.In(options)}),
        )

    async def async_step_done(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Finish the options flow."""
        return self.async_create_entry(title="", data={})

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    async def _save_rules(self) -> None:
        """Persist rules via the live manager's store and update its in-memory list."""
        manager = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id)
        if manager is not None:
            await manager.store.async_save({CONF_RULES: self._rules})
            manager.rules = list(self._rules)
        else:
            # Fallback: manager not loaded (shouldn't occur during a live options flow)
            store = storage.Store(self.hass, STORAGE_VERSION, STORAGE_KEY)
            await store.async_save({CONF_RULES: self._rules})


# ------------------------------------------------------------------ #
# Module-level helpers shared by both add and edit flows             #
# ------------------------------------------------------------------ #


def _rule_schema(defaults: dict | None = None) -> vol.Schema:
    d = defaults or {}
    return vol.Schema(
        {
            vol.Required(CONF_RULE_NAME, default=d.get(CONF_RULE_NAME, "")): str,
            vol.Optional(
                CONF_INTEGRATION_FILTER,
                default=d.get(CONF_INTEGRATION_FILTER, ""),
            ): str,
            vol.Optional(CONF_DEVICE_IDS, default=d.get(CONF_DEVICE_IDS, "")): str,
            vol.Optional(CONF_ENTITY_IDS, default=d.get(CONF_ENTITY_IDS, "")): str,
            vol.Optional(CONF_ENTITY_GLOBS, default=d.get(CONF_ENTITY_GLOBS, "")): str,
            vol.Optional(
                CONF_ENTITY_REGEX_INCLUDE,
                default=d.get(CONF_ENTITY_REGEX_INCLUDE, ""),
            ): str,
            vol.Optional(
                CONF_ENTITY_REGEX_EXCLUDE,
                default=d.get(CONF_ENTITY_REGEX_EXCLUDE, ""),
            ): str,
            vol.Required(
                CONF_KEEP_DAYS, default=d.get(CONF_KEEP_DAYS, DEFAULT_KEEP_DAYS)
            ): vol.All(int, vol.Range(min=1, max=365)),
            vol.Optional(CONF_ENABLED, default=d.get(CONF_ENABLED, True)): bool,
        }
    )


def _validate_rule_input(user_input: dict) -> dict[str, str]:
    errors: dict[str, str] = {}
    if not user_input.get(CONF_RULE_NAME, "").strip():
        errors[CONF_RULE_NAME] = "name_required"

    has_targets = any(
        user_input.get(f, "").strip()
        for f in (
            CONF_INTEGRATION_FILTER,
            CONF_DEVICE_IDS,
            CONF_ENTITY_IDS,
            CONF_ENTITY_GLOBS,
            CONF_ENTITY_REGEX_INCLUDE,
        )
    )
    if not has_targets:
        errors["base"] = "no_targets"

    # Validate regex patterns compile
    for field in (CONF_ENTITY_REGEX_INCLUDE, CONF_ENTITY_REGEX_EXCLUDE):
        for pattern in _split_csv(user_input.get(field, "")):
            try:
                re.compile(pattern)
            except re.error:
                errors[field] = "invalid_regex"
                break

    return errors


def _build_rule(user_input: dict) -> dict:
    """Parse comma-separated string fields into lists and return a rule dict."""
    return {
        CONF_RULE_NAME: user_input[CONF_RULE_NAME].strip(),
        CONF_INTEGRATION_FILTER: _split_csv(
            user_input.get(CONF_INTEGRATION_FILTER, "")
        ),
        CONF_DEVICE_IDS: _split_csv(user_input.get(CONF_DEVICE_IDS, "")),
        CONF_ENTITY_IDS: _split_csv(user_input.get(CONF_ENTITY_IDS, "")),
        CONF_ENTITY_GLOBS: _split_csv(user_input.get(CONF_ENTITY_GLOBS, "")),
        CONF_ENTITY_REGEX_INCLUDE: _split_csv(
            user_input.get(CONF_ENTITY_REGEX_INCLUDE, "")
        ),
        CONF_ENTITY_REGEX_EXCLUDE: _split_csv(
            user_input.get(CONF_ENTITY_REGEX_EXCLUDE, "")
        ),
        CONF_KEEP_DAYS: user_input[CONF_KEEP_DAYS],
        CONF_ENABLED: user_input.get(CONF_ENABLED, True),
    }
