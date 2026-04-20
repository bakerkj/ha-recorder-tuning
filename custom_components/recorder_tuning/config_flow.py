# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Config flow for Recorder Tuning.

Rules are defined exclusively in ``recorder_tuning.yaml`` in the HA config
directory. This flow manages only the non-rule settings: purge schedule,
short-term statistics retention, and dry-run mode.
"""

from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback

from . import _parse_hhmm
from .const import (
    CONF_DRY_RUN,
    CONF_PURGE_TIME,
    CONF_STATS_KEEP_DAYS,
    DEFAULT_DRY_RUN,
    DEFAULT_PURGE_TIME,
    DEFAULT_STATS_KEEP_DAYS,
    DOMAIN,
)


def _valid_time(value: str) -> str:
    """Validate HH:MM time string and return it zero-padded."""
    try:
        parsed = _parse_hhmm(value)
    except (TypeError, ValueError) as err:
        raise vol.Invalid("Time must be in HH:MM format, e.g. 03:00") from err
    return parsed.strftime("%H:%M")


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
    """Options flow for schedule, stats retention, and dry-run toggle.

    Purge rules are not editable here — define them in
    ``recorder_tuning.yaml`` and call ``recorder_tuning.reload``.
    """

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        return await self.async_step_menu()

    async def async_step_menu(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Show menu of non-rule settings."""
        dry_run_status = (
            "ON" if self.config_entry.data.get(CONF_DRY_RUN, DEFAULT_DRY_RUN) else "OFF"
        )
        return self.async_show_menu(
            step_id="menu",
            menu_options=[
                "set_schedule",
                "set_stats_retention",
                "set_dry_run",
                "done",
            ],
            description_placeholders={"dry_run": dry_run_status},
        )

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

    async def async_step_done(
        self, user_input: dict | None = None
    ) -> config_entries.ConfigFlowResult:
        """Finish the options flow."""
        return self.async_create_entry(title="", data={})
