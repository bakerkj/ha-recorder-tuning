# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Constants for Recorder Tuning."""

DOMAIN = "recorder_tuning"

# Config entry data keys
CONF_PURGE_TIME = "purge_time"
CONF_STATS_KEEP_DAYS = "stats_keep_days"

# Rule field keys
CONF_RULES = "rules"
CONF_RULE_NAME = "name"
CONF_ENABLED = "enabled"
CONF_KEEP_DAYS = "keep_days"

# Rule target selectors
CONF_INTEGRATION_FILTER = "integration_filter"  # list of integration/platform names
CONF_DEVICE_IDS = "device_ids"
CONF_ENTITY_IDS = "entity_ids"
CONF_ENTITY_GLOBS = "entity_globs"
CONF_ENTITY_REGEX_INCLUDE = "entity_regex_include"  # entity must match at least one
CONF_ENTITY_REGEX_EXCLUDE = "entity_regex_exclude"  # entity excluded if matches any

# How the positive selectors within a rule combine. "all" (default) means an
# entity must satisfy every present selector — the natural mental model of
# "each selector narrows the rule". "any" is the legacy union mode.
CONF_MATCH_MODE = "match_mode"
MATCH_MODE_ALL = "all"
MATCH_MODE_ANY = "any"
DEFAULT_MATCH_MODE = MATCH_MODE_ALL

# Defaults
DEFAULT_PURGE_TIME = "03:00"
DEFAULT_STATS_KEEP_DAYS = 30
DEFAULT_DRY_RUN = True

# Dry-run mode
CONF_DRY_RUN = "dry_run"

# ``run_purge_now`` service parameter: optional list of rule names to restrict
# the run to. When provided, only matching rules are executed AND the trailing
# global ``recorder.purge`` call is skipped (targeted runs are usually about
# testing specific rules, not the global sweep).
CONF_RULE_NAMES = "rule_names"

# After per-entity rules finish, optionally call HA's ``recorder.purge``
# service so the global ``purge_keep_days`` sweeps whatever the rules don't
# cover AND the short-term stats monkey-patch gets a chance to fire.
# Default True means recorder_tuning owns the nightly purge timing; set
# ``auto_purge: false`` on the recorder itself to avoid double-firing.
CONF_RUN_RECORDER_PURGE = "run_recorder_purge"
DEFAULT_RUN_RECORDER_PURGE = True
# ``recorder_purge_repack`` is the "always repack on every purge run" override
# — expensive, usually not what you want. If false, the cadence is controlled
# by ``auto_repack`` below.
CONF_RECORDER_PURGE_REPACK = "recorder_purge_repack"
DEFAULT_RECORDER_PURGE_REPACK = False

# Auto-repack cadence — three presets mirror the common choices:
#   never    — no repack (override-only)
#   weekly   — every Sunday
#   monthly  — second Sunday of the month (HA's native auto_repack cadence)
CONF_AUTO_REPACK = "auto_repack"
AUTO_REPACK_NEVER = "never"
AUTO_REPACK_WEEKLY = "weekly"
AUTO_REPACK_MONTHLY = "monthly"
DEFAULT_AUTO_REPACK = AUTO_REPACK_MONTHLY
