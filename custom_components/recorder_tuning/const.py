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

# Nested config block controlling how (and whether) we trigger HA's own
# ``recorder.purge`` service after our per-entity rules finish. All fields
# below live inside ``ha_recorder_purge:`` in configuration.yaml.
#
# ``enabled``     — if true, after the rules run call ``recorder.purge`` so
#                   the global ``purge_keep_days`` sweeps what rules don't
#                   cover AND the short-term stats monkey-patch fires. Intended
#                   to replace HA's ``auto_purge`` — set ``auto_purge: false``
#                   on the recorder itself to avoid double-firing.
# ``repack``      — cadence for ``repack=True`` on the purge call. Presets:
#                     never    — no scheduled repack
#                     weekly   — every Sunday
#                     monthly  — second Sunday of the month (HA's native
#                                ``auto_repack`` cadence)
# ``force_repack``— always-repack override. If true, repack on every purge run
#                   regardless of ``repack`` cadence. Expensive.
CONF_HA_RECORDER_PURGE = "ha_recorder_purge"
CONF_HA_RECORDER_PURGE_ENABLED = "enabled"
CONF_HA_RECORDER_PURGE_REPACK = "repack"
CONF_HA_RECORDER_PURGE_FORCE_REPACK = "force_repack"

DEFAULT_HA_RECORDER_PURGE_ENABLED = True
REPACK_NEVER = "never"
REPACK_WEEKLY = "weekly"
REPACK_MONTHLY = "monthly"
DEFAULT_HA_RECORDER_PURGE_REPACK = REPACK_MONTHLY
DEFAULT_HA_RECORDER_PURGE_FORCE_REPACK = False
