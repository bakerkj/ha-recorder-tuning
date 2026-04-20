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

# YAML config file — the only source of truth for purge rules. Lives in the
# HA config dir alongside configuration.yaml.
YAML_CONFIG_FILE = "recorder_tuning.yaml"

# Dry-run mode
CONF_DRY_RUN = "dry_run"
