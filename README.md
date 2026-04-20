# Recorder Tuning

A Home Assistant custom integration that fills two gaps the built-in recorder
leaves open:

1. **Per-entity purge rules** — automatically purge different entities on
   different schedules without touching the global `purge_keep_days`.
2. **Short-term statistics retention** — keep 5-minute statistics longer than
   raw state history, decoupling the two purge windows.

## Why

Home Assistant's recorder has a single `purge_keep_days` knob that controls
everything. There is no native way to:

- Flush high-frequency sensors (Frigate FPS, ESPHome debug values) after 3 days
  while keeping energy sensors for 30 days.
- Retain 5-minute statistics for 60 days while purging raw state history after
  30 days.

This integration provides both.

## Installation

### HACS (recommended)

Add this repository as a custom HACS integration repository, then install
**Recorder Tuning**.

### Manual

Copy `custom_components/recorder_tuning/` into your `config/custom_components/`
directory and restart Home Assistant.

## Setup

1. Go to **Settings → Integrations → Add Integration** and search for **Recorder
   Tuning**.
2. Set your preferred **daily purge time** (24h format, e.g. `03:00`).
3. Set the **short-term statistics retention** in days (must be ≥ your
   recorder's `purge_keep_days`; default 30).
4. Leave **dry-run mode** enabled (the default) until you have reviewed the logs
   and confirmed your rules are correct.
5. Create `recorder_tuning.yaml` in your HA config directory to define purge
   rules — see [YAML Configuration](#yaml-configuration) below. Call the
   `recorder_tuning.reload` service after editing the file.

Also configure `recorder` in `configuration.yaml`:

```yaml
recorder:
  purge_keep_days: 30 # global default for entities not covered by a rule
```

## Dry-Run Mode

**Dry-run mode is enabled by default.** When active, every purge run — scheduled
and manual — logs exactly what would be deleted without touching any data. Each
log line includes the entity ID, row count, and the date range that would be
removed.

Example log output:

```
[PURGE] rule 'ESPHome diagnostic sensors' (keep 7d) — 241 of 331 matched entities have data older than 2026-03-29 03:00 UTC (8,432,180 rows total)
[PURGE]   sensor.workshop_co2_uptime                            142543 rows  2026-01-01 00:00 UTC → 2026-03-29 03:00 UTC
[PURGE]   sensor.porch_light_voltage                             98201 rows  2026-01-15 08:12 UTC → 2026-03-29 03:00 UTC
[PURGE] rule 'Ping RTT' (keep 14d) — nothing to purge
```

In dry-run mode the prefix reads `[DRY RUN]` instead of `[PURGE]`. The same
per-entity detail is logged in both modes so you can always audit what was (or
would have been) deleted.

To disable dry-run and start live purging:

- **UI**: Settings → Integrations → Recorder Tuning → Configure → Toggle dry-run
  mode
- **Service**: call `recorder_tuning.run_purge_now` with `dry_run: false` for a
  one-off live purge while leaving the persistent setting unchanged.
- **Per-rule**: add `dry_run: true` (or `false`) to an individual rule in
  `recorder_tuning.yaml` to force its mode regardless of the integration-wide
  setting. Useful when iterating on a newly-added rule while leaving the rest of
  the rule set live.

## YAML Configuration

Purge rules are defined exclusively in `recorder_tuning.yaml` in your HA config
directory (alongside `configuration.yaml`). If the file is missing or invalid
the integration runs with zero rules active; state and stats retention still
apply.

```yaml
# recorder_tuning.yaml

rules:
  - name: Frigate camera metrics
    integration_filter: [frigate]
    keep_days: 7

  - name: ESPHome diagnostic sensors
    integration_filter: [esphome]
    entity_regex_include:
      - "_voltage$"
      - "_uptime$"
      - "_wifi_signal$"
    keep_days: 7

  - name: Davis weather station
    integration_filter: [mqtt]
    entity_regex_include: ["^sensor\\.davis_"]
    keep_days: 30
```

After editing the file, call `recorder_tuning.reload` to hot-swap the rules
without restarting Home Assistant. The integration does **not** watch the file
for changes — edits only take effect once you fire the reload service (or
restart HA). If the reload surfaces a parse / schema error, the previous rule
set is preserved.

### Rule matching

All enabled rules are applied on every purge run. If an entity matches more than
one rule, **every matching rule runs** and the most aggressive `keep_days`
(lowest value) determines how much history is kept. Rule order does not affect
the result.

Within a single rule, each _present_ positive selector becomes a predicate. How
those predicates combine is controlled by `match_mode`:

- `match_mode: all` (default) — the entity must satisfy **every** present
  selector. Adding a selector narrows the rule. This is what the rule name
  usually implies ("ESPHome diagnostic sensors" means entities that are ESPHome
  **and** look like diagnostics).
- `match_mode: any` — the entity matches if **any** selector matches (union).
  Useful for "this list of specific entities, plus anything matching this
  pattern."

Within a single selector, list items still OR together — e.g.
`integration_filter: [esphome, mqtt]` means the platform is `esphome` or `mqtt`;
`entity_regex_include: [p1, p2]` means any pattern matches.

`entity_regex_exclude` is always applied **after** positive selectors and
subtracts from the final set, regardless of `match_mode`.

### Rule fields

| Field                  | Type            | Required   | Description                                                                                                                       |
| ---------------------- | --------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `name`                 | string          | yes        | Identifier for the rule (free-form).                                                                                              |
| `keep_days`            | int, 1-365      | yes        | Days of recorder history to retain for matched entities.                                                                          |
| `enabled`              | bool            | no (true)  | Set to `false` to suspend a rule without deleting it.                                                                             |
| `dry_run`              | bool            | no         | Per-rule override. If set, forces this rule into dry-run (`true`) or live (`false`) regardless of the integration-wide setting.   |
| `match_mode`           | `all` \| `any`  | no (`all`) | How positive selectors combine within the rule. `all` = intersection; `any` = union.                                              |
| `integration_filter`   | list of strings | no         | Integration/platform names, e.g. `[frigate, esphome]`.                                                                            |
| `device_ids`           | list of strings | no         | Device IDs. All entities under each device (including disabled ones) are included. Find IDs at Settings → Devices → (device) URL. |
| `entity_ids`           | list of strings | no         | Explicit entity IDs.                                                                                                              |
| `entity_globs`         | list of strings | no         | Glob patterns matched against all registered entity IDs, e.g. `sensor.frigate_*_fps`.                                             |
| `entity_regex_include` | list of regexes | no         | Entities matching any pattern.                                                                                                    |
| `entity_regex_exclude` | list of regexes | no         | Entities matching any pattern are removed from the candidate set after positive selectors have run.                               |

At least one positive selector (`integration_filter`, `device_ids`,
`entity_ids`, `entity_globs`, or `entity_regex_include`) is required per rule.
Invalid rules (missing `name`, missing `keep_days`, out-of-range `keep_days`,
bad regex, unknown `match_mode`, etc.) are skipped individually — other valid
rules in the same file still run.

## Services

### `recorder_tuning.run_purge_now`

Immediately run all enabled purge rules. Useful for testing before waiting for
the overnight run.

| Parameter | Type | Default             | Description                                                                   |
| --------- | ---- | ------------------- | ----------------------------------------------------------------------------- |
| `dry_run` | bool | _(inherits config)_ | Override dry-run mode for this call only. Omit to use the persistent setting. |

```yaml
# Force a live purge even while dry-run mode is ON in config
service: recorder_tuning.run_purge_now
data:
  dry_run: false
```

### `recorder_tuning.reload`

Reload rules from `recorder_tuning.yaml` in the HA config directory without
restarting Home Assistant. If the file is missing or invalid the integration
runs with zero rules.

```yaml
service: recorder_tuning.reload
```

## How It Works

### Entity purge rules

1. At the configured time each day, the integration iterates all enabled rules.
2. For each rule it builds a candidate set using the union of all positive
   selectors (integrations, devices, entity IDs, glob patterns, regex include).
3. Regex exclude patterns are applied to the candidate set, removing any
   matches.
4. The DB is queried to log the row count and date range for every entity with
   purgeable data (visible in Home Assistant logs regardless of dry-run mode).
5. Unless dry-run mode is active, the resolved entity list is passed to HA's
   built-in `recorder.purge_entities` service with the rule's `keep_days`. Calls
   are batched in groups of 100.

### Short-term statistics retention

Home Assistant's recorder purges both `states` and `statistics_short_term` using
the same `purge_keep_days` cutoff. This integration monkey-patches
`homeassistant.components.recorder.purge.find_short_term_statistics_to_purge` at
startup, substituting a longer cutoff for the short-term statistics step only.
The `states` table continues to use your recorder's `purge_keep_days` unchanged.

The patch is conservative — it will never purge short-term statistics _more_
aggressively than the recorder would by default.

> **Fragility note**: This patches internal HA APIs and may break on future HA
> updates. Check after each major upgrade. Tested against Home Assistant
> 2026.3.x.

## Notes

- Only one instance of Recorder Tuning can be configured (single-entry
  integration).
- Disabling a rule (rather than deleting it) suspends it without losing its
  configuration.
- Removing the integration restores the original short-term statistics purge
  behavior on the next HA restart.
