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
5. Click **Configure** on the integration card to add entity purge rules.

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
[PURGE] rule 'ESPHome diagnostic sensors' — 241 of 331 matched entities have data older than 2026-03-29 03:00 UTC (8,432,180 rows total)
[PURGE]   sensor.workshop_co2_uptime                            142543 rows  2026-01-01 00:00 UTC → 2026-03-29 03:00 UTC
[PURGE]   sensor.porch_light_voltage                             98201 rows  2026-01-15 08:12 UTC → 2026-03-29 03:00 UTC
[PURGE] rule 'Ping RTT' — nothing to purge
```

In dry-run mode the prefix reads `[DRY RUN]` instead of `[PURGE]`. The same
per-entity detail is logged in both modes so you can always audit what was (or
would have been) deleted.

To disable dry-run and start live purging:

- **UI**: Settings → Integrations → Recorder Tuning → Configure → Toggle dry-run
  mode
- **Service**: call `recorder_tuning.run_purge_now` with `dry_run: false` for a
  one-off live purge while leaving the persistent setting unchanged.

## YAML Configuration

Rules can be managed in bulk by placing a `recorder_tuning.yaml` file in your HA
config directory alongside `configuration.yaml`. When the file is present it
takes full precedence over any rules stored via the UI.

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
without restarting Home Assistant. Removing the file and reloading reverts to
rules stored via the UI.

While YAML rules are active, `add_rule` and `remove_rule` service calls are
blocked with a warning — edit the file and reload instead.

## Configuring Purge Rules

Each rule targets a set of entities and assigns a `keep_days` value. Open
**Configure** on the integration card and choose **Add purge rule**.

### Rule matching

All enabled rules are applied on every purge run. If an entity matches more than
one rule, **every matching rule runs** and the most aggressive `keep_days`
(lowest value) determines how much history is kept. Place more specific rules
with narrower `keep_days` before broad integration-wide rules only for
readability — order does not affect the result.

### Target selectors

| Field                    | Description                                                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Integrations**         | Comma-separated integration names, e.g. `frigate, esphome`. All entities belonging to those integrations are included.                            |
| **Device IDs**           | Comma-separated device IDs. All non-disabled entities under each device are included. Find IDs in Settings → Devices & Services → (device) → URL. |
| **Entity IDs**           | Comma-separated explicit entity IDs.                                                                                                              |
| **Entity glob patterns** | Comma-separated glob patterns matched against all registered entity IDs, e.g. `sensor.frigate_*_fps`.                                             |
| **Regex include**        | Comma-separated regular expressions. Entities matching any pattern are added to the candidate set.                                                |
| **Regex exclude**        | Comma-separated regular expressions. Entities matching any pattern are removed from the candidate set, even if another selector matched them.     |

At least one positive selector (integration, device, entity ID, glob, or regex
include) is required per rule.

Regex exclude is applied **after** all positive selectors, acting as a final
filter:

```
integration_filter: esphome
entity_regex_exclude: _debug$, _raw$
```

### Example rules

| Rule Name              | Target                                                          | Keep Days |
| ---------------------- | --------------------------------------------------------------- | --------- |
| Frigate high-frequency | `entity_globs: sensor.frigate_*_fps, sensor.frigate_*_skipped`  | 3         |
| GPU / system stats     | `entity_globs: sensor.gpu_*, sensor.cpu_*`                      | 7         |
| Energy sensors         | `entity_globs: sensor.*_energy, sensor.*_power`                 | 15        |
| All ESPHome (no debug) | `integration_filter: esphome` + `entity_regex_exclude: _debug$` | 14        |
| Specific camera        | `device_ids: <device_id>`                                       | 5         |

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
restarting Home Assistant. If the file does not exist, reverts to rules stored
via the UI.

```yaml
service: recorder_tuning.reload
```

### `recorder_tuning.add_rule`

Add or update a rule from an automation or script. Blocked when YAML rules are
active.

```yaml
service: recorder_tuning.add_rule
data:
  name: "Frigate high-frequency"
  entity_globs:
    - sensor.frigate_*_fps
    - sensor.frigate_*_skipped
  keep_days: 3
  enabled: true
```

### `recorder_tuning.remove_rule`

Remove a rule by name. Blocked when YAML rules are active.

```yaml
service: recorder_tuning.remove_rule
data:
  name: "Frigate high-frequency"
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
6. Rules persist across restarts via HA's built-in storage API
   (`/.storage/recorder_tuning.rules`).

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
- When `recorder_tuning.yaml` is present, the UI rule list is read-only — edit
  the file and call `recorder_tuning.reload`.
