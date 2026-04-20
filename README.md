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

Recorder Tuning is **YAML-configured**. Add a top-level `recorder_tuning:` block
to `configuration.yaml`. Rules are typically pulled in via `!include` from a
separate file so the rule list doesn't clutter `configuration.yaml`:

```yaml
# configuration.yaml
recorder:
  purge_keep_days: 30 # global default for entities not covered by a rule
  auto_purge: false # recorder_tuning triggers the nightly purge itself
  auto_repack: false # recorder_tuning owns the repack cadence too

recorder_tuning:
  purge_time: "03:00"
  stats_keep_days: 60
  dry_run: true
  ha_recorder_purge:
    enabled: true # call recorder.purge after our rules run (default true)
    repack: monthly # cadence: never | weekly | monthly (HA's native 2nd Sunday)
    force_repack: false # always-repack override; ignores cadence when true
  rules: !include recorder_tuning.yaml
```

Setting `auto_purge: false` on the recorder is the recommended pairing: after
recorder_tuning runs its rules at `purge_time`, it calls `recorder.purge` itself
via the `ha_recorder_purge` block. That single trigger sweeps everything the
rules don't cover (respecting `purge_keep_days`) and fires the short-term-stats
patch. Leaving HA's own auto_purge on alongside works too — you just get two
purges a day instead of one.

```yaml
# recorder_tuning.yaml — the list of rules (no top-level wrapper key)
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

Reload after editing either file by calling the `recorder_tuning.reload` service
— no HA restart required. If the YAML fails validation the reload is rejected
and the previous configuration is preserved.

### Top-level fields

| Field               | Type           | Default       | Description                                                                                                                        |
| ------------------- | -------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `purge_time`        | `HH:MM` string | `03:00`       | Time of day to run the per-entity purge rules.                                                                                     |
| `stats_keep_days`   | int, 1-365     | `30`          | Days of 5-minute statistics to retain. Must be ≥ recorder's `purge_keep_days`.                                                     |
| `dry_run`           | bool           | `true`        | When true, every run logs what would be deleted without touching any data.                                                         |
| `ha_recorder_purge` | block          | _(see below)_ | Controls the trailing global `recorder.purge` call that runs after the rules. Fields: `enabled`, `repack`, `force_repack` (below). |
| `rules`             | list of rules  | `[]`          | Per-entity purge rules. Use `!include` to pull from a separate file (see example).                                                 |

**`ha_recorder_purge` sub-fields:**

| Field          | Type | Default   | Description                                                                                                                                               |
| -------------- | ---- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`      | bool | `true`    | After per-entity rules finish, call HA's `recorder.purge` so the global `purge_keep_days` sweeps uncovered entities and the short-term-stats patch fires. |
| `repack`       | enum | `monthly` | Repack cadence when `force_repack` is false. `monthly` = second Sunday of the month (HA native). Other values: `weekly` (Sundays) or `never`.             |
| `force_repack` | bool | `false`   | Force a repack on every purge run (expensive). Overrides `repack` when true. Leave off unless you know you want it.                                       |

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

### Dry-run precedence

For any given rule on any given run, the effective dry-run value is resolved in
this order:

1. **Top-level `dry_run: true`** in `configuration.yaml` is a **safety lock**.
   Nothing below can override it — no service call, no per-rule setting. While
   this lock is on, no data can be deleted, period.
2. **Service-call `dry_run:`** (if explicitly passed to
   `recorder_tuning.run_purge_now`) wins over per-rule settings.
3. **Per-rule `dry_run:`** (if set on that rule) wins over the top-level
   fallback.
4. **Top-level `dry_run:`** (false at this point) is the fallback.

Concrete implications:

- `dry_run: true` at the top is how you go live: your rules do nothing no matter
  what they say. You flip it to `false` to unlock.
- With the lock off, a rule with `dry_run: true` stays dry on scheduled runs —
  useful to pause an individual rule during rollout without disabling it.
- A service call with `dry_run: false` forces a rule to run live _for that one
  call only_ (but only if the top-level lock is off). Useful for one-off
  testing.
- A service call with `dry_run: true` forces every rule in that call to stay dry
  regardless of per-rule settings.

### Typical rollout path

1. Set `dry_run: true` on every rule in the YAML (the safety default).
2. Set top-level `dry_run: true`. Deploy. Watch the log for a few nights.
3. Flip top-level to `dry_run: false` — nothing changes yet because every rule
   is still individually `dry_run: true`.
4. Pick one rule you trust. Flip _its_ `dry_run: false`. Reload. It now runs
   live on the next schedule. Verify in the log.
5. Repeat for each rule until all are live; delete the per-rule `dry_run:` lines
   when you're fully confident.

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
| `dry_run`              | bool            | no         | Per-rule override. If set, forces this rule into dry-run (`true`) or live (`false`) regardless of the top-level setting.          |
| `match_mode`           | `all` \| `any`  | no (`all`) | How positive selectors combine within the rule. `all` = intersection; `any` = union.                                              |
| `integration_filter`   | list of strings | no         | Integration/platform names, e.g. `[frigate, esphome]`.                                                                            |
| `device_ids`           | list of strings | no         | Device IDs. All entities under each device (including disabled ones) are included. Find IDs at Settings → Devices → (device) URL. |
| `entity_ids`           | list of strings | no         | Explicit entity IDs.                                                                                                              |
| `entity_globs`         | list of strings | no         | Glob patterns matched against all registered entity IDs, e.g. `sensor.frigate_*_fps`.                                             |
| `entity_regex_include` | list of regexes | no         | Entities matching any pattern.                                                                                                    |
| `entity_regex_exclude` | list of regexes | no         | Entities matching any pattern are removed from the candidate set after positive selectors have run.                               |

At least one positive selector (`integration_filter`, `device_ids`,
`entity_ids`, `entity_globs`, or `entity_regex_include`) is required per rule.
Invalid YAML fails the whole `recorder_tuning:` block at startup (or the reload
call) — nothing is partially applied.

## Services

### `recorder_tuning.run_purge_now`

Immediately run enabled per-entity purge rules. Useful for testing before
waiting for the overnight run.

Unlike the scheduled nightly run, the trailing global `recorder.purge` is
**skipped by default** on a manual invocation — the common use case is testing
rules, and the global sweep is slow enough to be surprising by hand. Pass
`run_recorder_purge: true` to opt into the full nightly flow.

| Parameter           | Type        | Default             | Description                                                                                                                                                                                                        |
| ------------------- | ----------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `dry_run`           | bool        | _(inherits config)_ | Override dry-run mode for this call only. Omit to use the configured setting.                                                                                                                                      |
| `rule_names`        | list of str | _(all rules)_       | Restrict the run to rules whose `name:` matches (case-insensitive). Unknown names log a warning but don't abort. When provided, the trailing `recorder.purge` is always skipped regardless of `ha_recorder_purge`. |
| `keep_days`         | int, 1-365  | _(rule's value)_    | One-off override of `keep_days` for every rule that runs in this call. Does not persist. Useful for `rule_names`-targeted "what if I made this more aggressive" experiments.                                       |
| `ha_recorder_purge` | bool        | `false`             | Opt into the trailing HA `recorder.purge` call after the rules run — matches the nightly flow. Ignored when `rule_names` is provided.                                                                              |

```yaml
# Dry-run just one rule to verify its match set + log output
service: recorder_tuning.run_purge_now
data:
  dry_run: true
  rule_names:
    - "Frigate camera metrics"
```

```yaml
# "What if I made this rule more aggressive?" — dry-run one rule with a
# shorter retention to preview the extra rows that would be purged
service: recorder_tuning.run_purge_now
data:
  dry_run: true
  rule_names:
    - "Frigate camera metrics"
  keep_days: 3
```

```yaml
# Fire the full nightly flow on demand (rules + global recorder.purge)
service: recorder_tuning.run_purge_now
data:
  ha_recorder_purge: true
```

```yaml
# Force a live purge of rules (global recorder.purge still skipped) even while
# dry-run mode is ON in config
service: recorder_tuning.run_purge_now
data:
  dry_run: false
```

### `recorder_tuning.reload`

Reload `recorder_tuning:` (and any `!include`'d rules file) from
`configuration.yaml` without restarting Home Assistant. If the YAML is invalid
the reload is rejected and the previous configuration is preserved.

```yaml
service: recorder_tuning.reload
```

## How It Works

### Entity purge rules

1. At the configured time each day, the integration iterates all enabled rules.
2. For each rule it resolves a candidate entity set from the positive selectors
   (intersected or unioned per `match_mode`).
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

- Disabling a rule (`enabled: false`) suspends it without losing its
  configuration.
- Removing the `recorder_tuning:` block and restarting HA restores the original
  short-term statistics purge behavior.
