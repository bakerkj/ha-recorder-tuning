# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Coherence tests between ``config_flow.py`` and ``strings.json``.

A typo or a stale entry after a refactor (e.g., removing a step from
``strings.json`` but forgetting the method, or vice versa) renders fine
in English but breaks localisation and menu display. This test pins the
two sides together so drift is caught at CI time, not when a user opens
the options dialog.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path


# Options-flow steps that render UI. ``init`` is the entry point — HA
# forwards it to ``menu`` — so strings.json doesn't need an ``init`` entry.
# ``done`` produces no form (it just creates an empty entry).
_UI_STEPS = {"menu", "set_schedule", "set_stats_retention", "set_dry_run"}


def _strings() -> dict:
    path = (
        Path(__file__).parent.parent
        / "custom_components"
        / "recorder_tuning"
        / "strings.json"
    )
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def test_every_strings_options_step_has_a_method():
    """If strings.json references a step, the flow must implement it."""
    from custom_components.recorder_tuning.config_flow import (
        RecorderTuningOptionsFlow,
    )

    steps = _strings()["options"]["step"].keys()
    for step_id in steps:
        method_name = f"async_step_{step_id}"
        assert hasattr(RecorderTuningOptionsFlow, method_name), (
            f"strings.json declares options.step.{step_id} but "
            f"RecorderTuningOptionsFlow.{method_name} is missing"
        )


def test_every_ui_step_method_has_strings_entry():
    """Every UI-producing step method must appear in strings.json."""
    steps = _strings()["options"]["step"]
    missing = [s for s in _UI_STEPS if s not in steps]
    assert not missing, (
        f"options.step is missing entries for: {missing}. "
        f"strings.json and config_flow.py have drifted."
    )


def test_ui_steps_set_matches_flow_methods():
    """The known UI-producing steps map 1-to-1 to flow methods.

    Guards against someone adding an async_step_* that renders UI without
    updating strings.json, or removing a step without trimming strings.
    """
    from custom_components.recorder_tuning.config_flow import (
        RecorderTuningOptionsFlow,
    )

    step_methods = {
        name[len("async_step_") :]
        for name, _ in inspect.getmembers(
            RecorderTuningOptionsFlow, predicate=inspect.isfunction
        )
        if name.startswith("async_step_")
    }
    # init and done exist on the flow but aren't in _UI_STEPS; everything
    # else on the flow should be covered by the UI-steps set.
    non_ui = {"init", "done"}
    assert step_methods == _UI_STEPS | non_ui, (
        f"async_step_* methods on RecorderTuningOptionsFlow ({step_methods}) "
        f"don't match the expected set ({_UI_STEPS | non_ui}). "
        f"Update _UI_STEPS here and strings.json together."
    )


def test_config_flow_user_step_in_strings():
    """The initial config flow step must also be present."""
    strings = _strings()
    assert "user" in strings["config"]["step"], (
        "config.step.user is missing — the initial setup form will render "
        "without title/description."
    )
