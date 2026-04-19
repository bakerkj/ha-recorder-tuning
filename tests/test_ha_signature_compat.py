# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Guard against HA changing the signature of the function we monkey-patch.

``_apply_stats_patch`` in ``custom_components/recorder_tuning/__init__.py``
replaces
``homeassistant.components.recorder.purge.find_short_term_statistics_to_purge``
with a wrapper. If HA renames, moves, or changes its parameters, the patch
silently no-ops — the broad ``except`` in ``_apply_stats_patch`` swallows the
AttributeError with only a warning log.

These tests fail loudly instead, so:
  * bumping the HA pin locally surfaces the drift at PR time, and
  * the scheduled ``ha-dev-compat`` workflow catches upstream changes on HA
    ``dev`` before they land in a release.

If this test fails:
  1. Inspect the new signature upstream.
  2. Update ``EXPECTED_PARAMS`` below AND
     ``patched_find_short_term_statistics_to_purge`` in
     ``custom_components/recorder_tuning/__init__.py`` together.
"""

from __future__ import annotations

import inspect


EXPECTED_PARAMS: tuple[str, ...] = ("purge_before", "max_bind_vars")


def test_find_short_term_statistics_to_purge_exists() -> None:
    from homeassistant.components.recorder import purge  # noqa: PLC0415

    assert hasattr(purge, "find_short_term_statistics_to_purge"), (
        "homeassistant.components.recorder.purge.find_short_term_statistics_to_purge "
        "has been removed or renamed — update _apply_stats_patch in "
        "custom_components/recorder_tuning/__init__.py."
    )


def test_find_short_term_statistics_to_purge_signature() -> None:
    from homeassistant.components.recorder import purge  # noqa: PLC0415

    sig = inspect.signature(purge.find_short_term_statistics_to_purge)
    params = tuple(sig.parameters)

    assert params == EXPECTED_PARAMS, (
        f"HA signature drift: expected {EXPECTED_PARAMS!r}, got {params!r}. "
        f"Update patched_find_short_term_statistics_to_purge in "
        f"custom_components/recorder_tuning/__init__.py to match."
    )
