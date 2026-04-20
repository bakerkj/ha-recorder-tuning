# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.
"""Tests for pure helper functions in config_flow.py."""

import pytest

from custom_components.recorder_tuning.config_flow import _valid_time


def test_valid_time_ok():
    assert _valid_time("03:00") == "03:00"


def test_valid_time_midnight():
    assert _valid_time("0:00") == "00:00"


def test_valid_time_end_of_day():
    assert _valid_time("23:59") == "23:59"


def test_valid_time_bad_format():
    import voluptuous as vol

    with pytest.raises(vol.Invalid):
        _valid_time("3am")


def test_valid_time_out_of_range():
    import voluptuous as vol

    with pytest.raises(vol.Invalid):
        _valid_time("25:00")
