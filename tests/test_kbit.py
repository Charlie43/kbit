#!/usr/bin/env python

"""Tests for `kbit` package."""

import pytest

from kbit import kbit


def test(response):
    assert kbit.main() == 1
