#!/usr/bin/env python

"""Tests for `kbit_src` package."""

import pytest

from kbit_src import kbit


def test(response):
    assert kbit.main() == 1
