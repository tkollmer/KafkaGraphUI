"""Tests for grouping engine."""

import pytest
from grouping_engine import GroupingEngine


class TestGroupingEngine:
    def test_default_grouping(self):
        engine = GroupingEngine()
        result = engine.preview_grouping(
            ["payment-service-abc12", "payment-service-def34"],
            engine.pattern,
        )
        assert isinstance(result, dict)

    def test_custom_regex(self):
        engine = GroupingEngine(regex_pattern=r"-\d+$")
        result = engine.preview_grouping(
            ["worker-1", "worker-2", "processor-1"],
            engine.pattern,
        )
        assert isinstance(result, dict)

    def test_empty_input(self):
        engine = GroupingEngine()
        result = engine.preview_grouping([], engine.pattern)
        assert isinstance(result, dict)
