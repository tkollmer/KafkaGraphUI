"""Shared test fixtures — mock kafka module so imports work without broker."""

import sys
import os
from unittest.mock import MagicMock

# Mock kafka module before any imports that depend on it
kafka_mock = MagicMock()
kafka_mock.errors = MagicMock()
kafka_mock.errors.KafkaError = Exception
kafka_mock.errors.KafkaConnectionError = Exception
kafka_mock.errors.TopicAuthorizationFailedError = Exception
kafka_mock.errors.GroupAuthorizationFailedError = Exception
sys.modules["kafka"] = kafka_mock
sys.modules["kafka.errors"] = kafka_mock.errors

# Add backend root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
