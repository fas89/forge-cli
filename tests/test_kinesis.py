"""Tests for providers/aws/actions/kinesis.py — input validation (mocked boto3)."""

import sys
import pytest
from unittest.mock import patch, MagicMock

# We need boto3 + botocore mocked since kinesis.py imports them inside functions
_mock_boto3 = MagicMock()
_mock_botocore = MagicMock()
_mock_botocore_exc = MagicMock()


def _patch_boto():
    """Context manager to make boto3 importable."""
    return patch.dict(sys.modules, {
        "boto3": _mock_boto3,
        "botocore": _mock_botocore,
        "botocore.exceptions": _mock_botocore_exc,
    })


from fluid_build.providers.aws.actions.kinesis import (
    ensure_stream,
    ensure_firehose,
    put_records,
    ensure_analytics_application,
)


# ── ensure_stream validation ─────────────────────────────────────────
class TestEnsureStreamValidation:
    def test_missing_stream_name(self):
        with _patch_boto():
            result = ensure_stream({})
        assert result["status"] == "error"
        assert "stream_name" in result["error"]
        assert result["changed"] is False

    def test_invalid_shard_count_zero(self):
        with _patch_boto():
            result = ensure_stream({"stream_name": "s", "shard_count": 0})
        assert result["status"] == "error"
        assert "shard_count" in result["error"]

    def test_invalid_shard_count_too_high(self):
        with _patch_boto():
            result = ensure_stream({"stream_name": "s", "shard_count": 500})
        assert result["status"] == "error"
        assert "shard_count" in result["error"]

    def test_invalid_shard_count_type(self):
        with _patch_boto():
            result = ensure_stream({"stream_name": "s", "shard_count": "abc"})
        assert result["status"] == "error"

    def test_invalid_retention_too_low(self):
        with _patch_boto():
            result = ensure_stream({"stream_name": "s", "retention_hours": 1})
        assert result["status"] == "error"
        assert "retention_hours" in result["error"]

    def test_invalid_retention_too_high(self):
        with _patch_boto():
            result = ensure_stream({"stream_name": "s", "retention_hours": 99999})
        assert result["status"] == "error"
        assert "retention_hours" in result["error"]

    def test_boto3_not_available(self):
        """When boto3 import fails, error is returned."""
        with patch.dict(sys.modules, {"boto3": None}):
            result = ensure_stream({"stream_name": "s"})
        assert result["status"] == "error"
        assert "boto3" in result["error"]


# ── ensure_firehose validation ───────────────────────────────────────
class TestEnsureFirehoseValidation:
    def test_missing_delivery_stream_name(self):
        with _patch_boto():
            result = ensure_firehose({})
        assert result["status"] == "error"
        assert "delivery_stream_name" in result["error"]

    def test_missing_s3_destination(self):
        with _patch_boto():
            result = ensure_firehose({"delivery_stream_name": "my-stream"})
        assert result["status"] == "error"
        assert "s3_destination" in result["error"]


# ── put_records validation ───────────────────────────────────────────
class TestPutRecordsValidation:
    def test_missing_stream_name(self):
        with _patch_boto():
            result = put_records({})
        assert result["status"] == "error"
        assert "stream_name" in result["error"]

    def test_empty_records(self):
        with _patch_boto():
            result = put_records({"stream_name": "s", "records": []})
        assert result["status"] == "ok"
        assert result["changed"] is False

    def test_batch_too_large(self):
        with _patch_boto():
            result = put_records({"stream_name": "s", "records": [{}] * 501})
        assert result["status"] == "error"
        assert "500" in result["error"]


# ── ensure_analytics_application validation ──────────────────────────
class TestEnsureAnalyticsValidation:
    def test_missing_application_name(self):
        with _patch_boto():
            result = ensure_analytics_application({})
        assert result["status"] == "error"
        assert "application_name" in result["error"]

    def test_missing_service_execution_role(self):
        with _patch_boto():
            result = ensure_analytics_application({"application_name": "app"})
        assert result["status"] == "error"
        assert "service_execution_role" in result["error"]

    def test_invalid_runtime(self):
        with _patch_boto():
            result = ensure_analytics_application({
                "application_name": "app",
                "service_execution_role": "arn:aws:iam::role/test",
                "runtime_environment": "INVALID",
            })
        assert result["status"] == "error"
        assert "runtime_environment" in result["error"]
