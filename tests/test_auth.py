"""Tests for fluid_build/auth.py — GCP authentication."""
import os
import pytest
from unittest.mock import patch, MagicMock

from fluid_build.auth import GcpAuthResult, authenticate, doctor


class TestGcpAuthResult:
    def test_dataclass(self):
        result = GcpAuthResult(credentials="mock_cred", project_id="proj1", mode="adc")
        assert result.credentials == "mock_cred"
        assert result.project_id == "proj1"
        assert result.mode == "adc"


class TestAuthenticate:
    def test_unknown_mode(self):
        with pytest.raises(ValueError, match="Unknown auth mode"):
            authenticate("unknown-mode", None)

    def test_sa_key_missing_path(self):
        with pytest.raises(ValueError, match="Pass --credentials"):
            authenticate("sa-key", None)

    def test_external_missing_path(self):
        with pytest.raises(ValueError, match="Pass --credentials"):
            authenticate("external", None)

    def test_sa_key_file_not_found(self):
        with pytest.raises((FileNotFoundError, RuntimeError)):
            authenticate("sa-key", "/nonexistent/service-account.json")

    def test_external_file_not_found(self):
        with pytest.raises((FileNotFoundError, RuntimeError)):
            authenticate("external", "/nonexistent/external.json")

    def test_default_mode_is_adc(self):
        # Default mode should be "adc"
        with patch("fluid_build.auth._adc") as mock_adc:
            mock_adc.return_value = GcpAuthResult(credentials="fake", project_id="proj", mode="adc")
            result = authenticate(None, None)
            assert result.mode == "adc"
            mock_adc.assert_called_once()


class TestDoctor:
    def test_basic(self):
        info = doctor("gcp", "my-project")
        assert info["provider"] == "gcp"
        assert info["project"] == "my-project"

    def test_no_project(self):
        with patch.dict(os.environ, {}, clear=True):
            info = doctor("gcp", None)
            assert info["provider"] == "gcp"
            # project might be None if no env vars set
            assert "project" in info

    def test_env_vars(self):
        with patch.dict(os.environ, {
            "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/creds.json",
            "GOOGLE_CLOUD_QUOTA_PROJECT": "quota-proj",
        }):
            info = doctor("gcp", "my-proj")
            assert info["adc_env"] == "/path/to/creds.json"
            assert info["quota_project"] == "quota-proj"
