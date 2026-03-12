"""Tests for fluid_build.cli.auth — AuthStatus, AuthResult, CLIError dataclasses."""
from fluid_build.cli.auth import AuthStatus, AuthResult, CLIError as AuthCLIError


class TestAuthStatus:
    def test_values(self):
        assert AuthStatus.AUTHENTICATED.value == "authenticated"
        assert AuthStatus.NOT_AUTHENTICATED.value == "not_authenticated"
        assert AuthStatus.EXPIRED.value == "expired"
        assert AuthStatus.ERROR.value == "error"
        assert AuthStatus.UNKNOWN.value == "unknown"


class TestAuthResult:
    def test_defaults(self):
        r = AuthResult(provider="gcp", status=AuthStatus.AUTHENTICATED)
        assert r.provider == "gcp"
        assert r.status == AuthStatus.AUTHENTICATED
        assert r.user_info == {}
        assert r.credentials_path is None
        assert r.scopes == []
        assert r.error_message is None

    def test_custom_fields(self):
        r = AuthResult(
            provider="aws", status=AuthStatus.EXPIRED,
            user_info={"account": "123"}, expires_at="2025-01-01",
            scopes=["read", "write"], error_message="Token expired",
        )
        assert r.provider == "aws"
        assert r.status == AuthStatus.EXPIRED
        assert r.expires_at == "2025-01-01"
        assert len(r.scopes) == 2


class TestAuthCLIError:
    def test_basic(self):
        e = AuthCLIError(1, "command_not_found", {"command": "gcloud"})
        assert e.code == 1
        assert e.message == "command_not_found"
        assert e.details["command"] == "gcloud"

    def test_default_details(self):
        e = AuthCLIError(2, "some_error")
        assert e.details == {}
