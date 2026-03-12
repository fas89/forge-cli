"""Tests for fluid_build.cli.console — centralised output helpers."""
import sys
import pytest
from io import StringIO
from unittest.mock import patch

from fluid_build.cli.console import (
    cprint, info, success, warning, error, heading, detail, hint,
    RICH_AVAILABLE,
)


class TestCprint:
    def test_plain_text(self, capsys):
        cprint("hello world")
        captured = capsys.readouterr()
        assert "hello" in captured.out

    def test_strips_markup_when_no_rich(self, capsys):
        with patch("fluid_build.cli.console.console", None):
            cprint("[bold]bold text[/bold]")
            captured = capsys.readouterr()
            assert "bold text" in captured.out
            assert "[bold]" not in captured.out

    def test_empty_call(self, capsys):
        cprint()
        # Should not crash


class TestHelpers:
    def test_info(self, capsys):
        info("test message")
        captured = capsys.readouterr()
        assert "test message" in captured.out

    def test_success(self, capsys):
        success("done")
        captured = capsys.readouterr()
        assert "done" in captured.out

    def test_warning(self, capsys):
        warning("be careful")
        captured = capsys.readouterr()
        assert "be careful" in captured.out

    def test_error(self, capsys):
        error("something broke")
        captured = capsys.readouterr()
        assert "something broke" in captured.err

    def test_heading(self, capsys):
        heading("My Section")
        captured = capsys.readouterr()
        assert "My Section" in captured.out

    def test_detail(self, capsys):
        detail("Status", "OK")
        captured = capsys.readouterr()
        assert "Status" in captured.out
        assert "OK" in captured.out

    def test_hint(self, capsys):
        hint("try this")
        captured = capsys.readouterr()
        assert "try this" in captured.out
