# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for batch.logging_config — structured JSON logging setup."""

import json
import logging
import re
from io import StringIO

import pytest

from batch.logging_config import configure_logging

# ---------------------------------------------------------------------------
# Fixture: restore root logger after each test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _restore_root_logger():
    """Save and restore the root logger's handlers and level after each test."""
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    yield
    root.handlers.clear()
    for h in original_handlers:
        root.addHandler(h)
    root.setLevel(original_level)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_logger(name: str = "test.logger") -> tuple[logging.Logger, StringIO]:
    """Return a logger whose output is captured in a StringIO buffer.

    configure_logging() must have been called first so the root logger is set up.
    The captured logger inherits handlers from root via propagation.
    """
    buf = StringIO()
    # Replace the stdout handler's stream temporarily
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.stream = buf
            break
    return logging.getLogger(name), buf


# ---------------------------------------------------------------------------
# JSON format (default)
# ---------------------------------------------------------------------------


class TestJsonFormat:
    def setup_method(self):
        configure_logging(service="test-svc")

    def _emit(self, message: str, level: str = "info", **extra) -> dict:
        log, buf = _capture_logger()
        getattr(log, level)(message, extra=extra if extra else None)
        line = buf.getvalue().strip()
        return json.loads(line)

    def test_output_is_valid_json(self):
        log, buf = _capture_logger()
        log.info("hello world")
        line = buf.getvalue().strip()
        parsed = json.loads(line)
        assert isinstance(parsed, dict)

    def test_required_fields_present(self):
        record = self._emit("test message")
        assert "timestamp" in record
        assert "level" in record
        assert "logger" in record
        assert "message" in record
        assert "service" in record

    def test_message_field(self):
        record = self._emit("my message")
        assert record["message"] == "my message"

    def test_level_field_uppercased(self):
        record = self._emit("msg", level="info")
        assert record["level"] == "INFO"

        log, buf = _capture_logger("warn.logger")
        log.warning("w")
        record = json.loads(buf.getvalue().strip())
        assert record["level"] == "WARNING"

    def test_logger_field_is_module_name(self):
        log, buf = _capture_logger("myapp.module")
        log.info("msg")
        record = json.loads(buf.getvalue().strip())
        assert record["logger"] == "myapp.module"

    def test_service_field(self):
        record = self._emit("msg")
        assert record["service"] == "test-svc"

    def test_timestamp_is_iso8601_utc(self):
        record = self._emit("msg")
        ts = record["timestamp"]
        # e.g. 2026-03-30T10:00:00.123Z
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", ts), ts

    def test_extra_fields_included(self):
        record = self._emit("msg", model="mymodel", job_id="abc123", duration_ms=42)
        assert record["model"] == "mymodel"
        assert record["job_id"] == "abc123"
        assert record["duration_ms"] == 42

    def test_one_record_per_line(self):
        log, buf = _capture_logger()
        log.info("first")
        log.info("second")
        lines = [ln for ln in buf.getvalue().strip().splitlines() if ln]
        assert len(lines) == 2
        for line in lines:
            json.loads(line)  # each line must be valid JSON

    def test_no_levelname_field(self):
        """levelname should be renamed to level, not appear twice."""
        record = self._emit("msg")
        assert "levelname" in record or "level" in record
        assert "levelname" not in record

    def test_no_asctime_field(self):
        """asctime should be renamed to timestamp."""
        record = self._emit("msg")
        assert "asctime" not in record

    def test_no_name_field(self):
        """name should be renamed to logger."""
        record = self._emit("msg")
        assert "name" not in record


# ---------------------------------------------------------------------------
# Text format (LOG_FORMAT=text)
# ---------------------------------------------------------------------------


class TestTextFormat:
    def setup_method(self):
        configure_logging(service="test-svc")

    def test_text_format_is_not_json(self, monkeypatch):
        monkeypatch.setenv("LOG_FORMAT", "text")
        configure_logging(service="test-svc")

        log, buf = _capture_logger()
        log.info("hello text")
        line = buf.getvalue().strip()

        with pytest.raises((json.JSONDecodeError, ValueError)):
            json.loads(line)

    def test_text_format_contains_message(self, monkeypatch):
        monkeypatch.setenv("LOG_FORMAT", "text")
        configure_logging(service="test-svc")

        log, buf = _capture_logger()
        log.info("my text message")
        assert "my text message" in buf.getvalue()


# ---------------------------------------------------------------------------
# Log level
# ---------------------------------------------------------------------------


class TestLogLevel:
    def test_default_level_is_info(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        configure_logging()
        assert logging.getLogger().level == logging.INFO

    def test_log_level_env_var(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        configure_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_log_level_argument_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        configure_logging(level="WARNING")
        assert logging.getLogger().level == logging.WARNING

    def test_debug_messages_suppressed_at_info_level(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        configure_logging()
        log, buf = _capture_logger()
        log.debug("should not appear")
        assert buf.getvalue() == ""

    def test_debug_messages_emitted_at_debug_level(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        configure_logging()
        log, buf = _capture_logger()
        log.debug("should appear")
        assert "should appear" in buf.getvalue()


# ---------------------------------------------------------------------------
# File handler (VZ_LOG_DIR)
# ---------------------------------------------------------------------------


class TestFileHandler:
    def test_no_file_handler_by_default(self, monkeypatch):
        monkeypatch.delenv("VZ_LOG_DIR", raising=False)
        configure_logging()
        handlers = logging.getLogger().handlers
        file_handlers = [h for h in handlers if isinstance(h, logging.handlers.BaseRotatingHandler)]
        assert file_handlers == []

    def test_file_handler_created_when_log_dir_set(self, monkeypatch, tmp_path):
        monkeypatch.setenv("VZ_LOG_DIR", str(tmp_path))
        configure_logging(service="my-svc")
        handlers = logging.getLogger().handlers
        file_handlers = [h for h in handlers if isinstance(h, logging.handlers.BaseRotatingHandler)]
        assert len(file_handlers) == 1
        assert "my-svc.log" in str(file_handlers[0].baseFilename)

    def test_file_handler_creates_log_dir(self, monkeypatch, tmp_path):
        log_dir = tmp_path / "nested" / "logs"
        monkeypatch.setenv("VZ_LOG_DIR", str(log_dir))
        configure_logging()
        assert log_dir.exists()

    def test_file_receives_same_json(self, monkeypatch, tmp_path):
        monkeypatch.setenv("VZ_LOG_DIR", str(tmp_path))
        configure_logging(service="test-svc")

        logging.getLogger("file.test").info("file message", extra={"key": "val"})

        log_file = tmp_path / "test-svc.log"
        content = log_file.read_text().strip()
        record = json.loads(content)
        assert record["message"] == "file message"
        assert record["key"] == "val"


# ---------------------------------------------------------------------------
# Re-configuration (idempotent)
# ---------------------------------------------------------------------------


def test_reconfigure_clears_previous_handlers():
    configure_logging(service="first")
    configure_logging(service="second")
    # Should not accumulate handlers on repeated calls
    assert len(logging.getLogger().handlers) == 1


# ---------------------------------------------------------------------------
# Import guard: logging.handlers accessible in test namespace
# ---------------------------------------------------------------------------
import logging.handlers  # noqa: E402 (needed for isinstance check above)
