"""Tests for the logger utility."""

import logging

from sepa_pipeline.utils.logger import get_logger


class TestLogger:
    """Test cases for logger configuration."""

    def test_logger_initialization(self):
        """Test that logger initializes correctly."""
        logger = get_logger("test_logger")
        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_logger_name(self):
        """Test that logger has correct name."""
        logger = get_logger("test_logger")
        assert logger.name == "test_logger"

    def test_logger_level(self):
        """Test that logger has appropriate level."""
        logger = get_logger("test_logger")
        # Should be at least INFO level
        assert logger.level <= logging.INFO

    def test_logger_handlers(self):
        """Test that logger has handlers configured."""
        logger = get_logger("test_logger")
        assert len(logger.handlers) > 0

    def test_logger_can_log(self, caplog):
        """Test that logger can actually log messages."""
        logger = get_logger("test_logger")
        test_message = "Test log message"
        logger.info(test_message)

        # Check that the message was logged
        assert test_message in caplog.text
