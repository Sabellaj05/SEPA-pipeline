"""Tests for the logger utility."""

import logging

from sepa_pipeline.utils.logger import logger


class TestLogger:
    """Test cases for logger configuration."""

    def test_logger_initialization(self):
        """Test that logger initializes correctly."""
        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_logger_name(self):
        """Test that logger has correct name."""
        assert logger.name == "sepa_pipeline.utils.logger"

    def test_logger_level(self):
        """Test that logger has appropriate level."""
        # Should be at least INFO level
        assert logger.level <= logging.INFO

    def test_logger_handlers(self):
        """Test that logger has handlers configured."""
        assert len(logger.handlers) > 0

    def test_logger_can_log(self, caplog):
        """Test that logger can actually log messages."""
        test_message = "Test log message"
        logger.info(test_message)

        # Check that the message was logged
        assert test_message in caplog.text
