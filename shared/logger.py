"""
Structured logging with JSON output and pretty console output.
Uses structlog for async-safe logging.
"""

import logging
import logging.handlers
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import structlog


def setup_logging(
    name: str,
    level: str = "INFO",
    log_format: str = "console",
    log_dir: str = "logs",
) -> structlog.BoundLogger:
    """
    Set up structured logging with both console and file handlers.

    Args:
        name: Logger name (e.g., __name__).
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_format: Output format ('console' or 'json').
        log_dir: Directory for log files.

    Returns:
        Configured structlog logger.
    """
    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)

    # Configure stdlib logging (for third-party libraries)
    stdlib_level = getattr(logging, level.upper(), logging.INFO)

    # Console handler with pretty output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(stdlib_level)

    # File handler with JSON output
    file_path = Path(log_dir) / f"{name}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        file_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10,
    )
    file_handler.setLevel(logging.DEBUG)

    # Set formatters
    if log_format == "json":
        # JSON formatter for files
        json_formatter = JSONFormatter()
        file_handler.setFormatter(json_formatter)
        console_formatter = PrettyConsoleFormatter()
        console_handler.setFormatter(console_formatter)
    else:
        # Pretty console formatter
        formatter = PrettyConsoleFormatter()
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if log_format == "json" else
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(name)


class JSONFormatter(logging.Formatter):
    """JSON formatter for logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class PrettyConsoleFormatter(logging.Formatter):
    """Pretty formatter for console output."""

    LEVEL_COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        level = record.levelname
        color = self.LEVEL_COLORS.get(level, "")
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]

        return (
            f"{timestamp} {color}[{level:8}]{self.RESET} "
            f"{record.name:30} {record.getMessage()}"
        )


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a structlog logger instance.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Configured structlog logger.
    """
    return structlog.get_logger(name)


def bind_context(**kwargs: Any) -> None:
    """
    Bind context variables that will be included in all subsequent log messages.

    Args:
        **kwargs: Context variables (e.g., user_id="123", request_id="abc").
    """
    logger = structlog.get_logger()
    logger = logger.bind(**kwargs)
