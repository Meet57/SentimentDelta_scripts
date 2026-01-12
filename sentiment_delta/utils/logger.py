"""
Reusable logging utility for the SentimentDelta project.
Provides consistent logging across all modules.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str, 
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Setup and return a configured logger.
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for logging to file
        format_string: Custom format string for log messages
    
    Returns:
        Configured logger instance
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level.upper()))
    
    formatter = logging.Formatter(format_string)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with default configuration.
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        Logger instance
    """
    return setup_logger(name)


def log_operation_start(logger: logging.Logger, operation: str, **kwargs) -> None:
    """
    Log the start of an operation with optional parameters.
    
    Args:
        logger: Logger instance
        operation: Description of the operation
        **kwargs: Additional parameters to log
    """
    params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"Starting {operation}" + (f" with {params}" if params else ""))


def log_operation_end(logger: logging.Logger, operation: str, **kwargs) -> None:
    """
    Log the end of an operation with optional results.
    
    Args:
        logger: Logger instance
        operation: Description of the operation
        **kwargs: Additional results to log
    """
    params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"Completed {operation}" + (f" - {params}" if params else ""))


def log_error(logger: logging.Logger, operation: str, error: Exception) -> None:
    """
    Log an error with context.
    
    Args:
        logger: Logger instance
        operation: Description of what was being attempted
        error: The exception that occurred
    """
    logger.error(f"Error during {operation}: {str(error)}")