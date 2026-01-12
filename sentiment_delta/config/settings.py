"""
Configuration settings for the SentimentDelta project.
Centralized configuration management with environment support.
"""

import os
from typing import List, Dict, Any, Optional
from pathlib import Path

# Default configuration values
DEFAULT_CONFIG = {
    # Stock tickers to process
    "TICKERS": ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "NFLX"],
    
    # Data processing settings
    "DATA_PERIOD": "1mo",
    "DATA_INTERVAL": "1h",
    "START_DATE": "2023-12-30",
    "END_DATE": "2024-12-30",
    
    # Database settings
    "DATABASE_NAME": "stock_market_db",
    "BATCH_SIZE": 1000,
    
    # MongoDB connection settings
    "CONNECTION_OPTIONS": {
        "serverSelectionTimeoutMS": 5000,
        "retryWrites": True,
        "w": "majority"
    },
    
    # Scraping settings
    "SCRAPING_MAX_PAGES": 10,
    "SCRAPING_DELAY": 2,
    "SCRAPING_TIMEOUT": 30,
    
    # Embedding model
    "EMBEDDING_MODEL": "all-MiniLM-L6-v2",
    
    # Logging settings
    "LOG_LEVEL": "INFO",
    "LOG_FILE": None,  # Set to file path to enable file logging
}


class Config:
    """Configuration management class."""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration.
        
        Args:
            config_dict: Optional configuration dictionary to override defaults
        """
        self._config = DEFAULT_CONFIG.copy()
        
        if config_dict:
            self._config.update(config_dict)
        
        # Load from environment variables
        self._load_from_env()
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        # MongoDB URI (required)
        mongodb_uri = os.getenv("MONGODB_URI")
        if mongodb_uri:
            self._config["MONGODB_URI"] = mongodb_uri
        elif "MONGODB_URI" not in self._config:
            # Set default for development (with warning)
            self._config["MONGODB_URI"] = "mongodb+srv://meetvalorent:meetvalorent@cluster.x0neiha.mongodb.net/?tlsInsecure=true&appName=Cluster"
        
        # Other optional environment variables
        env_mappings = {
            "DATABASE_NAME": "DATABASE_NAME",
            "LOG_LEVEL": "LOG_LEVEL",
            "BATCH_SIZE": ("BATCH_SIZE", int),
            "SCRAPING_MAX_PAGES": ("SCRAPING_MAX_PAGES", int),
            "SCRAPING_DELAY": ("SCRAPING_DELAY", int),
            "DATA_PERIOD": "DATA_PERIOD",
            "DATA_INTERVAL": "DATA_INTERVAL",
        }
        
        for config_key, env_info in env_mappings.items():
            if isinstance(env_info, tuple):
                env_key, type_converter = env_info
            else:
                env_key, type_converter = env_info, str
            
            env_value = os.getenv(env_key)
            if env_value:
                try:
                    self._config[config_key] = type_converter(env_value)
                except ValueError:
                    pass  # Keep default value if conversion fails
        
        # Handle tickers from environment (comma-separated)
        env_tickers = os.getenv("TICKERS")
        if env_tickers:
            self._config["TICKERS"] = [ticker.strip().upper() for ticker in env_tickers.split(",")]
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        self._config[key] = value
    
    def update(self, config_dict: Dict[str, Any]) -> None:
        """Update configuration with a dictionary."""
        self._config.update(config_dict)
    
    @property
    def tickers(self) -> List[str]:
        """Get list of stock tickers."""
        return self.get("TICKERS", [])
    
    @property
    def mongodb_uri(self) -> str:
        """Get MongoDB connection URI."""
        return self.get("MONGODB_URI")
    
    @property
    def database_name(self) -> str:
        """Get database name."""
        return self.get("DATABASE_NAME")
    
    @property
    def connection_options(self) -> Dict[str, Any]:
        """Get MongoDB connection options."""
        return self.get("CONNECTION_OPTIONS", {})
    
    @property
    def batch_size(self) -> int:
        """Get batch size for database operations."""
        return self.get("BATCH_SIZE", 1000)
    
    @property
    def data_period(self) -> str:
        """Get data period for stock downloads."""
        return self.get("DATA_PERIOD", "1mo")
    
    @property
    def data_interval(self) -> str:
        """Get data interval for stock downloads."""
        return self.get("DATA_INTERVAL", "1h")
    
    @property
    def scraping_max_pages(self) -> int:
        """Get maximum pages for scraping."""
        return self.get("SCRAPING_MAX_PAGES", 10)
    
    @property
    def scraping_delay(self) -> int:
        """Get delay between scraping requests."""
        return self.get("SCRAPING_DELAY", 2)
    
    @property
    def log_level(self) -> str:
        """Get logging level."""
        return self.get("LOG_LEVEL", "INFO")
    
    @property
    def log_file(self) -> Optional[str]:
        """Get log file path."""
        return self.get("LOG_FILE")
    
    @property
    def embedding_model(self) -> str:
        """Get embedding model name."""
        return self.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    
    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary."""
        return self._config.copy()
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return list of validation errors.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        if not self.mongodb_uri:
            errors.append("MONGODB_URI is required")
        
        if not self.tickers:
            errors.append("At least one ticker must be specified")
        
        if self.batch_size <= 0:
            errors.append("BATCH_SIZE must be positive")
        
        if self.scraping_max_pages <= 0:
            errors.append("SCRAPING_MAX_PAGES must be positive")
        
        return errors


# Global configuration instance
_global_config = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        Configuration instance
    """
    global _global_config
    if _global_config is None:
        _global_config = Config()
    return _global_config


def set_config(config: Config) -> None:
    """Set the global configuration instance."""
    global _global_config
    _global_config = config


def create_config(config_dict: Optional[Dict[str, Any]] = None) -> Config:
    """
    Create a new configuration instance.
    
    Args:
        config_dict: Optional configuration dictionary
    
    Returns:
        New configuration instance
    """
    return Config(config_dict)


# Convenience functions for accessing common config values
def get_tickers() -> List[str]:
    """Get configured tickers."""
    return get_config().tickers


def get_mongodb_uri() -> str:
    """Get MongoDB URI."""
    return get_config().mongodb_uri


def get_database_name() -> str:
    """Get database name."""
    return get_config().database_name