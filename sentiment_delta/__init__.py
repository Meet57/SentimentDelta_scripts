"""SentimentDelta - A modular stock market sentiment analysis toolkit.

This package provides clean, reusable tools for:
- Stock data downloading and processing
- Financial news scraping and sentiment analysis  
- MongoDB integration with vector search capabilities
- Configurable logging and error handling

Example usage:
    from sentiment_delta.config.settings import get_config
    from sentiment_delta.data.processor import process_ticker_data
    from sentiment_delta.utils.database import create_mongodb_manager
    
    config = get_config()
    db_manager = create_mongodb_manager(config.mongodb_uri, config.database_name)
    data = process_ticker_data('AAPL')
"""

__version__ = "1.0.0"
__author__ = "SentimentDelta Team"
__email__ = "contact@sentimentdelta.com"

# Make key classes and functions easily accessible
from .config.settings import get_config, Config
from .utils.logger import setup_logger, get_logger
from .utils.database import MongoDBManager, create_mongodb_manager

__all__ = [
    'get_config',
    'Config', 
    'setup_logger',
    'get_logger',
    'MongoDBManager',
    'create_mongodb_manager'
]