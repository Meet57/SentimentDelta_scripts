#!/usr/bin/env python3
"""
SentimentDelta - Stock Data Pipeline Main Entry Point

A clean, modular pipeline for downloading stock data, processing it,
and storing it in MongoDB for further analysis.
"""

import warnings
from typing import Dict, Any, List
from tqdm import tqdm

# Import refactored modules
from sentiment_delta.config.settings import get_config, Config
from sentiment_delta.utils.logger import setup_logger, log_operation_start, log_operation_end, log_error
from sentiment_delta.utils.database import create_mongodb_manager, MongoDBManager
from sentiment_delta.data.processor import process_multiple_tickers, get_data_summary

warnings.filterwarnings('ignore')


def setup_logging(config: Config):
    """Setup logging based on configuration."""
    return setup_logger(
        __name__, 
        level=config.log_level,
        log_file=config.log_file
    )


def process_and_store_ticker_data(db_manager: MongoDBManager, config: Config) -> Dict[str, Any]:
    """
    Process stock data for all configured tickers and store in MongoDB.
    
    Args:
        db_manager: MongoDB manager instance
        config: Configuration instance
    
    Returns:
        Results summary dictionary
    """
    logger = setup_logging(config)
    
    log_operation_start(logger, "ticker data processing", 
                       ticker_count=len(config.tickers))
    
    # Process all tickers
    ticker_data = process_multiple_tickers(
        config.tickers, 
        period=config.data_period,
        interval=config.data_interval
    )
    
    results = {
        'successful': [],
        'failed': [],
        'details': {}
    }
    
    # Store data for each ticker
    for ticker, data in tqdm(ticker_data.items(), desc="Storing data"):
        if data is None or data.empty:
            results['failed'].append(ticker)
            continue
        
        try:
            # Store in MongoDB collection named after ticker
            records = data.to_dict('records')
            total_inserted = db_manager.clear_and_insert_bulk(
                ticker, records, config.batch_size
            )
            
            # Create indexes for better performance
            indexes = [('Date', -1), ('Close', 1)]
            db_manager.create_indexes(ticker, indexes)
            
            # Get summary
            summary = get_data_summary(data, ticker)
            results['successful'].append(ticker)
            results['details'][ticker] = summary
            
            logger.info(f"Successfully stored {total_inserted} records for {ticker}")
            
        except Exception as e:
            log_error(logger, f"storing data for {ticker}", e)
            results['failed'].append(ticker)
    
    log_operation_end(logger, "ticker data processing",
                     successful=len(results['successful']),
                     failed=len(results['failed']))
    
    return results


def print_pipeline_summary(results: Dict[str, Any], config: Config):
    """Print a summary of the pipeline execution."""
    print(f"\n{'='*60}")
    print("PIPELINE EXECUTION SUMMARY")
    print(f"{'='*60}")
    
    print(f"Tickers processed: {len(config.tickers)}")
    print(f"Successful: {len(results['successful'])}")
    print(f"Failed: {len(results['failed'])}")
    
    if results['successful']:
        print(f"\n✅ Successfully processed tickers:")
        for ticker in results['successful']:
            details = results['details'][ticker]
            print(f"  • {ticker}: {details['rows']} records ({details['date_range']})")
    
    if results['failed']:
        print(f"\n❌ Failed to process tickers:")
        for ticker in results['failed']:
            print(f"  • {ticker}")
    
    print(f"\n{'='*60}")


def main():
    """Main pipeline execution function."""
    # Get configuration
    config = get_config()
    logger = setup_logging(config)
    
    # Validate configuration
    validation_errors = config.validate()
    if validation_errors:
        logger.error("Configuration validation failed:")
        for error in validation_errors:
            logger.error(f"  - {error}")
        return 1
    
    log_operation_start(logger, "SentimentDelta pipeline", 
                       tickers=config.tickers,
                       period=f"{config.data_period} ({config.data_interval})")
    
    # Connect to MongoDB
    try:
        db_manager = create_mongodb_manager(
            config.mongodb_uri,
            config.database_name,
            config.connection_options
        )
    except ConnectionError as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 1
    
    try:
        # Process and store ticker data
        results = process_and_store_ticker_data(db_manager, config)
        
        # Print summary
        print_pipeline_summary(results, config)
        
        log_operation_end(logger, "SentimentDelta pipeline",
                         total_tickers=len(config.tickers),
                         successful=len(results['successful']))
        
        return 0 if not results['failed'] else 1
        
    except Exception as e:
        log_error(logger, "pipeline execution", e)
        return 1
        
    finally:
        # Always disconnect from MongoDB
        db_manager.disconnect()


def run_scraping_pipeline(tickers: List[str] = None, max_pages: int = None):
    """
    Run the news scraping pipeline.
    
    Args:
        tickers: Optional list of tickers to scrape (defaults to config)
        max_pages: Optional max pages per ticker (defaults to config)
    """
    from sentiment_delta.data.scraper import scrape_multiple_tickers, prepare_article_for_storage
    
    config = get_config()
    logger = setup_logging(config)
    
    # Use provided tickers or fall back to config
    tickers = tickers or config.tickers
    max_pages = max_pages or config.scraping_max_pages
    
    log_operation_start(logger, "news scraping pipeline",
                       tickers=tickers, max_pages=max_pages)
    
    # Connect to MongoDB
    try:
        db_manager = create_mongodb_manager(
            config.mongodb_uri,
            config.database_name,
            config.connection_options
        )
        
        # Initialize embedding model
        db_manager.init_embedding_model(config.embedding_model)
        
    except ConnectionError as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 1
    
    try:
        # Scrape articles
        scraped_data = scrape_multiple_tickers(tickers, max_pages)
        
        # Store articles with embeddings
        total_articles = 0
        for ticker, articles in scraped_data.items():
            if articles:
                logger.info(f"Storing {len(articles)} articles for {ticker}")
                
                for article in articles:
                    # Prepare article with embeddings
                    article_with_embedding = prepare_article_for_storage(
                        article, db_manager.create_embedding
                    )
                    
                    # Store in news collection
                    db_manager.create_document('news', article_with_embedding)
                    total_articles += 1
        
        log_operation_end(logger, "news scraping pipeline",
                         total_articles=total_articles)
        
        print(f"\nSuccessfully scraped and stored {total_articles} articles")
        return 0
        
    except Exception as e:
        log_error(logger, "scraping pipeline execution", e)
        return 1
        
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    import sys
    
    # Check for scraping mode
    if len(sys.argv) > 1 and sys.argv[1] == "scrape":
        exit_code = run_scraping_pipeline()
    else:
        exit_code = main()
    
    sys.exit(exit_code)