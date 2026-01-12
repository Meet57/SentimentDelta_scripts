"""Simple stock data processing."""

from typing import List, Dict, Optional
import yfinance as yf
import pandas as pd


def process_ticker_data(ticker, period="1mo", interval="1d", logger=None):
    """Download and process stock data for a ticker."""
    try:
        if logger:
            logger.info(f"Downloading {ticker} data: period={period}, interval={interval}")
        
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        
        if data.empty:
            if logger:
                logger.warning(f"No data returned for {ticker}")
            return None
            
        if logger:
            logger.info(f"Downloaded {len(data)} rows for {ticker}")
        
        # Simple cleanup
        data.reset_index(inplace=True)
        
        # Fix MultiIndex columns if present
        if hasattr(data.columns, 'levels'):
            # Flatten MultiIndex columns by taking the first level
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
        
        # Ensure all column names are strings
        data.columns = [str(col) for col in data.columns]
        
        # Clean data
        data = data.ffill().bfill().dropna()
        data['Ticker'] = ticker
        
        return data
        
    except Exception as e:
        if logger:
            logger.error(f"Error processing {ticker}: {str(e)}")
        return None


def process_multiple_tickers(tickers, period="1mo", interval="1d", logger=None, start_date=None, end_date=None):
    """Process multiple tickers."""
    results = {}
    successful = 0
    failed = 0
    
    for ticker in tickers:
        try:
            if logger:
                logger.info(f"Starting download for {ticker}")
            
            data = process_ticker_data(ticker, period, interval, logger)
            results[ticker] = data
            
            if data is not None:
                successful += 1
                if logger:
                    logger.info(f"Successfully processed {ticker}: {len(data)} records")
            else:
                failed += 1
                if logger:
                    logger.warning(f"Failed to get data for {ticker}")
                
        except Exception as e:
            failed += 1
            if logger:
                logger.error(f"Exception processing {ticker}: {str(e)}")
            results[ticker] = None
    
    if logger:
        logger.info(f"Processing complete: successful={successful}, failed={failed}")
    
    return results