"""
Stock data processing utilities for the SentimentDelta project.
Handles downloading, cleaning, and processing of stock data.
"""

from typing import Optional, List, Dict, Any
import yfinance as yf
import pandas as pd
import numpy as np

from ..utils.logger import get_logger, log_operation_start, log_operation_end, log_error

logger = get_logger(__name__)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names for consistency.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with standardized columns
    """
    # Flatten multi-level columns if they exist
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip('_') for col in df.columns.values]
    
    # Handle different possible date column names
    date_columns = ['Date', 'Datetime', 'date', 'datetime']
    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break
    
    if date_col and date_col != 'Date':
        df = df.rename(columns={date_col: 'Date'})
    
    # Ensure standard column names
    expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df.columns = [col if col in expected_cols else col.title() for col in df.columns]
    
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate stock data.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    # Sort by date
    df = df.sort_values('Date')
    
    # Handle missing values - forward fill then backward fill
    df = df.ffill().bfill()
    
    # Remove any remaining NaN rows
    df = df.dropna()
    
    # Replace inf values with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Ensure Date is datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    return df


def validate_data(df: pd.DataFrame, ticker: str) -> bool:
    """
    Validate that DataFrame contains required data.
    
    Args:
        df: DataFrame to validate
        ticker: Stock ticker symbol
    
    Returns:
        True if data is valid, False otherwise
    """
    if df is None or df.empty:
        logger.warning(f"No data available for {ticker}")
        return False
    
    if 'Date' not in df.columns:
        logger.error(f"Missing Date column for {ticker}. Available columns: {list(df.columns)}")
        return False
    
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Missing columns for {ticker}: {missing_cols}")
    
    return True


def download_ticker_data(ticker: str, period: str = "1mo", interval: str = "1h") -> Optional[pd.DataFrame]:
    """
    Download stock data for a ticker using yfinance.
    
    Args:
        ticker: Stock ticker symbol
        period: Time period (e.g., "1mo", "3mo", "1y")
        interval: Data interval (e.g., "1h", "1d")
    
    Returns:
        DataFrame with stock data or None if failed
    """
    try:
        log_operation_start(logger, f"downloading data for {ticker}", period=period, interval=interval)
        
        stock_df = yf.download(ticker, period=period, interval=interval)
        
        if stock_df.empty:
            logger.warning(f"No data downloaded for {ticker}")
            return None
        
        log_operation_end(logger, f"downloading data for {ticker}", rows=len(stock_df))
        return stock_df
        
    except Exception as e:
        log_error(logger, f"downloading data for {ticker}", e)
        return None


def process_ticker_data(ticker: str, period: str = "1mo", interval: str = "1h") -> Optional[pd.DataFrame]:
    """
    Download and process stock data for a single ticker.
    
    Args:
        ticker: Stock ticker symbol
        period: Time period for data download
        interval: Data interval
    
    Returns:
        Processed DataFrame or None if failed
    """
    log_operation_start(logger, f"processing {ticker}")
    
    # Download data
    stock_df = download_ticker_data(ticker, period, interval)
    if stock_df is None:
        return None
    
    # Standardize columns
    stock_df = standardize_column_names(stock_df)
    
    # Validate data structure
    if not validate_data(stock_df, ticker):
        return None
    
    # Reset index to get Date as column
    stock_df.reset_index(inplace=True)
    
    # Clean data
    stock_df = clean_data(stock_df)
    
    # Add ticker column
    stock_df['Ticker'] = ticker
    
    log_operation_end(logger, f"processing {ticker}", rows=len(stock_df))
    return stock_df


def process_multiple_tickers(tickers: List[str], period: str = "1mo", interval: str = "1h") -> Dict[str, Optional[pd.DataFrame]]:
    """
    Process multiple tickers and return results.
    
    Args:
        tickers: List of stock ticker symbols
        period: Time period for data download
        interval: Data interval
    
    Returns:
        Dictionary mapping ticker to processed DataFrame (or None if failed)
    """
    log_operation_start(logger, "processing multiple tickers", count=len(tickers))
    
    results = {}
    successful = []
    failed = []
    
    for ticker in tickers:
        try:
            data = process_ticker_data(ticker, period, interval)
            results[ticker] = data
            
            if data is not None:
                successful.append(ticker)
            else:
                failed.append(ticker)
                
        except Exception as e:
            log_error(logger, f"processing ticker {ticker}", e)
            results[ticker] = None
            failed.append(ticker)
    
    log_operation_end(logger, "processing multiple tickers", 
                     successful=len(successful), failed=len(failed))
    
    if failed:
        logger.warning(f"Failed to process tickers: {failed}")
    
    return results


def get_data_summary(df: pd.DataFrame, ticker: str) -> Dict[str, Any]:
    """
    Get summary information about the processed data.
    
    Args:
        df: Processed DataFrame
        ticker: Stock ticker symbol
    
    Returns:
        Dictionary with summary information
    """
    if df is None or df.empty:
        return {"ticker": ticker, "status": "failed", "rows": 0}
    
    return {
        "ticker": ticker,
        "status": "success",
        "rows": len(df),
        "date_range": f"{df['Date'].min()} to {df['Date'].max()}",
        "columns": list(df.columns),
        "first_date": df['Date'].min(),
        "last_date": df['Date'].max()
    }