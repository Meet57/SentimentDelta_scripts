# Data Processing Functions for Stock Data

import yfinance as yf
import pandas as pd
import numpy as np

def download_and_clean_ticker(ticker, start_date, end_date):
    try:
        # Download data
        stock_df = yf.download(
            ticker,
            period="1mo", 
            interval="1h",
        )
        
        if stock_df.empty:
            return None
        
        # Flatten multi-level columns if they exist
        if isinstance(stock_df.columns, pd.MultiIndex):
            stock_df.columns = ['_'.join(col).strip('_') for col in stock_df.columns.values]
        
        # Reset index to get Date as column
        stock_df.reset_index(inplace=True)
        
        # Handle different possible date column names
        date_columns = ['Date', 'Datetime', 'date', 'datetime']
        date_col = None
        for col in date_columns:
            if col in stock_df.columns:
                date_col = col
                break
        
        if date_col and date_col != 'Date':
            stock_df.rename(columns={date_col: 'Date'}, inplace=True)
        
        # Ensure we have standard column names
        expected_cols = ['Date', 'Open', 'High', 'Low', 'Close','Volume']
        stock_df.columns = [col if col in expected_cols else col.title() for col in stock_df.columns]
        
        # Check if we have the Date column
        if 'Date' not in stock_df.columns:
            print(f"Error processing {ticker}: No date column found. Available columns: {list(stock_df.columns)}")
            return None
        
        # Sort by date
        stock_df = stock_df.sort_values('Date')
        
        # Handle missing values - forward fill then backward fill
        stock_df = stock_df.ffill().bfill()
        
        # Remove any remaining NaN rows
        stock_df = stock_df.dropna()
        
        # Replace inf values with None
        stock_df = stock_df.replace([np.inf, -np.inf], None)
        
        # Ensure Date is datetime
        stock_df['Date'] = pd.to_datetime(stock_df['Date'])
        
        # Add ticker column
        stock_df['Ticker'] = ticker
        
        return stock_df
        
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None
