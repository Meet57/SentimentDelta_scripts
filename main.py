# Stock Data Pipeline - Main Orchestration
# Downloads stock data, cleans it, and stores each ticker in its own MongoDB collection

import warnings
warnings.filterwarnings('ignore')

from tqdm import tqdm

# Import custom modules
from config import TICKERS, START_DATE, END_DATE, MONGODB_URI, DATABASE_NAME, BATCH_SIZE, CONNECTION_OPTIONS
from database import connect_to_mongodb, get_database, clear_and_insert_data, create_indexes, close_connection
from data_processor import download_and_clean_ticker

def main():
    """Main pipeline execution"""
    
    print(f"Starting pipeline: {len(TICKERS)} tickers ({START_DATE} to {END_DATE})\n")
    
    client, _, mongodb_connected = connect_to_mongodb(MONGODB_URI, CONNECTION_OPTIONS)
    
    if not mongodb_connected:
        print("Cannot proceed without MongoDB connection")
        return
    
    db = get_database(client, DATABASE_NAME)
    
    print("Downloading and processing tickers...\n")
    
    results = {
        'successful': [],
        'failed': [],
        'details': {}
    }
    
    for ticker in tqdm(TICKERS, desc="Processing"):
        stock_data = download_and_clean_ticker(ticker, START_DATE, END_DATE)
        
        if stock_data is None or stock_data.empty:
            results['failed'].append(ticker)
            continue
        
        collection = db[ticker]
        
        try:
            total_inserted = clear_and_insert_data(collection, stock_data, BATCH_SIZE)
            create_indexes(collection, ticker)
            doc_count = collection.count_documents({})
            
            results['successful'].append(ticker)
            results['details'][ticker] = {
                'documents': doc_count,
                'date_range': f"{stock_data['Date'].min()} to {stock_data['Date'].max()}",
                'columns': list(stock_data.columns)
            }
            
        except Exception as e:
            print(f"\n✗ Error with {ticker}: {e}")
            results['failed'].append(ticker)
    
    print()
    
    close_connection(client)

if __name__ == "__main__":
    main()