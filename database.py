# MongoDB Connection and Database Operations

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def connect_to_mongodb(mongodb_uri, connection_options=None):
    """
    Connect to MongoDB Atlas.
    
    Args:
        mongodb_uri: MongoDB connection string
        serverSelectionTimeoutMS: Timeout in milliseconds
    
    Returns:
        Tuple of (client, db, success_flag)
    """
    try:
        # Default connection options
        if connection_options is None:
            connection_options = {
                "serverSelectionTimeoutMS": 5000,
            }
        
        # Connect to MongoDB
        client = MongoClient(mongodb_uri, **connection_options)
        
        # Test connection
        client.admin.command('ping')
        
        print("Connected to MongoDB")
        return client, None, True
        
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None, None, False
    except Exception as e:
        print(f"Connection error: {e}")
        return None, None, False

def get_database(client, database_name):
    """
    Get database from connected client.
    
    Args:
        client: MongoDB client
        database_name: Name of database to access
    
    Returns:
        Database object
    """
    return client[database_name]

def clear_and_insert_data(collection, stock_data, batch_size):
    """
    Clear existing data and insert new stock data.
    
    Args:
        collection: MongoDB collection
        stock_data: DataFrame with stock data
        batch_size: Size of batches for insertion
    
    Returns:
        Number of inserted documents
    """
    import numpy as np
    
    # Clear and insert
    collection.delete_many({})
    records = stock_data.replace({np.nan: None}).to_dict('records')
    
    total_inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        result = collection.insert_many(batch, ordered=False)
        total_inserted += len(result.inserted_ids)
    
    return total_inserted

def create_indexes(collection, ticker):
    """
    Create indexes on collection for better query performance.
    
    Args:
        collection: MongoDB collection
        ticker: Stock ticker symbol
    """
    collection.create_index([("Date", -1)], name="date_idx")
    collection.create_index([("Close", 1)], name="close_idx")

def close_connection(client):
    """
    Close MongoDB connection.
    
    Args:
        client: MongoDB client
    """
    if client:
        client.close()
