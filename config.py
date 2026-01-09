# Configuration Parameters for Stock Data Pipeline

# Stock tickers to download
TICKERS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "NFLX"]

# Date range
START_DATE = "2023-12-30"
END_DATE = "2024-12-30"

# MongoDB Atlas Configuration
# Replace with your actual connection string
# Note: For macOS SSL issues, tlsInsecure=true is used for development
MONGODB_URI = "mongodb+srv://meetvalorent:meetvalorent@cluster.x0neiha.mongodb.net/?tlsInsecure=true&appName=Cluster"
DATABASE_NAME = "stock_market_db"

# Connection options
CONNECTION_OPTIONS = {
    "serverSelectionTimeoutMS": 5000,
    "retryWrites": True,
    "w": "majority"
}

# Batch size for inserting documents
BATCH_SIZE = 1000

# Check logging functions
# Move to env