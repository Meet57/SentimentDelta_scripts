"""
Unified MongoDB operations for the SentimentDelta project.
Consolidates connection management, CRUD operations, and vector search.
"""

from typing import Optional, Dict, List, Any, Tuple, Union
import numpy as np
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from sentence_transformers import SentenceTransformer

from .logger import get_logger

logger = get_logger(__name__)


class MongoDBManager:
    """Manages MongoDB connections and operations."""
    
    def __init__(self, mongodb_uri: str, database_name: str, connection_options: Optional[Dict] = None):
        """
        Initialize MongoDB manager.
        
        Args:
            mongodb_uri: MongoDB connection string
            database_name: Name of the database
            connection_options: Additional connection options
        """
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.connection_options = connection_options or {
            "serverSelectionTimeoutMS": 5000,
            "retryWrites": True,
            "w": "majority"
        }
        self.client = None
        self.db = None
        self.embedding_model = None
    
    def connect(self) -> bool:
        """
        Connect to MongoDB Atlas.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.client = MongoClient(self.mongodb_uri, **self.connection_options)
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info("Connected to MongoDB successfully")
            return True
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def get_collection(self, collection_name: str):
        """Get a collection from the database."""
        if not self.db:
            raise ValueError("Not connected to database")
        return self.db[collection_name]
    
    def clear_and_insert_bulk(self, collection_name: str, data: List[Dict], batch_size: int = 1000) -> int:
        """
        Clear collection and insert data in batches.
        
        Args:
            collection_name: Name of the collection
            data: List of documents to insert
            batch_size: Size of batches for insertion
        
        Returns:
            Number of inserted documents
        """
        collection = self.get_collection(collection_name)
        
        # Clear existing data
        collection.delete_many({})
        logger.info(f"Cleared collection '{collection_name}'")
        
        # Replace NaN values with None
        clean_data = []
        for doc in data:
            clean_doc = {}
            for key, value in doc.items():
                if isinstance(value, (np.floating, float)) and np.isnan(value):
                    clean_doc[key] = None
                else:
                    clean_doc[key] = value
            clean_data.append(clean_doc)
        
        # Insert in batches
        total_inserted = 0
        for i in range(0, len(clean_data), batch_size):
            batch = clean_data[i:i + batch_size]
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        
        logger.info(f"Inserted {total_inserted} documents into '{collection_name}'")
        return total_inserted
    
    def create_indexes(self, collection_name: str, indexes: List[Tuple[str, int]]) -> None:
        """
        Create indexes on collection for better query performance.
        
        Args:
            collection_name: Name of the collection
            indexes: List of (field_name, direction) tuples
        """
        collection = self.get_collection(collection_name)
        for field, direction in indexes:
            index_name = f"{field}_idx"
            collection.create_index([(field, direction)], name=index_name)
            logger.info(f"Created index '{index_name}' on '{collection_name}'")
    
    def init_embedding_model(self, model_name: str = 'all-MiniLM-L6-v2') -> None:
        """Initialize the embedding model for vector operations."""
        if not self.embedding_model:
            self.embedding_model = SentenceTransformer(model_name)
            logger.info(f"Initialized embedding model: {model_name}")
    
    def create_embedding(self, text: str) -> List[float]:
        """
        Create embedding for text.
        
        Args:
            text: Input text
        
        Returns:
            List of embeddings
        """
        if not self.embedding_model:
            self.init_embedding_model()
        
        embedding = self.embedding_model.encode(text)
        return embedding.tolist()
    
    def create_document(self, collection_name: str, data: Dict[str, Any]) -> str:
        """
        Create a new document.
        
        Args:
            collection_name: Name of collection
            data: Document data
        
        Returns:
            Document ID as string
        """
        collection = self.get_collection(collection_name)
        result = collection.insert_one(data)
        return str(result.inserted_id)
    
    def read_document(self, collection_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Read a document by ID.
        
        Args:
            collection_name: Name of collection
            doc_id: Document ID
        
        Returns:
            Document dict or None
        """
        collection = self.get_collection(collection_name)
        doc = collection.find_one({"_id": ObjectId(doc_id)})
        if doc:
            doc['_id'] = str(doc['_id'])
        return doc
    
    def read_documents(self, collection_name: str, query: Optional[Dict] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Read multiple documents.
        
        Args:
            collection_name: Name of collection
            query: Search filter (optional)
            limit: Max results (optional)
        
        Returns:
            List of documents
        """
        collection = self.get_collection(collection_name)
        cursor = collection.find(query or {})
        if limit:
            cursor = cursor.limit(limit)
        
        docs = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            docs.append(doc)
        return docs
    
    def update_document(self, collection_name: str, doc_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update a document.
        
        Args:
            collection_name: Name of collection
            doc_id: Document ID
            update_data: Data to update
        
        Returns:
            True if updated, False if not found
        """
        collection = self.get_collection(collection_name)
        result = collection.update_one(
            {"_id": ObjectId(doc_id)}, 
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    def delete_document(self, collection_name: str, doc_id: str) -> bool:
        """
        Delete a document.
        
        Args:
            collection_name: Name of collection
            doc_id: Document ID
        
        Returns:
            True if deleted, False if not found
        """
        collection = self.get_collection(collection_name)
        result = collection.delete_one({"_id": ObjectId(doc_id)})
        return result.deleted_count > 0
    
    def vector_search(self, collection_name: str, query_text: str, 
                     index_name: str = "sentiment_data_vector", 
                     limit: int = 5, num_candidates: int = 100) -> List[Dict[str, Any]]:
        """
        Perform vector search using MongoDB Atlas Vector Search.
        
        Args:
            collection_name: Name of collection
            query_text: Text to search for
            index_name: Name of the vector search index
            limit: Number of results to return
            num_candidates: Number of candidates for vector search
        
        Returns:
            List of matching documents
        """
        if not self.embedding_model:
            self.init_embedding_model()
        
        query_embedding = self.create_embedding(query_text)
        collection = self.get_collection(collection_name)
        
        pipeline = [
            {
                "$vectorSearch": {
                    "index": index_name,
                    "path": "embedding", 
                    "queryVector": query_embedding,
                    "numCandidates": num_candidates,
                    "limit": limit
                }
            }
        ]
        
        try:
            results = list(collection.aggregate(pipeline))
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            return results
        except Exception as e:
            logger.warning(f"Vector search failed, using text search: {e}")
            # Fallback to regular text search
            return self.read_documents(
                collection_name, 
                {"text": {"$regex": query_text, "$options": "i"}},
                limit=limit
            )
    
    def count_documents(self, collection_name: str, query: Optional[Dict] = None) -> int:
        """Count documents in collection."""
        collection = self.get_collection(collection_name)
        return collection.count_documents(query or {})


def create_mongodb_manager(mongodb_uri: str, database_name: str, 
                          connection_options: Optional[Dict] = None) -> MongoDBManager:
    """
    Factory function to create and connect a MongoDB manager.
    
    Args:
        mongodb_uri: MongoDB connection string
        database_name: Name of the database
        connection_options: Additional connection options
    
    Returns:
        Connected MongoDBManager instance
    
    Raises:
        ConnectionError: If connection fails
    """
    manager = MongoDBManager(mongodb_uri, database_name, connection_options)
    if not manager.connect():
        raise ConnectionError("Failed to connect to MongoDB")
    return manager