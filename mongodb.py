# Simple MongoDB CRUD Operations with Embeddings

from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from bson import ObjectId
from config import MONGODB_URI, DATABASE_NAME

# Initialize
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]
model = SentenceTransformer('all-MiniLM-L6-v2')

def create_embedding(text):
    """
    Create embedding for a sentence.
    
    Args:
        text: Input text/sentence
        
    Returns:
        List of embeddings
    """
    embedding = model.encode(text)
    return embedding.tolist()

def create_document(collection_name, data):
    """
    Create a new document.
    
    Args:
        collection_name: Name of collection
        text: Text content for the document
        **kwargs: Additional document data
        
    Returns:
        Document ID as string
    """
    result = db[collection_name].insert_one(data)
    return str(result.inserted_id)

def read_document(collection_name, doc_id):
    """
    Read a document by ID.
    
    Args:
        collection_name: Name of collection
        doc_id: Document ID
        
    Returns:
        Document dict or None
    """
    doc = db[collection_name].find_one({"_id": ObjectId(doc_id)})
    if doc:
        doc['_id'] = str(doc['_id'])
    return doc

def read_documents(collection_name, query=None, limit=None):
    """
    Read multiple documents.
    
    Args:
        collection_name: Name of collection
        query: Search filter (optional)
        limit: Max results (optional)
        
    Returns:
        List of documents
    """
    cursor = db[collection_name].find(query or {})
    if limit:
        cursor = cursor.limit(limit)
    
    docs = []
    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        docs.append(doc)
    return docs

def update_document(collection_name, doc_id, update_data):
    """
    Update a document.
    
    Args:
        collection_name: Name of collection
        doc_id: Document ID
        update_data: Data to update (dict)
        
    Returns:
        True if updated, False if not found
    """
    result = db[collection_name].update_one(
        {"_id": ObjectId(doc_id)}, 
        {"$set": update_data}
    )
    return result.modified_count > 0

def delete_document(collection_name, doc_id):
    """
    Delete a document.
    
    Args:
        collection_name: Name of collection
        doc_id: Document ID
        
    Returns:
        True if deleted, False if not found
    """
    result = db[collection_name].delete_one({"_id": ObjectId(doc_id)})
    return result.deleted_count > 0

def vector_search(collection_name, query_text, index_name="sentiment_data_vector", limit=5, num_candidates=100):
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
    # Create embedding for the query text
    query_embedding = create_embedding(query_text)
    
    # MongoDB Atlas Vector Search aggregation pipeline
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
        results = list(db[collection_name].aggregate(pipeline))
        # Convert ObjectId to string for each result
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        return results
        
    except Exception as e:
        print(f"Vector search error: {e}")
        # Fallback to regular text search
        fallback_results = read_documents(
            collection_name, 
            {"text": {"$regex": query_text, "$options": "i"}},
            limit=limit
        )
        return fallback_results