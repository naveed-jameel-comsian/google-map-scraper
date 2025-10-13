"""
MongoDB integration for email caching by domain.
"""

import os
import asyncio
from typing import List, Dict, Optional, Any, TYPE_CHECKING
from datetime import datetime, timezone
import logging

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
else:
    try:
        from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    except ImportError:
        AsyncIOMotorClient = None
        AsyncIOMotorDatabase = None
        AsyncIOMotorCollection = None
        ConnectionFailure = Exception
        ServerSelectionTimeoutError = Exception

# MongoDB configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "email_scraper")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "domain_emails")
MONGODB_RECORDS_COLLECTION = os.getenv("MONGODB_RECORDS_COLLECTION", "query_records")
MONGODB_DISABLED = os.getenv("MONGODB_DISABLED", "false").lower() in ("true", "1", "yes")

# Global MongoDB connection
_mongodb_client: Optional[Any] = None
_mongodb_db: Optional[Any] = None
_mongodb_collection: Optional[Any] = None
_mongodb_records_collection: Optional[Any] = None

logger = logging.getLogger(__name__)


async def get_mongodb_client() -> Optional[Any]:
    """Get or create MongoDB client connection."""
    global _mongodb_client
    
    if MONGODB_DISABLED:
        msg = "MongoDB is disabled via MONGODB_DISABLED environment variable"
        print(f"[MongoDB] {msg}")
        logger.info(msg)
        return None
    
    if AsyncIOMotorClient is None:
        msg = "MongoDB driver (motor/pymongo) not available. Install with: pip install motor pymongo"
        print(f"[MongoDB] ERROR: {msg}")
        logger.warning(msg)
        return None
    
    if _mongodb_client is None:
        try:
            print(f"[MongoDB] Attempting to connect to MongoDB at {MONGODB_URI}...")
            logger.info(f"Attempting to connect to MongoDB at {MONGODB_URI}...")
            _mongodb_client = AsyncIOMotorClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=2000,  # 2 second timeout
                connectTimeoutMS=2000,
                socketTimeoutMS=2000
            )
            # Test connection with shorter timeout
            print(f"[MongoDB] Sending ping command to test connection...")
            await asyncio.wait_for(_mongodb_client.admin.command('ping'), timeout=2.0)
            msg = f"[SUCCESS] Successfully connected to MongoDB at {MONGODB_URI}"
            print(f"[MongoDB] {msg}")
            logger.info(msg)
        except asyncio.TimeoutError:
            msg = f"[ERROR] MongoDB connection timeout at {MONGODB_URI} - Is MongoDB running?"
            print(f"[MongoDB] ERROR: {msg}")
            logger.warning(msg)
            _mongodb_client = None
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            msg = f"[ERROR] Failed to connect to MongoDB at {MONGODB_URI}: {e}"
            print(f"[MongoDB] ERROR: {msg}")
            logger.warning(msg)
            logger.warning("Hint: Start MongoDB with 'brew services start mongodb-community' or 'sudo systemctl start mongodb'")
            _mongodb_client = None
        except Exception as e:
            msg = f"[ERROR] Unexpected error connecting to MongoDB: {type(e).__name__}: {e}"
            print(f"[MongoDB] ERROR: {msg}")
            logger.warning(msg)
            _mongodb_client = None
    else:
        print(f"[MongoDB] Reusing existing MongoDB client connection")
    
    return _mongodb_client


async def get_mongodb_collection() -> Optional[Any]:
    """Get MongoDB collection for domain emails."""
    global _mongodb_db, _mongodb_collection
    
    if _mongodb_collection is None:
        client = await get_mongodb_client()
        if client is not None:
            _mongodb_db = client[MONGODB_DATABASE]
            _mongodb_collection = _mongodb_db[MONGODB_COLLECTION]
            
            # Create indexes for better performance
            try:
                await _mongodb_collection.create_index("domain", unique=True)
                await _mongodb_collection.create_index("last_updated")
                await _mongodb_collection.create_index("emails.email")
            except Exception as e:
                logger.warning(f"Failed to create MongoDB indexes: {e}")
    
    return _mongodb_collection


async def close_mongodb_connection():
    """Close MongoDB connection."""
    global _mongodb_client, _mongodb_db, _mongodb_collection, _mongodb_records_collection
    
    if _mongodb_client:
        _mongodb_client.close()
        _mongodb_client = None
        _mongodb_db = None
        _mongodb_collection = None
        _mongodb_records_collection = None


def reset_mongodb_connection():
    """Reset MongoDB connection state (synchronous helper)."""
    global _mongodb_client, _mongodb_db, _mongodb_collection, _mongodb_records_collection
    
    if _mongodb_client:
        _mongodb_client.close()
    
    _mongodb_client = None
    _mongodb_db = None
    _mongodb_collection = None
    _mongodb_records_collection = None
    logger.info("MongoDB connection state reset")


async def get_emails_for_domain(domain: str) -> Optional[List[Dict[str, Any]]]:
    """
    Retrieve cached emails for a domain from MongoDB.
    
    Args:
        domain: The domain name to look up
        
    Returns:
        List of email documents if found, None if not cached
    """
    try:
        collection = await get_mongodb_collection()
        if collection is None:
            return None
        
        # Use timeout for the query
        doc = await asyncio.wait_for(
            collection.find_one({"domain": domain.lower()}), 
            timeout=3.0
        )
        if doc and doc.get("emails"):
            logger.info(f"Found {len(doc['emails'])} cached emails for domain: {domain}")
            return doc["emails"]
        else:
            logger.info(f"No cached emails found for domain: {domain}")
            return None
    except asyncio.TimeoutError:
        logger.warning(f"MongoDB query timeout for domain {domain}")
        return None
    except Exception as e:
        logger.warning(f"Error retrieving emails for domain {domain}: {e}")
        return None


async def store_emails_for_domain(domain: str, emails: List[Dict[str, Any]], source: str = "scraper") -> bool:
    """
    Store emails for a domain in MongoDB.
    
    Args:
        domain: The domain name
        emails: List of email documents with 'email' and 'found_on' fields
        source: Source of the emails (e.g., 'scraper', 'hunter')
        
    Returns:
        True if stored successfully, False otherwise
    """
    try:
        collection = await get_mongodb_collection()
        if collection is None:
            return False
        
        # Prepare email documents with additional metadata
        email_docs = []
        for email_data in emails:
            email_doc = {
                "email": email_data.get("email", "").lower(),
                "found_on": email_data.get("found_on", ""),
                "source": source,
                "cached_at": datetime.now(timezone.utc).isoformat()
            }
            email_docs.append(email_doc)
        
        # Prepare document to store
        doc = {
            "domain": domain.lower(),
            "emails": email_docs,
            "email_count": len(email_docs),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source": source
        }
        
        # Upsert (insert or update) with timeout
        result = await asyncio.wait_for(
            collection.replace_one(
                {"domain": domain.lower()},
                doc,
                upsert=True
            ),
            timeout=5.0
        )
        
        if result.upserted_id or result.modified_count:
            logger.info(f"Stored {len(email_docs)} emails for domain: {domain}")
            return True
        else:
            logger.warning(f"Failed to store emails for domain: {domain}")
            return False
            
    except asyncio.TimeoutError:
        logger.warning(f"MongoDB store timeout for domain {domain}")
        return False
    except Exception as e:
        logger.warning(f"Error storing emails for domain {domain}: {e}")
        return False


async def is_domain_cached(domain: str) -> bool:
    """
    Check if a domain has cached emails in MongoDB.
    
    Args:
        domain: The domain name to check
        
    Returns:
        True if domain has cached emails, False otherwise
    """
    collection = await get_mongodb_collection()
    if collection is None:
        return False
    
    try:
        count = await collection.count_documents({"domain": domain.lower()})
        return count > 0
    except Exception as e:
        logger.error(f"Error checking if domain {domain} is cached: {e}")
        return False


async def get_cached_domains_count() -> int:
    """Get the total number of cached domains."""
    collection = await get_mongodb_collection()
    if not collection:
        return 0
    
    try:
        return await collection.count_documents({})
    except Exception as e:
        logger.error(f"Error getting cached domains count: {e}")
        return 0


async def delete_domain_cache(domain: str) -> bool:
    """
    Delete cached emails for a domain.
    
    Args:
        domain: The domain name to delete from cache
        
    Returns:
        True if deleted successfully, False otherwise
    """
    collection = await get_mongodb_collection()
    if collection is None:
        return False
    
    try:
        result = await collection.delete_one({"domain": domain.lower()})
        if result.deleted_count > 0:
            logger.info(f"Deleted cache for domain: {domain}")
            return True
        else:
            logger.info(f"No cache found for domain: {domain}")
            return False
    except Exception as e:
        logger.error(f"Error deleting cache for domain {domain}: {e}")
        return False


# Query Records Collection Functions

async def get_mongodb_records_collection() -> Optional[Any]:
    """Get MongoDB collection for query records."""
    global _mongodb_db, _mongodb_records_collection
    
    print(f"[MongoDB] Getting records collection (current state: {_mongodb_records_collection is not None})")
    
    if _mongodb_records_collection is None:
        print(f"[MongoDB] Getting MongoDB client...")
        client = await get_mongodb_client()
        if client is not None:
            print(f"[MongoDB] Client obtained, accessing database: {MONGODB_DATABASE}")
            _mongodb_db = client[MONGODB_DATABASE]
            _mongodb_records_collection = _mongodb_db[MONGODB_RECORDS_COLLECTION]
            print(f"[MongoDB] Records collection obtained: {MONGODB_RECORDS_COLLECTION}")
            
            # Create indexes for better performance (non-critical, ignore errors)
            try:
                print(f"[MongoDB] Creating indexes...")
                # Use asyncio.wait_for to timeout if event loop is unstable
                await asyncio.wait_for(
                    _mongodb_records_collection.create_index("run_id", unique=True),
                    timeout=2.0
                )
                await asyncio.wait_for(
                    _mongodb_records_collection.create_index("created_at"),
                    timeout=2.0
                )
                await asyncio.wait_for(
                    _mongodb_records_collection.create_index("search_term"),
                    timeout=2.0
                )
                print(f"[MongoDB] Indexes created successfully")
                logger.info("MongoDB records indexes created successfully")
            except (asyncio.TimeoutError, RuntimeError) as e:
                # Silently ignore - indexes will be created on next successful connection
                print(f"[MongoDB] Skipped index creation: {type(e).__name__}")
                logger.debug(f"Skipped index creation (will retry later): {type(e).__name__}")
            except Exception as e:
                print(f"[MongoDB] Index creation failed: {e}")
                logger.debug(f"Failed to create MongoDB records indexes: {e}")
        else:
            print(f"[MongoDB] ERROR: Client is None, cannot get collection")
    else:
        print(f"[MongoDB] Reusing existing records collection")
    
    return _mongodb_records_collection


async def save_query_record(
    run_id: str,
    search_term: str,
    search_location: str,
    started_at: str,
    finished_at: str,
    emails: List[Dict[str, Any]],
    email_counters: Dict[str, Any]
) -> bool:
    """
    Save a query record with email data to MongoDB.
    
    Args:
        run_id: Unique identifier for the query/run
        search_term: The search term used
        search_location: The location searched
        started_at: ISO timestamp when query started
        finished_at: ISO timestamp when query finished
        emails: List of email documents in format:
                [{"name": "Business Name", "website": "url", "emails": ["email1", "email2"], "email_count": 2}, ...]
        email_counters: Counters for email statistics (sites_processed, emails_verified)
        
    Returns:
        True if stored successfully, False otherwise
    """
    try:
        print(f"[MongoDB] save_query_record called: run_id={run_id}, businesses={len(emails)}")
        logger.info(f"save_query_record called: run_id={run_id}, businesses={len(emails)}")
        
        collection = await get_mongodb_records_collection()
        if collection is None:
            msg = "MongoDB records collection not available (connection might be down)"
            print(f"[MongoDB] ERROR: {msg}")
            logger.warning(msg)
            return False
        
        # Prepare document to store
        print(f"[MongoDB] Preparing document for run_id={run_id}")
        doc = {
            "run_id": run_id,
            "search_term": search_term,
            "search_location": search_location,
            "started_at": started_at,
            "finished_at": finished_at,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "emails": emails,
            "email_count": len(emails),
            "email_counters": email_counters
        }
        
        print(f"[MongoDB] Document prepared, saving to MongoDB...")
        logger.info(f"Saving record to MongoDB: run_id={run_id}, businesses={len(emails)}, counters={email_counters}")
        
        # Upsert (insert or update) with timeout
        print(f"[MongoDB] Calling collection.replace_one...")
        result = await asyncio.wait_for(
            collection.replace_one(
                {"run_id": run_id},
                doc,
                upsert=True
            ),
            timeout=10.0
        )
        
        print(f"[MongoDB] replace_one completed: upserted_id={result.upserted_id}, modified={result.modified_count}")
        
        if result.upserted_id or result.modified_count:
            msg = f"[SUCCESS] Successfully saved query record: run_id={run_id}, businesses={len(emails)}, upserted={bool(result.upserted_id)}, modified={result.modified_count}"
            print(f"[MongoDB] {msg}")
            logger.info(msg)
            return True
        else:
            msg = f"MongoDB operation completed but no changes made for run_id: {run_id}"
            print(f"[MongoDB] WARNING: {msg}")
            logger.warning(msg)
            return False
            
    except asyncio.TimeoutError:
        logger.warning(f"MongoDB store timeout for run_id {run_id}")
        return False
    except RuntimeError as e:
        if "event loop" in str(e).lower():
            logger.debug(f"Event loop issue saving record for run_id {run_id}: {e}")
        else:
            logger.warning(f"Runtime error storing query record for run_id {run_id}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Error storing query record for run_id {run_id}: {e}")
        return False


async def get_all_query_records() -> List[Dict[str, Any]]:
    """
    Retrieve all query records from MongoDB.
    
    Returns:
        List of query record documents
    """
    try:
        collection = await get_mongodb_records_collection()
        if collection is None:
            return []
        
        # Query all records, sorted by created_at descending
        cursor = collection.find({}).sort("created_at", -1)
        records = await asyncio.wait_for(cursor.to_list(length=1000), timeout=5.0)
        
        # Convert ObjectId to string for JSON serialization
        for record in records:
            if "_id" in record:
                record["_id"] = str(record["_id"])
        
        logger.info(f"Retrieved {len(records)} query records")
        return records
        
    except asyncio.TimeoutError:
        logger.warning("MongoDB query timeout for all records")
        return []
    except Exception as e:
        logger.warning(f"Error retrieving query records: {e}")
        return []


async def get_query_record_by_run_id(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific query record by run_id.
    
    Args:
        run_id: The run_id to look up
        
    Returns:
        Query record document if found, None otherwise
    """
    try:
        collection = await get_mongodb_records_collection()
        if collection is None:
            return None
        
        doc = await asyncio.wait_for(
            collection.find_one({"run_id": run_id}),
            timeout=3.0
        )
        
        if doc:
            # Convert ObjectId to string for JSON serialization
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            logger.info(f"Found query record for run_id: {run_id}")
            return doc
        else:
            logger.info(f"No query record found for run_id: {run_id}")
            return None
            
    except asyncio.TimeoutError:
        logger.warning(f"MongoDB query timeout for run_id {run_id}")
        return None
    except Exception as e:
        logger.warning(f"Error retrieving query record for run_id {run_id}: {e}")
        return None


async def delete_query_record(run_id: str) -> bool:
    """
    Delete a query record.
    
    Args:
        run_id: The run_id to delete
        
    Returns:
        True if deleted successfully, False otherwise
    """
    collection = await get_mongodb_records_collection()
    if collection is None:
        return False
    
    try:
        result = await collection.delete_one({"run_id": run_id})
        if result.deleted_count > 0:
            logger.info(f"Deleted query record for run_id: {run_id}")
            return True
        else:
            logger.info(f"No query record found for run_id: {run_id}")
            return False
    except Exception as e:
        logger.error(f"Error deleting query record for run_id {run_id}: {e}")
        return False

