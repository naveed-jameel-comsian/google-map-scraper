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
MONGODB_DISABLED = os.getenv("MONGODB_DISABLED", "false").lower() in ("true", "1", "yes")

# Global MongoDB connection
_mongodb_client: Optional[Any] = None
_mongodb_db: Optional[Any] = None
_mongodb_collection: Optional[Any] = None

logger = logging.getLogger(__name__)


async def get_mongodb_client() -> Optional[Any]:
    """Get or create MongoDB client connection."""
    global _mongodb_client
    
    if MONGODB_DISABLED:
        logger.info("MongoDB is disabled via MONGODB_DISABLED environment variable")
        return None
    
    if _mongodb_client is None and AsyncIOMotorClient is not None:
        try:
            _mongodb_client = AsyncIOMotorClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=2000,  # 2 second timeout
                connectTimeoutMS=2000,
                socketTimeoutMS=2000
            )
            # Test connection with shorter timeout
            await asyncio.wait_for(_mongodb_client.admin.command('ping'), timeout=2.0)
            logger.info(f"Connected to MongoDB at {MONGODB_URI}")
        except asyncio.TimeoutError:
            logger.warning(f"MongoDB connection timeout at {MONGODB_URI}")
            _mongodb_client = None
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.warning(f"Failed to connect to MongoDB: {e}")
            _mongodb_client = None
        except Exception as e:
            logger.warning(f"Unexpected error connecting to MongoDB: {e}")
            _mongodb_client = None
    
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
    global _mongodb_client, _mongodb_db, _mongodb_collection
    
    if _mongodb_client:
        _mongodb_client.close()
        _mongodb_client = None
        _mongodb_db = None
        _mongodb_collection = None


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
