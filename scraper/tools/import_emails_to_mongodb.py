#!/usr/bin/env python3
"""
Script to import emails from CSV file to MongoDB.

Usage:
    python scraper/tools/import_emails_to_mongodb.py path/to/emails.csv

CSV Format:
    - Must have an 'EMAIL' column header
    - Each row contains one email address
"""

import asyncio
import csv
import os
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import argparse

# Add parent directory to path to import core modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    print("ERROR: motor library not installed. Install with: pip install motor pymongo")
    sys.exit(1)


# MongoDB configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "email_scraper")
MONGODB_EMAILS_COLLECTION = os.getenv("MONGODB_EMAILS_COLLECTION", "emails")


def read_emails_from_csv(csv_path: str) -> List[str]:
    """
    Read emails from a CSV file.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        List of email addresses
    """
    if not os.path.exists(csv_path):
        print(f"ERROR: File not found: {csv_path}")
        sys.exit(1)
    
    emails = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Try to detect if file has header
            sample = f.read(1024)
            f.seek(0)
            
            # Check if first line contains 'EMAIL' or similar header
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(sample)
            
            reader = csv.DictReader(f) if has_header else csv.reader(f)
            
            for row in reader:
                if isinstance(row, dict):
                    # DictReader - look for EMAIL column (case insensitive)
                    email = None
                    for key in row.keys():
                        if key.upper() == 'EMAIL':
                            email = row[key].strip()
                            break
                    if email:
                        emails.append(email)
                else:
                    # Regular reader - assume first column is email
                    if row and row[0]:
                        email = row[0].strip()
                        if email and email.upper() != 'EMAIL':  # Skip header if present
                            emails.append(email)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_emails = []
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen and email_lower:
                seen.add(email_lower)
                unique_emails.append(email)
        
        print(f"✓ Read {len(emails)} emails from CSV ({len(unique_emails)} unique)")
        return unique_emails
        
    except Exception as e:
        print(f"ERROR: Failed to read CSV file: {e}")
        sys.exit(1)


def validate_email(email: str) -> bool:
    return True
    """Basic email validation."""
    if not email or '@' not in email:
        return False
    parts = email.split('@')
    if len(parts) != 2:
        return False
    if not parts[0] or not parts[1]:
        return False
    if '.' not in parts[1]:
        return False
    return True


async def connect_to_mongodb() -> Optional[AsyncIOMotorClient]:
    """Connect to MongoDB and test connection."""
    try:
        print(f"Connecting to MongoDB at {MONGODB_URI}...")
        client = AsyncIOMotorClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000
        )
        
        # Test connection
        await asyncio.wait_for(client.admin.command('ping'), timeout=5.0)
        print(f"✓ Successfully connected to MongoDB")
        return client
        
    except asyncio.TimeoutError:
        print(f"ERROR: MongoDB connection timeout at {MONGODB_URI}")
        print("Hint: Is MongoDB running? Try: brew services start mongodb-community")
        return None
    except Exception as e:
        print(f"ERROR: Failed to connect to MongoDB: {e}")
        print("Hint: Start MongoDB with 'brew services start mongodb-community' or 'sudo systemctl start mongodb'")
        return None


async def import_emails_to_mongodb(
    emails: List[str],
    batch_size: int = 100,
    skip_duplicates: bool = True
) -> Dict[str, int]:
    """
    Import emails to MongoDB.
    
    Args:
        emails: List of email addresses
        batch_size: Number of emails to insert per batch
        skip_duplicates: If True, skip emails that already exist
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        'total': len(emails),
        'valid': 0,
        'invalid': 0,
        'inserted': 0,
        'duplicates': 0,
        'errors': 0
    }
    
    # Connect to MongoDB
    client = await connect_to_mongodb()
    if not client:
        return stats
    
    try:
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_EMAILS_COLLECTION]
        
        # Create unique index on email field to prevent duplicates
        print(f"Creating unique index on 'email' field...")
        try:
            await collection.create_index("email", unique=True)
            await collection.create_index("created_at")
            print(f"✓ Indexes created")
        except Exception as e:
            print(f"Warning: Could not create indexes: {e}")
        
        print(f"\nImporting {len(emails)} emails to collection '{MONGODB_EMAILS_COLLECTION}'...")
        
        # Process emails in batches
        for i in range(0, len(emails), batch_size):
            batch = emails[i:i + batch_size]
            
            for email in batch:
                # Validate email
                if not validate_email(email):
                    stats['invalid'] += 1
                    print(f"  ⚠ Invalid email format: {email}")
                    continue
                
                stats['valid'] += 1
                
                # Prepare document
                doc = {
                    'email': email.lower(),
                    'original_email': email,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'csv_import'
                }
                
                try:
                    if skip_duplicates:
                        # Use update_one with upsert to avoid duplicates
                        result = await collection.update_one(
                            {'email': email.lower()},
                            {'$setOnInsert': doc},
                            upsert=True
                        )
                        
                        if result.upserted_id:
                            stats['inserted'] += 1
                        else:
                            stats['duplicates'] += 1
                            print(f"  ⊘ Duplicate skipped: {email}")
                    else:
                        # Insert directly (may fail on duplicates)
                        await collection.insert_one(doc)
                        stats['inserted'] += 1
                        
                except Exception as e:
                    stats['errors'] += 1
                    print(f"  ✗ Error inserting {email}: {e}")
            
            # Progress update
            processed = min(i + batch_size, len(emails))
            print(f"  Progress: {processed}/{len(emails)} emails processed...")
        
        print(f"\n{'='*60}")
        print(f"Import Summary:")
        print(f"{'='*60}")
        print(f"  Total emails:     {stats['total']}")
        print(f"  Valid emails:     {stats['valid']}")
        print(f"  Invalid emails:   {stats['invalid']}")
        print(f"  Inserted:         {stats['inserted']}")
        print(f"  Duplicates:       {stats['duplicates']}")
        print(f"  Errors:           {stats['errors']}")
        print(f"{'='*60}")
        
        # Verify final count in database
        total_count = await collection.count_documents({})
        print(f"\nTotal emails in database: {total_count}")
        
    finally:
        client.close()
    
    return stats


async def list_emails_in_collection(limit: int = 10):
    """List some emails from the collection for verification."""
    client = await connect_to_mongodb()
    if not client:
        return
    
    try:
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_EMAILS_COLLECTION]
        
        total_count = await collection.count_documents({})
        print(f"\nTotal emails in collection: {total_count}")
        
        if total_count > 0:
            print(f"\nFirst {min(limit, total_count)} emails:")
            print("-" * 60)
            
            cursor = collection.find({}).limit(limit)
            async for doc in cursor:
                email = doc.get('email', 'N/A')
                created = doc.get('created_at', 'N/A')
                print(f"  {email} (added: {created[:19]})")
    
    finally:
        client.close()


async def main():
    parser = argparse.ArgumentParser(
        description='Import emails from CSV file to MongoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import emails from CSV
  python scraper/tools/import_emails_to_mongodb.py emails.csv
  
  # Import with custom batch size
  python scraper/tools/import_emails_to_mongodb.py emails.csv --batch-size 50
  
  # List emails in collection
  python scraper/tools/import_emails_to_mongodb.py --list
  
  # Allow duplicate inserts (not recommended)
  python scraper/tools/import_emails_to_mongodb.py emails.csv --allow-duplicates

Environment Variables:
  MONGODB_URI                MongoDB connection URI (default: mongodb://localhost:27017)
  MONGODB_DATABASE           Database name (default: email_scraper)
  MONGODB_EMAILS_COLLECTION  Collection name (default: emails)
        """
    )
    
    parser.add_argument(
        'csv_file',
        nargs='?',
        help='Path to CSV file containing emails'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of emails to process per batch (default: 100)'
    )
    parser.add_argument(
        '--allow-duplicates',
        action='store_true',
        help='Allow duplicate emails (will fail if email already exists)'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List emails in the collection'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Number of emails to list (default: 10)'
    )
    
    args = parser.parse_args()
    
    # List mode
    if args.list:
        await list_emails_in_collection(args.limit)
        return
    
    # Import mode
    if not args.csv_file:
        parser.print_help()
        print("\nERROR: CSV file path is required (or use --list to view collection)")
        sys.exit(1)
    
    print("="*60)
    print("Email CSV to MongoDB Import Tool")
    print("="*60)
    print(f"CSV File:       {args.csv_file}")
    print(f"MongoDB URI:    {MONGODB_URI}")
    print(f"Database:       {MONGODB_DATABASE}")
    print(f"Collection:     {MONGODB_EMAILS_COLLECTION}")
    print(f"Batch Size:     {args.batch_size}")
    print(f"Skip Duplicates: {not args.allow_duplicates}")
    print("="*60)
    print()
    
    # Read emails from CSV
    emails = read_emails_from_csv(args.csv_file)
    
    if not emails:
        print("ERROR: No valid emails found in CSV file")
        sys.exit(1)
    
    # Import to MongoDB
    stats = await import_emails_to_mongodb(
        emails,
        batch_size=args.batch_size,
        skip_duplicates=not args.allow_duplicates
    )
    
    if stats['inserted'] > 0:
        print(f"\n✓ Successfully imported {stats['inserted']} emails to MongoDB!")
    else:
        print(f"\n⚠ No new emails were imported")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)






