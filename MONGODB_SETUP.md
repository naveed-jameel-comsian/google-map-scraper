# MongoDB Setup for Email Caching

This project now includes MongoDB integration for caching emails by domain name to improve performance and avoid redundant scraping.

## Configuration

Set the following environment variables:

```bash
# MongoDB Configuration
export MONGODB_URI=mongodb://localhost:27017
export MONGODB_DATABASE=email_scraper
export MONGODB_COLLECTION=domain_emails

# Hunter API Key (optional)
export HUNTER_API_KEY=your_hunter_api_key_here

# Output Directory
export OUT_ROOT=out
```

## Troubleshooting

### If MongoDB is causing scraping failures:

1. **Disable MongoDB temporarily**:
   ```bash
   export MONGODB_DISABLED=true
   ```
   The scraper will work without caching.

2. **Check MongoDB connection**:
   ```bash
   # Test if MongoDB is running
   mongosh --eval "db.runCommand('ping')"
   ```

3. **Common issues**:
   - MongoDB not installed or not running
   - Wrong connection URI
   - Network connectivity issues
   - Permission problems

### Error Messages

- `emails_scraping_failed`: Usually indicates MongoDB connection issues
- The scraper will automatically fall back to non-cached operation if MongoDB fails
- All MongoDB operations have timeouts and error handling

## How It Works

1. **Cache Check**: Before scraping any domain, the system checks MongoDB for existing emails
2. **Cache Hit**: If emails are found in cache, they are returned immediately (no scraping needed)
3. **Cache Miss**: If no cached emails exist, normal scraping proceeds
4. **Cache Store**: After scraping, all found emails are stored in MongoDB for future use

## Database Structure

The MongoDB collection stores documents with this structure:

```json
{
  "_id": ObjectId("..."),
  "domain": "example.com",
  "emails": [
    {
      "email": "contact@example.com",
      "found_on": "https://example.com/contact",
      "source": "scraper",
      "cached_at": "2024-01-01T12:00:00Z"
    }
  ],
  "email_count": 1,
  "last_updated": "2024-01-01T12:00:00Z",
  "source": "scraper"
}
```

## Installation

1. Install MongoDB locally or use MongoDB Atlas
2. Install Python dependencies:
   ```bash
   pip install pymongo==4.9.1 motor==3.6.0
   ```
3. Set environment variables
4. Run the scraper - it will automatically use MongoDB if available

## Benefits

- **Faster Performance**: Cached domains return results instantly
- **Reduced API Calls**: Hunter API calls are cached separately
- **Cost Savings**: Fewer external API requests
- **Reliability**: Works even if MongoDB is unavailable (graceful fallback)

## Cache Management

- Hunter results are cached separately with `_hunter` suffix
- Cache is automatically updated when new emails are found
- No manual cache invalidation needed
- Cache persists across scraper runs
