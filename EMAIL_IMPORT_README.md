# Email Import to MongoDB

This script imports emails from a CSV file into MongoDB.

## Features

- ✅ Reads emails from CSV files
- ✅ Validates email format
- ✅ Automatically detects CSV headers
- ✅ Prevents duplicate emails
- ✅ Batch processing for performance
- ✅ Progress tracking
- ✅ Detailed statistics

## CSV Format

Your CSV file should have an `EMAIL` column header (case insensitive):

```csv
EMAIL
amanda@nationalrecalls.com
4sauers@comcast.net
agx4@yahoo.com
```

Or without header (first column will be treated as email):

```csv
amanda@nationalrecalls.com
4sauers@comcast.net
agx4@yahoo.com
```

## Usage

### Prerequisites

1. **Start MongoDB**:
   ```bash
   brew services start mongodb-community
   ```

2. **Activate virtual environment**:
   ```bash
   source venv/bin/activate
   ```

### Import Emails from CSV

```bash
python scraper/tools/import_emails_to_mongodb.py path/to/your/emails.csv
```

Example with your Brevo contacts:
```bash
python scraper/tools/import_emails_to_mongodb.py "All Brevo Contacts.csv"
```

### Advanced Options

**Custom batch size** (default: 100):
```bash
python scraper/tools/import_emails_to_mongodb.py emails.csv --batch-size 50
```

**List existing emails** in MongoDB:
```bash
python scraper/tools/import_emails_to_mongodb.py --list
```

**List more emails** (default shows 10):
```bash
python scraper/tools/import_emails_to_mongodb.py --list --limit 20
```

**Allow duplicate emails** (not recommended):
```bash
python scraper/tools/import_emails_to_mongodb.py emails.csv --allow-duplicates
```

### View Help

```bash
python scraper/tools/import_emails_to_mongodb.py --help
```

## Configuration

Configure via environment variables (optional):

```bash
# MongoDB connection URI
export MONGODB_URI="mongodb://localhost:27017"

# Database name
export MONGODB_DATABASE="email_scraper"

# Collection name
export MONGODB_EMAILS_COLLECTION="emails"
```

## Output Example

```
============================================================
Email CSV to MongoDB Import Tool
============================================================
CSV File:       All Brevo Contacts.csv
MongoDB URI:    mongodb://localhost:27017
Database:       email_scraper
Collection:     emails
Batch Size:     100
Skip Duplicates: True
============================================================

✓ Read 3 emails from CSV (3 unique)
Connecting to MongoDB at mongodb://localhost:27017...
✓ Successfully connected to MongoDB
Creating unique index on 'email' field...
✓ Indexes created

Importing 3 emails to collection 'emails'...
  Progress: 3/3 emails processed...

============================================================
Import Summary:
============================================================
  Total emails:     3
  Valid emails:     3
  Invalid emails:   0
  Inserted:         3
  Duplicates:       0
  Errors:           0
============================================================

Total emails in database: 3

✓ Successfully imported 3 emails to MongoDB!
```

## MongoDB Collection Structure

Each email is stored as a document with the following fields:

```json
{
  "email": "amanda@nationalrecalls.com",
  "original_email": "amanda@nationalrecalls.com",
  "created_at": "2025-10-17T12:34:56.789Z",
  "source": "csv_import",
  "status": "active"
}
```

## Querying Emails in MongoDB

Using MongoDB shell or Compass:

```javascript
// View all emails
db.emails.find()

// Count total emails
db.emails.countDocuments()

// Find specific email
db.emails.findOne({email: "amanda@nationalrecalls.com"})

// Find emails by domain
db.emails.find({email: /@example\.com$/})
```

## Error Handling

The script handles:
- ✅ Invalid email formats (skipped with warning)
- ✅ Duplicate emails (skipped by default)
- ✅ MongoDB connection failures
- ✅ Missing or invalid CSV files
- ✅ Batch processing errors

## Notes

- **Duplicate Prevention**: The script creates a unique index on the `email` field, so duplicate emails are automatically skipped
- **Case Insensitive**: All emails are stored in lowercase for consistency
- **Original Preserved**: The original email (with original casing) is also stored
- **Batch Processing**: Emails are processed in batches of 100 by default for optimal performance
- **Safe to Re-run**: Running the script multiple times with the same CSV is safe - duplicates will be skipped

## Troubleshooting

### MongoDB Connection Failed

```bash
# Start MongoDB
brew services start mongodb-community

# Check if MongoDB is running
brew services list | grep mongodb
```

### Module Not Found Error

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies if needed
pip install motor pymongo
```

### Permission Denied

```bash
# Make script executable
chmod +x scraper/tools/import_emails_to_mongodb.py
```

