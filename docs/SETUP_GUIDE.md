# Setup Guide - Tenna Lakehouse Data Platform

This guide walks you through setting up the Tenna data platform in Microsoft Fabric.

## Prerequisites

- Microsoft Fabric workspace with appropriate permissions
- Tenna account with API access
- Tenna API token (get from account settings)

## Step 1: Create Lakehouse

1. Open your Microsoft Fabric workspace
2. Click **+ New** → **Lakehouse**
3. Name it: `Tenna_Raw`
4. Click **Create**

## Step 2: Upload Notebooks

1. In your Fabric workspace, click **+ New** → **Import notebook**
2. Upload `ten_tables_ingestion.ipynb`
3. Repeat for `daily_ingestion_function.ipynb`
4. Attach both notebooks to the `Tenna_Raw` lakehouse

## Step 3: Configure API Credentials

### For ten_tables_ingestion.ipynb:

```python
# Line 23-25: Update with your credentials
API_TOKEN = "your_actual_tenna_api_token"
BASE_URL = "https://api.tenna.com/v1"  # Keep as-is
LIMIT = 100  # Adjust if needed
```

### For daily_ingestion_function.ipynb:

```python
# Line 21-25: Update with your credentials
API_TOKEN = "your_actual_tenna_api_token"
BASE_URL = "https://api.tenna.com/v1"  # Keep as-is
LIMIT = 100
MAX_RETRIES = 5
RETRY_DELAY = 60
```

## Step 4: Initial Data Load (One-Time)

1. Open `ten_tables_ingestion.ipynb`
2. Click **Run all**
3. Wait for completion (may take 1-3 hours)
4. Verify 10 tables were created in `Tenna_Raw` lakehouse

**Expected tables:**
- `asset_financials`
- `assets`
- `asset_assignee_history`
- `asset_label_associations`
- `asset_dt_codes`
- `asset_labels`
- `asset_organization_history`
- `asset_registrations`
- `asset_site_history`
- `asset_warranties`
- `control_last_run` (tracking table)

## Step 5: Set Up Daily Incremental Loads

The `daily_ingestion_function.ipynb` notebook needs to be run 4 times (once per endpoint).

### Create 4 Separate Notebooks:

1. **Duplicate the notebook 4 times**:
   - Right-click `daily_ingestion_function.ipynb` → **Duplicate**
   - Rename copies:
     - `daily_readings_ingestion`
     - `daily_utilizations_ingestion`
     - `daily_site_utilizations_ingestion`
     - `daily_working_hours_ingestion`

2. **Configure each notebook** (Lines 31-32):

**Notebook 1: daily_readings_ingestion**
```python
endpoint_name = "asset-readings-daily"
table_name = "asset_readings_daily"
```

**Notebook 2: daily_utilizations_ingestion**
```python
endpoint_name = "asset-utilizations-daily"
table_name = "asset_utilizations_daily"
```

**Notebook 3: daily_site_utilizations_ingestion**
```python
endpoint_name = "asset-site-daily-utilizations"
table_name = "asset_site_daily_utilizations"
```

**Notebook 4: daily_working_hours_ingestion**
```python
endpoint_name = "asset-net-working-hours-daily"
table_name = "asset_net_working_hours_daily"
```

3. **Run each notebook** to create the initial tables

## Step 6: Create Automated Pipeline

1. In your Fabric workspace, click **+ New** → **Data pipeline**
2. Name it: `Tenna_Daily_Refresh`
3. Add activities in sequence:

### Pipeline Structure:

```
[Static Tables Refresh]
    ↓
[Daily Readings]
    ↓
[Daily Utilizations]
    ↓
[Daily Site Utilizations]
    ↓
[Daily Working Hours]
    ↓
[Refresh Semantic Model]
```

### Add Activities:

1. **Add Notebook Activity** → Select `ten_tables_ingestion`
2. **Add Notebook Activity** → Select `daily_readings_ingestion`
3. **Add Notebook Activity** → Select `daily_utilizations_ingestion`
4. **Add Notebook Activity** → Select `daily_site_utilizations_ingestion`
5. **Add Notebook Activity** → Select `daily_working_hours_ingestion`

### Connect Activities:
- Click **On success** output from one activity
- Drag to next activity

## Step 7: Schedule Pipeline

1. Click **Home** tab → **Schedule**
2. Configure schedule:
   - **Repeat**: Daily
   - **Time**: 2:00 AM (or your preferred time)
   - **Time zone**: Your timezone
3. Click **Apply**

## Step 8: Create Semantic Model (Optional)

If you're building Power BI dashboards:

1. In Fabric workspace, click **+ New** → **Semantic model**
2. Select your lakehouse: `Tenna_Raw`
3. Choose tables to include
4. Define relationships:
   - `assets.asset_id` ↔ `asset_financials.asset_id`
   - `assets.asset_id` ↔ `asset_utilizations_daily.asset_id`
   - etc.
5. Create DAX measures as needed

## Step 9: Validation

Run these SQL queries to validate your setup:

```sql
-- Check row counts
SELECT 'assets' as table_name, COUNT(*) as rows FROM Tenna_Raw.assets
UNION ALL
SELECT 'asset_financials', COUNT(*) FROM Tenna_Raw.asset_financials;

-- Check for duplicates
SELECT asset_id, COUNT(*) as count
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

-- Verify last run
SELECT * FROM Tenna_Raw.control_last_run;
SELECT * FROM pipeline_last_run ORDER BY last_run_time DESC;
```

See `sql/validation_queries.sql` for comprehensive checks.

## Troubleshooting

### Issue: Rate Limit Errors (429)

**Solution**: The code handles this automatically with exponential backoff. If persistent:
- Reduce `LIMIT` from 100 to 50
- Increase `RETRY_DELAY` from 60 to 120 seconds

### Issue: Schema Mismatch Errors

**Solution**: Delete table and re-run with `overwriteSchema=true`:
```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
```

### Issue: Duplicates in Daily Tables

**Solution**: The MERGE logic should prevent this. If it occurs:
```sql
-- Find duplicates
SELECT asset_id, date, COUNT(*) 
FROM asset_utilizations_daily
GROUP BY asset_id, date
HAVING COUNT(*) > 1;

-- Delete table and reload
DROP TABLE asset_utilizations_daily;
-- Then re-run notebook
```

### Issue: Missing Data

**Solution**: Check incremental loading:
```sql
-- Check last load date
SELECT MAX(date) FROM asset_utilizations_daily;

-- Compare to last run time
SELECT * FROM pipeline_last_run 
WHERE endpoint_name = 'asset-utilizations-daily';
```

## Monitoring

### Check Pipeline Status
- Go to **Fabric workspace** → **Pipelines**
- View run history for `Tenna_Daily_Refresh`

### Check Data Freshness
```sql
SELECT 
    MAX(updated_at) as latest_update,
    DATEDIFF(hour, MAX(updated_at), GETDATE()) as hours_old
FROM Tenna_Raw.assets;
```

### Monitor Record Counts
```sql
SELECT 
    endpoint_name,
    last_run_time,
    records_processed
FROM pipeline_last_run
ORDER BY last_run_time DESC;
```

## Performance Optimization

### For Large Endpoints (500K+ rows)

Consider using **Dataflow Gen2** instead of Python notebooks:

1. Create **New Dataflow Gen2**
2. Add **REST API connector**
3. Configure:
   - URL: `https://api.tenna.com/v1/{endpoint}`
   - Headers: `Authorization: Bearer {token}`
   - Pagination: Offset-based
4. Add transformations (flatten, type casts)
5. Set destination: Your lakehouse table
6. Add to pipeline before semantic model refresh

### Cache Control Table

For frequently accessed control tables:
```python
df.cache()  # Already implemented
```

## Next Steps

1. **Build Power BI Dashboard**
   - Connect to semantic model
   - Create visualizations
   - Publish to workspace

2. **Add Data Quality Alerts**
   - Monitor row counts
   - Check for nulls
   - Validate date ranges

3. **Extend to Additional Endpoints**
   - Follow same pattern
   - Add to configuration
   - Update pipeline

## Support

For issues specific to:
- **Tenna API**: Contact Tenna support or check API docs
- **Microsoft Fabric**: Check Microsoft Fabric documentation
- **This implementation**: Open an issue on GitHub

---

**Congratulations!** You now have a fully automated data platform. 🎉# Setup Guide - Tenna Lakehouse Data Platform

This guide walks you through setting up the Tenna data platform in Microsoft Fabric.

## Prerequisites

- Microsoft Fabric workspace with appropriate permissions
- Tenna account with API access
- Tenna API token (get from account settings)

## Step 1: Create Lakehouse

1. Open your Microsoft Fabric workspace
2. Click **+ New** → **Lakehouse**
3. Name it: `Tenna_Raw`
4. Click **Create**

## Step 2: Upload Notebooks

1. In your Fabric workspace, click **+ New** → **Import notebook**
2. Upload `ten_tables_ingestion.ipynb`
3. Repeat for `daily_ingestion_function.ipynb`
4. Attach both notebooks to the `Tenna_Raw` lakehouse

## Step 3: Configure API Credentials

### For ten_tables_ingestion.ipynb:

```python
# Line 23-25: Update with your credentials
API_TOKEN = "your_actual_tenna_api_token"
BASE_URL = "https://api.tenna.com/v1"  # Keep as-is
LIMIT = 100  # Adjust if needed
```

### For daily_ingestion_function.ipynb:

```python
# Line 21-25: Update with your credentials
API_TOKEN = "your_actual_tenna_api_token"
BASE_URL = "https://api.tenna.com/v1"  # Keep as-is
LIMIT = 100
MAX_RETRIES = 5
RETRY_DELAY = 60
```

## Step 4: Initial Data Load (One-Time)

1. Open `ten_tables_ingestion.ipynb`
2. Click **Run all**
3. Wait for completion (may take 1-3 hours)
4. Verify 10 tables were created in `Tenna_Raw` lakehouse

**Expected tables:**
- `asset_financials`
- `assets`
- `asset_assignee_history`
- `asset_label_associations`
- `asset_dt_codes`
- `asset_labels`
- `asset_organization_history`
- `asset_registrations`
- `asset_site_history`
- `asset_warranties`
- `control_last_run` (tracking table)

## Step 5: Set Up Daily Incremental Loads

The `daily_ingestion_function.ipynb` notebook needs to be run 4 times (once per endpoint).

### Create 4 Separate Notebooks:

1. **Duplicate the notebook 4 times**:
   - Right-click `daily_ingestion_function.ipynb` → **Duplicate**
   - Rename copies:
     - `daily_readings_ingestion`
     - `daily_utilizations_ingestion`
     - `daily_site_utilizations_ingestion`
     - `daily_working_hours_ingestion`

2. **Configure each notebook** (Lines 31-32):

**Notebook 1: daily_readings_ingestion**
```python
endpoint_name = "asset-readings-daily"
table_name = "asset_readings_daily"
```

**Notebook 2: daily_utilizations_ingestion**
```python
endpoint_name = "asset-utilizations-daily"
table_name = "asset_utilizations_daily"
```

**Notebook 3: daily_site_utilizations_ingestion**
```python
endpoint_name = "asset-site-daily-utilizations"
table_name = "asset_site_daily_utilizations"
```

**Notebook 4: daily_working_hours_ingestion**
```python
endpoint_name = "asset-net-working-hours-daily"
table_name = "asset_net_working_hours_daily"
```

3. **Run each notebook** to create the initial tables

## Step 6: Create Automated Pipeline

1. In your Fabric workspace, click **+ New** → **Data pipeline**
2. Name it: `Tenna_Daily_Refresh`
3. Add activities in sequence:

### Pipeline Structure:

```
[Static Tables Refresh]
    ↓
[Daily Readings]
    ↓
[Daily Utilizations]
    ↓
[Daily Site Utilizations]
    ↓
[Daily Working Hours]
    ↓
[Refresh Semantic Model]
```

### Add Activities:

1. **Add Notebook Activity** → Select `ten_tables_ingestion`
2. **Add Notebook Activity** → Select `daily_readings_ingestion`
3. **Add Notebook Activity** → Select `daily_utilizations_ingestion`
4. **Add Notebook Activity** → Select `daily_site_utilizations_ingestion`
5. **Add Notebook Activity** → Select `daily_working_hours_ingestion`

### Connect Activities:
- Click **On success** output from one activity
- Drag to next activity

## Step 7: Schedule Pipeline

1. Click **Home** tab → **Schedule**
2. Configure schedule:
   - **Repeat**: Daily
   - **Time**: 2:00 AM (or your preferred time)
   - **Time zone**: Your timezone
3. Click **Apply**

## Step 8: Create Semantic Model (Optional)

If you're building Power BI dashboards:

1. In Fabric workspace, click **+ New** → **Semantic model**
2. Select your lakehouse: `Tenna_Raw`
3. Choose tables to include
4. Define relationships:
   - `assets.asset_id` ↔ `asset_financials.asset_id`
   - `assets.asset_id` ↔ `asset_utilizations_daily.asset_id`
   - etc.
5. Create DAX measures as needed


## Step 9: Validation

Run these SQL queries to validate your setup:

> **Note:** The queries below use example table names. You have **14 tables total** in your lakehouse. Update the table names in these queries to check row counts and duplicates for each of your specific tables.
```sql
-- Check row counts (UPDATE table names for all 14 tables)
SELECT 'assets' as table_name, COUNT(*) as rows FROM Tenna_Raw.assets
UNION ALL
SELECT 'asset_financials', COUNT(*) FROM Tenna_Raw.asset_financials
UNION ALL
SELECT 'asset_readings_daily', COUNT(*) FROM Tenna_Raw.asset_readings_daily;
-- Add remaining 11 tables...

-- Check for duplicates (UPDATE table name and key field)
SELECT asset_id, COUNT(*) as count
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;
-- Repeat for each table with appropriate primary key

-- Verify last run
SELECT * FROM Tenna_Raw.control_last_run;
SELECT * FROM Tenna_Raw.endpoint_tracking ORDER BY last_run_time DESC;
```

**Your 14 Tables:**
- Static Tables (10): `asset_financials`, `assets`, `asset_assignee_history`, `asset_label_associations`, `asset_dt_codes`, `asset_labels`, `asset_organization_history`, `asset_registrations`, `asset_site_history`, `asset_warranties`
- Daily Tables (4): `asset_readings_daily`, `asset_utilizations_daily`, `asset_site_daily_utilizations`, `asset_net_working_hours_daily`

See `sql/validation_queries.sql` for comprehensive checks across all 14 tables.

## Troubleshooting

### Issue: Rate Limit Errors (429)

**Solution**: The code handles this automatically with exponential backoff. If persistent:
- Reduce `LIMIT` from 100 to 50
- Increase `RETRY_DELAY` from 60 to 120 seconds

### Issue: Schema Mismatch Errors

**Solution**: Delete table and re-run with `overwriteSchema=true`:
```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
```

### Issue: Duplicates in Daily Tables

**Solution**: The MERGE logic should prevent this. If it occurs:
```sql
-- Find duplicates
SELECT asset_id, date, COUNT(*) 
FROM asset_utilizations_daily
GROUP BY asset_id, date
HAVING COUNT(*) > 1;

-- Delete table and reload
DROP TABLE asset_utilizations_daily;
-- Then re-run notebook
```

### Issue: Missing Data

**Solution**: Check incremental loading:
```sql
-- Check last load date
SELECT MAX(date) FROM asset_utilizations_daily;

-- Compare to last run time
SELECT * FROM pipeline_last_run 
WHERE endpoint_name = 'asset-utilizations-daily';
```

## Monitoring

### Check Pipeline Status
- Go to **Fabric workspace** → **Pipelines**
- View run history for `Tenna_Daily_Refresh`

### Check Data Freshness
```sql
SELECT 
    MAX(updated_at) as latest_update,
    DATEDIFF(hour, MAX(updated_at), GETDATE()) as hours_old
FROM Tenna_Raw.assets;
```

### Monitor Record Counts
```sql
SELECT 
    endpoint_name,
    last_run_time,
    records_processed
FROM pipeline_last_run
ORDER BY last_run_time DESC;
```

## Performance Optimization

### For Large Endpoints (500K+ rows)

Consider using **Dataflow Gen2** instead of Python notebooks:

1. Create **New Dataflow Gen2**
2. Add **REST API connector**
3. Configure:
   - URL: `https://api.tenna.com/v1/{endpoint}`
   - Headers: `Authorization: Bearer {token}`
   - Pagination: Offset-based
4. Add transformations (flatten, type casts)
5. Set destination: Your lakehouse table
6. Add to pipeline before semantic model refresh

### Cache Control Table

For frequently accessed control tables:
```python
df.cache()  # Already implemented
```

## Next Steps

1. **Build Power BI Dashboard**
   - Connect to semantic model
   - Create visualizations
   - Publish to workspace

2. **Add Data Quality Alerts**
   - Monitor row counts
   - Check for nulls
   - Validate date ranges

3. **Extend to Additional Endpoints**
   - Follow same pattern
   - Add to configuration
   - Update pipeline

## Support

For issues specific to:
- **Tenna API**: Contact Tenna support or check API docs
- **Microsoft Fabric**: Check Microsoft Fabric documentation
- **This implementation**: Open an issue on GitHub

---

**Congratulations!** You now have a fully automated data platform. 🎉
