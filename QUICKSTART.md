# Quick Start Checklist

Use this checklist to get up and running quickly.

## ✅ Pre-Setup

- [ ] Have Tenna API token ready
- [ ] Have Microsoft Fabric workspace access
- [ ] Have appropriate permissions to create resources

## ✅ Initial Setup (One-Time)

### Step 1: Create Resources
- [ ] Create Fabric Lakehouse named `Tenna_Raw`
- [ ] Upload `ten_tables_ingestion.ipynb` to workspace
- [ ] Upload `daily_ingestion_function.ipynb` to workspace
- [ ] Attach notebooks to `Tenna_Raw` lakehouse

### Step 2: Configure Credentials
- [ ] Update `API_TOKEN` in `ten_tables_ingestion.ipynb` (line 23)
- [ ] Update `API_TOKEN` in `daily_ingestion_function.ipynb` (line 21)

### Step 3: Run Initial Load
- [ ] Run `ten_tables_ingestion.ipynb` (wait 1-3 hours)
- [ ] Verify 10 tables created in lakehouse
- [ ] Check `control_last_run` table exists

### Step 4: Configure Daily Tables
- [ ] Duplicate `daily_ingestion_function.ipynb` 4 times
- [ ] Configure each copy for specific endpoint:
  - [ ] `asset-readings-daily` → `asset_readings_daily`
  - [ ] `asset-utilizations-daily` → `asset_utilizations_daily`
  - [ ] `asset-site-daily-utilizations` → `asset_site_daily_utilizations`
  - [ ] `asset-net-working-hours-daily` → `asset_net_working_hours_daily`
- [ ] Run each notebook once to create tables
- [ ] Verify `pipeline_last_run` table exists

## ✅ Automation Setup

### Step 5: Create Pipeline
- [ ] Create new Data Pipeline named `Tenna_Daily_Refresh`
- [ ] Add notebook activities in order:
  1. [ ] `ten_tables_ingestion`
  2. [ ] `daily_readings_ingestion`
  3. [ ] `daily_utilizations_ingestion`
  4. [ ] `daily_site_utilizations_ingestion`
  5. [ ] `daily_working_hours_ingestion`
- [ ] Connect activities with "On success" conditions
- [ ] Save pipeline

### Step 6: Schedule Pipeline
- [ ] Set schedule to daily at 2 AM (or preferred time)
- [ ] Set correct timezone
- [ ] Enable schedule
- [ ] Test run pipeline manually

## ✅ Validation

### Step 7: Verify Data
- [ ] Check all 14 tables exist in lakehouse
- [ ] Run row count validation query
- [ ] Check for duplicate records
- [ ] Verify date ranges in daily tables
- [ ] Confirm last run timestamps

### Step 8: Monitor First Scheduled Run
- [ ] Wait for first scheduled run
- [ ] Check pipeline run history
- [ ] Verify incremental loading worked
- [ ] Validate no duplicates added

## ✅ Optional: Power BI Dashboard

### Step 9: Create Semantic Model
- [ ] Create new semantic model from lakehouse
- [ ] Define table relationships
- [ ] Create DAX measures
- [ ] Test calculations

### Step 10: Build Dashboard
- [ ] Create new Power BI report
- [ ] Connect to semantic model
- [ ] Build visualizations
- [ ] Publish to workspace
- [ ] Set up automatic refresh

## 🔧 Quick Reference

### Important File Locations
```
notebooks/
├── ten_tables_ingestion.ipynb          ← Update API_TOKEN on line 23
├── daily_readings_ingestion.ipynb      ← Set endpoint on lines 31-32
├── daily_utilizations_ingestion.ipynb  ← Set endpoint on lines 31-32
├── daily_site_utilizations_ingestion.ipynb
└── daily_working_hours_ingestion.ipynb
```

### Key Configuration Lines

**Static tables notebook (ten_tables_ingestion.ipynb):**
```python
API_TOKEN = "YOUR_TOKEN"  # Line 23
```

**Daily tables notebooks:**
```python
API_TOKEN = "YOUR_TOKEN"   # Line 21
endpoint_name = "..."      # Line 31
table_name = "..."         # Line 32
```

### Validation Queries

**Check tables exist:**
```sql
SHOW TABLES FROM Tenna_Raw;
```

**Check row counts:**
```sql
SELECT 'assets' as table, COUNT(*) as rows FROM Tenna_Raw.assets
UNION ALL
SELECT 'asset_utilizations_daily', COUNT(*) FROM asset_utilizations_daily;
```

**Check for duplicates:**
```sql
SELECT asset_id, COUNT(*) 
FROM Tenna_Raw.assets 
GROUP BY asset_id 
HAVING COUNT(*) > 1;
```

**Check last run:**
```sql
SELECT * FROM pipeline_last_run ORDER BY last_run_time DESC;
```

**Monthly Charge:**
```sql
-- Pro-rated Monthly Charge Calculation
-- Replicates the 13, 8, 10 day split for January 2025
SELECT 
    asset_id,
    MonthYear,
    organization_department,
    days_on_job,
    -- Calculate Tier based on Total Days for the Asset in that Month
    CASE 
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 22 THEN 1.00
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 15 THEN 0.75
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 8  THEN 0.50
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 1  THEN 0.25
        ELSE 0 
    END AS billing_tier,
    -- Pro-rate the cost across multiple departments in one month
    (days_on_job / CAST(SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) AS FLOAT)) 
    * (internal_rental_rate * billing_tier) as monthly_charge
FROM ExpandedAssetBilling;
```

## 🚨 Common Issues

| Issue | Solution |
|-------|----------|
| Rate limit (429) errors | Automatic retry with backoff (already handled) |
| Schema mismatch | Use `overwriteSchema=true` option |
| Duplicates in daily tables | MERGE logic should prevent - check config |
| Missing incremental data | Verify `date_from` parameter in logs |
| Notebook timeout | Split large endpoints to Dataflow Gen2 |

## 📊 Expected Results

After successful setup:
- **14 total tables** (10 static + 4 daily)
- **2 tracking tables** (control_last_run, pipeline_last_run)
- **Daily automated refresh** at scheduled time
- **Zero duplicates** in all tables
- **Incremental loading** for daily tables (only new data)

## 🎯 Success Criteria

You're done when:
- ✅ All 14 tables exist with data
- ✅ Pipeline runs successfully on schedule
- ✅ No duplicates in any table
- ✅ Incremental loads working (check `pipeline_last_run`)
- ✅ Validation queries return expected results
- ✅ (Optional) Power BI dashboard displays data

## 📞 Need Help?

1. Check `SETUP_GUIDE.md` for detailed instructions
2. Review `sql/validation_queries.sql` for troubleshooting
3. Check Fabric pipeline run history for errors
4. Open GitHub issue if problem persists

---

**Estimated Setup Time:** 2-4 hours (mostly waiting for initial data load)
