# Tenna Lakehouse Data Platform - Microsoft Fabric

An automated data engineering solution built on Microsoft Fabric that centralizes equipment tracking data from 14 Tenna API endpoints into a lakehouse architecture with Delta tables, enabling real-time analytics and automated reporting.

## 🎯 Project Overview

This project delivers an end-to-end data platform that:
- Ingests data from 14 Tenna API endpoints (1,300 to 1.5M rows per endpoint)
- Stores all data as Delta tables in Microsoft Fabric OneLake
- Implements incremental loading with deduplication logic
- Provides automated refresh pipelines for zero-touch reporting
- Powers interactive Power BI dashboards for equipment analytics

## 🏗️ Architecture

```
Tenna API (14 endpoints)
    ↓
PySpark Ingestion Layer (Python + Dataflow Gen2)
    ↓
Delta Tables in OneLake
    ↓
Semantic Model (relationships + DAX measures)
    ↓
Power BI Dashboards
```

**Data Split Strategy:**
- **Python/PySpark**: 10 smaller endpoints (1,300 - 500,000 rows)
- **Dataflow Gen2**: 4 largest endpoints (500,000 - 1.5M rows) for optimized performance

## 📊 Data Sources

### Static Tables (Full Refresh)
1. `asset_financials` - Financial data, purchases, rentals, depreciation
2. `assets` - Core asset/equipment master data
3. `asset_assignee_history` - Equipment assignment tracking
4. `asset_label_associations` - Asset labeling and categorization
5. `asset_dt_codes` - Diagnostic trouble codes
6. `asset_labels` - Label definitions
7. `asset_organization_history` - Organizational changes
8. `asset_registrations` - Registration and compliance data
9. `asset_site_history` - Site location history
10. `asset_warranties` - Warranty information

### Daily Tables (Incremental Load)
11. `asset_readings_daily` - Daily equipment readings (hours, odometer)
12. `asset_utilizations_daily` - Daily utilization metrics
13. `asset_site_daily_utilizations` - Site-level daily utilization
14. `asset_net_working_hours_daily` - Net working hours calculations

## 🚀 Key Features

### 1. Reusable Ingestion Function
- Handles API pagination automatically
- Flattens nested JSON structures
- Type enforcement (timestamps, longs, booleans, doubles)
- Converts complex types to JSON strings
- Schema validation and cleanup

### 2. Incremental Loading with Deduplication
- Tracks last successful run time
- Uses `date_from` parameter for incremental loads
- MERGE operations prevent duplicates
- Composite key handling (e.g., `asset_id` + `date`)

### 3. Rate Limit Handling
- Exponential backoff retry logic
- Configurable retry attempts (default: 5)
- Automatic delay between requests
- Graceful degradation on persistent failures

### 4. Data Quality
- SQL validation queries for row counts
- Null value identification
- Schema correctness verification
- Duplicate detection post-load

### 5. Control Table Pattern
- `control_last_run` - Tracks pipeline execution metadata
- `pipeline_last_run` - Records incremental load checkpoints

## 📁 Repository Structure

```
├── notebooks/
│   ├── ten_tables_ingestion.ipynb       # Static tables (full refresh)
│   └── daily_ingestion_function.ipynb   # Daily tables (incremental)
├── config/
│   └── config_template.json             # Configuration template
├── sql/
│   └── validation_queries.sql           # Data quality checks
├── docs/
│   ├── architecture.md                  # Detailed architecture
│   └── setup_guide.md                   # Setup instructions
├── .gitignore
├── requirements.txt
└── README.md
```

## 🛠️ Technology Stack

- **Platform**: Microsoft Fabric
- **Data Storage**: OneLake (Delta Lake format)
- **Processing**: PySpark (Python)
- **ETL**: Dataflow Gen2 (for large endpoints)
- **Orchestration**: Fabric Data Pipelines
- **Visualization**: Power BI
- **API**: Tenna REST API v1

## ⚙️ Setup Instructions

### Prerequisites
- Microsoft Fabric workspace access
- Tenna API token with read permissions
- Python 3.8+ (for local development)

### Configuration

1. **Create Lakehouse**
   ```
   - Navigate to Fabric workspace
   - Create new Lakehouse named "Tenna_Raw"
   ```

2. **Set API Credentials**
   ```python
   # In your notebook, set:
   API_TOKEN = "your_tenna_api_token_here"
   BASE_URL = "https://api.tenna.com/v1"
   ```

3. **Upload Notebooks**
   - Import `ten_tables_ingestion.ipynb` for static tables
   - Import `daily_ingestion_function.ipynb` for daily tables

4. **Configure Pipeline**
   - Create Fabric Pipeline
   - Add notebook activities in sequence
   - Schedule daily refresh (recommended: 2 AM)

### First Run

```python
# Run ten_tables_ingestion.ipynb first (one-time setup)
# This creates all static tables and control tables

# Then run daily_ingestion_function.ipynb
# This handles incremental daily data
```

## 🔄 Automated Refresh Pipeline

The Fabric Pipeline orchestrates:

1. **Run Python Notebook** → Refresh static tables
2. **Refresh Dataflows** → Process large endpoints
3. **Refresh Semantic Model** → Update relationships/measures
4. **Sync to Power BI** → Publish updated dashboards

**Result**: 100% automated, zero-touch reporting

## 📊 Power BI Dashboard

> **⚠️ IMPORTANT:** The dashboard shown below uses **completely fictional data** created for demonstration purposes. The actual production dashboard contains sensitive company and client information that cannot be publicly shared.

![Equipment Fleet Analytics Dashboard](images/equipment-fleet-dashboard-overview.png)

This sample dashboard demonstrates the analytical capabilities and visual design delivered to stakeholders, including:

### Key Features:
- **Real-time KPI monitoring** - Fleet utilization, total hours, asset count, fleet value
- **Trend analysis** - Utilization patterns over time with target benchmarks
- **Asset performance** - Top performing equipment identification
- **Fleet composition** - Distribution by manufacturer and operational status
- **Interactive filtering** - Drill-down capabilities across all dimensions

### Technical Implementation:
- **Data Model:** Star schema with 2 fact tables and relationships
- **DAX Measures:** Custom calculations for utilization, aggregations, and KPIs
- **Visualizations:** Cards, line charts, bar charts, tables, and pie charts
- **Design:** Consistent color theming with professional styling

**📁 Full Power BI file (.pbix) and sample data available in the [`/powerbi/`](powerbi/) folder**

The fictional dataset includes:
- **100 equipment assets** across multiple manufacturers and types
- **6 months of daily utilization data** 
- **Multiple work sites** and operator assignments
- **Complete financial and operational metrics**

### Dashboard Preview Features:
1. **KPI Cards** - High-level metrics at-a-glance
2. **Utilization Trend Line Chart** - Historical performance with target line
3. **Manufacturer Distribution Bar Chart** - Fleet composition analysis  
4. **Top 10 Assets Table** - Performance leaderboard
5. **Status Pie Chart** - Operational vs maintenance breakdown

## 🧪 Data Quality Checks

Example validation queries:

```sql
-- Row count validation
SELECT COUNT(*) as total_rows FROM Tenna_Raw.assets;

-- Check for nulls in critical fields
SELECT COUNT(*) as null_count 
FROM Tenna_Raw.assets 
WHERE asset_id IS NULL;

-- Duplicate detection
SELECT asset_id, COUNT(*) as dup_count
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

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

## 🔍 Code Highlights

### Type Enforcement
```python
# Timestamp columns
datetime_cols=["created_at", "updated_at", "purchase_date"]

# Long integer columns
long_int_cols=["year", "cumulative_hours_end_of_day"]

# Boolean columns
boolean_cols=["billable", "ecu_hours_provided"]

# Double columns (percentages, rates)
double_cols=["utilization_percentage", "expected_run_hours"]
```

### Incremental Loading
```python
# Fetch only new data since last run
if last_run_time:
    params["date_from"] = max_date.strftime('%Y-%m-%d')
```

### Deduplication with MERGE
```python
delta_table.alias("target").merge(
    df.alias("source"),
    f"target.{merge_key} = source.{merge_key}"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

## 📊 Performance

- **Initial Load**: ~2-3 hours for all 14 endpoints
- **Daily Incremental**: ~10-15 minutes
- **Total Data Volume**: Millions of records across tables
- **Refresh Frequency**: Daily (automated)

## 🤝 Contributing

This is a demonstration project showcasing Microsoft Fabric capabilities. Feel free to adapt the patterns for your own use cases.

## 📝 License

MIT License - See LICENSE file for details

## 🙋 Questions?

For questions about this implementation approach, please open an issue or reach out via LinkedIn.

---

**Note**: This repository contains sanitized code with credentials removed. You'll need to configure your own API tokens and workspace details to run this code.
