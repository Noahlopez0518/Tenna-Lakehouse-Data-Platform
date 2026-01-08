import requests
import json
import pandas as pd
from datetime import datetime

from pyspark.sql.functions import col, lit, to_json, base64
from pyspark.sql.types import (
    StructType, MapType, ArrayType, BinaryType,
    DecimalType, NullType, DateType, TimestampType, LongType,
    StructField, StringType
)

API_TOKEN = "TENNA_API_TOKEN_HERE"
BASE_URL = "https://api.tenna.com/v1"
LIMIT = 100

print("\n" + "="*70)
print("INITIALIZING CONTROL TABLE")
print("="*70)

control_schema = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("last_run_time", TimestampType(), False),
    StructField("status", StringType(), False)
])

try:
    spark.sql("SELECT * FROM Tenna_Raw.control_last_run LIMIT 1")
    print("Control table already exists")
except:
    print("Creating control_last_run table...")
    control_df = spark.createDataFrame([
        ("tenna_asset_tables", datetime.now(), "initialized")
    ], schema=control_schema)
    
    control_df.write.format("delta").mode("overwrite").saveAsTable("Tenna_Raw.control_last_run")
    print("Control table created")

def download_and_save_endpoint(endpoint_name, table_name, datetime_cols=[], long_int_cols=[], boolean_cols=[]):
    """
    Downloads data from Tenna API endpoint and saves to lakehouse table
    
    Args:
        endpoint_name: API endpoint (e.g., "assets", "asset-financials")
        table_name: Table name to save to (e.g., "Tenna_Raw.assets")
        datetime_cols: List of columns to cast as timestamp
        long_int_cols: List of columns to cast as long integer
        boolean_cols: List of columns to cast as boolean
    """
    
    print(f"\n{'='*70}")
    print(f"Processing: {endpoint_name}")
    print(f"Target table: {table_name}")
    print(f"{'='*70}\n")
    
    url = f"{BASE_URL}/{endpoint_name}"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    
    offset = 0
    all_records = []
    
    while True:
        print(f" → Fetching offset {offset} ... ", end="")
        r = requests.get(url, headers=headers, params={"limit": LIMIT, "offset": offset})
        r.raise_for_status()
        page = r.json().get("results", [])
        print(f"{len(page)} rows")
        
        if not page:
            break
        
        all_records.extend(page)
        offset += LIMIT
    
    print(f"\n Download complete: {len(all_records)} records\n")

 pdf = pd.DataFrame(all_records)
    print(f"Columns loaded: {len(pdf.columns)}")
    
    for c in pdf.columns:
        if pdf[c].apply(lambda x: isinstance(x, (list, dict))).any():
            pdf[c] = pdf[c].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        if pdf[c].isnull().all():
            pdf[c] = ""
    
    print("Pandas cleanup done\n")
    
    df = spark.createDataFrame(pdf)
    
    print("Cleaning schema...")
    
    for c in datetime_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(TimestampType()))
    
    for c in long_int_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(LongType()))
    
    for c in boolean_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("boolean"))
    
    null_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "null"]
    for c in null_cols:
        df = df.withColumn(c, lit("").cast("string"))
    
    for f in df.schema.fields:
        if isinstance(f.dataType, (StructType, MapType, ArrayType)):
            df = df.withColumn(f.name, to_json(col(f.name)))
    
    for f in df.schema.fields:
        if isinstance(f.dataType, BinaryType):
            df = df.withColumn(f.name, base64(col(f.name)).cast("string"))
    
    for f in df.schema.fields:

       if isinstance(f.dataType, DecimalType):
            df = df.withColumn(f.name, col(f.name).cast("double"))
    
    allowed_simple = {"string", "bigint", "double", "boolean", "date", "timestamp", "int", "long", "float"}
    for f in df.schema.fields:
        if f.dataType.simpleString() not in allowed_simple:
            df = df.withColumn(f.name, col(f.name).cast("string"))
    
    print(f"Saving to table: {table_name}")
    df.cache()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    
    print(f"SUCCESS: {table_name}")
    print(f"  - Rows: {len(all_records):,}")
    print(f"  - Columns: {len(df.columns)}")
    print()
    
    return df

print("\n" + "="*70)
print("STARTING TENNA API DATA PIPELINE - ALL 13 TABLES")
print("="*70)

# 1. Asset Financials
df_financials = download_and_save_endpoint(
    endpoint_name="asset-financials",
    table_name="Tenna_Raw.asset_financials",
    datetime_cols=["purchase_date", "created_at", "updated_at", "last_reading_date", 
                   "rental_start", "rental_actual_end", "rental_forecasted_end", 
                   "rpo_expiration", "sold_date", "insurance_expiration_date", 
                   "insurance_start_date"],
    long_int_cols=["difference_labor_cost", "disposition_broker_fee", "hauling_cost", 
                   "remaining_days", "loan_term_in_months", "lease_term_in_months"],
    boolean_cols=["rpo_option"]
)

# 2. Assets
df_assets = download_and_save_endpoint(
    endpoint_name="assets",
    table_name="Tenna_Raw.assets",
    datetime_cols=["created_at", "updated_at", "tracker_last_data_received", 
                   "tracker_installed_at", "tracker_verification_date"],
    long_int_cols=["year", "engine_year", "dimensions_tire_quantity", 
                   "tracker_days_since_last_location_report"],
    boolean_cols=["billable", "ifta", "vehicle_type_hazmat", "vehicle_type_apportioned",
                  "eld_enabled", "tracker_associated", "ignore_ecu_idle_hours"]
)

# 3. Asset Assignee History
df_assignee_history = download_and_save_endpoint(
    endpoint_name="asset-assignee-history",
    table_name="Tenna_Raw.asset_assignee_history",
    datetime_cols=["assignment_start", "assignment_end", "updated_at"],
    long_int_cols=[],
    boolean_cols=["currently_assigned"]
)

# 4. Asset Label Associations
df_label_associations = download_and_save_endpoint(
    endpoint_name="asset-label-associations",
    table_name="Tenna_Raw.asset_label_associations",
    datetime_cols=["associated_at", "removed_at", "updated_at", "asset_label_created_at"],
    long_int_cols=["asset_label_order"],
    boolean_cols=["currently_applied", "asset_label_deactivated"]
)

# 5. Asset DT Codes (Diagnostic Trouble Codes)
df_dt_codes = download_and_save_endpoint(
    endpoint_name="asset-dt-codes",
    table_name="Tenna_Raw.asset_dt_codes",
    datetime_cols=["timestamp", "acknowledged_at", "created_at", "updated_at"],
    long_int_cols=[],
    boolean_cols=["is_acknowledged"]
)

# 6. Asset Labels
df_labels = download_and_save_endpoint(
    endpoint_name="asset-labels",
    table_name="Tenna_Raw.asset_labels",
    datetime_cols=["deactivated_at", "created_at", "updated_at"],
    long_int_cols=["order"],
    boolean_cols=["deactivated"]
)

# 7. Asset Organization History
df_org_history = download_and_save_endpoint(
    endpoint_name="asset-organization-history",
    table_name="Tenna_Raw.asset_organization_history",
    datetime_cols=["from", "to", "updated_at"],
    long_int_cols=[],
    boolean_cols=["currently_applied"]
)

# 8. Asset Registrations
df_registrations = download_and_save_endpoint(
    endpoint_name="asset-registrations",
    table_name="Tenna_Raw.asset_registrations",
    datetime_cols=["expiration_date", "updated_at"],
    long_int_cols=[],
    boolean_cols=[]
)

# 9. Asset Site History
df_site_history = download_and_save_endpoint(
    endpoint_name="asset-site-history",
    table_name="Tenna_Raw.asset_site_history",
    datetime_cols=["enter_date", "exit_date", "updated_at"],
    long_int_cols=[],
    boolean_cols=["currently_applied"]
)
# 10. Asset Warranties
df_warranties = download_and_save_endpoint(
    endpoint_name="asset-warranties",
    table_name="Tenna_Raw.asset_warranties",
    datetime_cols=["start_date", "end_value_date", "updated_at"],
    long_int_cols=[],
    boolean_cols=["expired"]
)

print("\n" + "="*70)
print("UPDATING CONTROL TABLE")
print("="*70)

control_update = spark.createDataFrame([
    ("tenna_asset_tables", datetime.now(), "success")
], schema=control_schema)

control_update.write.format("delta").mode("overwrite").saveAsTable("Tenna_Raw.control_last_run")

print(f"Pipeline completed successfully at {datetime.now()}")

print("\n" + "="*70)
print("PIPELINE COMPLETE - ALL 10 TABLES CREATED")
print("="*70)
print("Tables created:")
print("  1. Tenna_Raw.asset_financials")
print("  2. Tenna_Raw.assets")
print("  3. Tenna_Raw.asset_assignee_history")
print("  4. Tenna_Raw.asset_label_associations")
print("  5. Tenna_Raw.asset_dt_codes")
print("  6. Tenna_Raw.asset_labels")
print("  7. Tenna_Raw.asset_organization_history")
print("  8. Tenna_Raw.asset_registrations")
print("  9. Tenna_Raw.asset_site_history")
print(" 10. Tenna_Raw.asset_warranties")
print("\nControl table:")
print(" 11. Tenna_Raw.control_last_run")
print("="*70)