endpoint_name = ""
table_name = ""

import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta

from pyspark.sql.functions import col, lit, to_json, base64
from pyspark.sql.types import (
    StructType, MapType, ArrayType, BinaryType,
    DecimalType, NullType, DateType, TimestampType, LongType,
    StructField, StringType, DoubleType
)
from delta.tables import DeltaTable

API_TOKEN = "TENNA_API_TOKEN_HERE"  
BASE_URL = "https://api.tenna.com/v1"
LIMIT = 100
MAX_RETRIES = 5
RETRY_DELAY = 60

endpoint_configs = {
    "asset-readings-daily": {
        "datetime_cols": ["date"],
        "long_int_cols": ["cumulative_hours_end_of_day", "cumulative_km_end_of_day"],
        "boolean_cols": ["ecu_hours_provided", "ecu_odometer_provided", "ignore_ecu_hours", "ignore_ecu_odometer"],
        "merge_key": "asset_reading_daily_id"
    },
    "asset-utilizations-daily": {
        "datetime_cols": ["date", "created_at", "updated_at"],
        "long_int_cols": ["active_run_seconds", "asset_target_work_hours", "asset_target_work_miles"],
        "boolean_cols": ["asset_billable"],
        "merge_key": None  # Will use asset_id + date
    },
 "asset-site-daily-utilizations": {
        "datetime_cols": ["date", "created_at", "updated_at"],
        "long_int_cols": ["active_run_seconds", "asset_year", "asset_target_work_hours"],
        "boolean_cols": [],
        "merge_key": "asset_site_daily_utilization_id"
    },
    "asset-net-working-hours-daily": {
        "datetime_cols": ["date", "created_at", "updated_at"],
        "long_int_cols": ["total_duration_seconds", "total_idle_duration_seconds", "working_seconds", "avg_daily_idle_seconds"],
        "boolean_cols": [],
        "merge_key": "asset_net_working_hour_daily_id"
    }
}

config = endpoint_configs.get(endpoint_name, {
    "datetime_cols": [],
    "long_int_cols": [],
    "boolean_cols": [],
    "merge_key": None
})

print(f"\n{'='*70}")
print(f"Processing: {endpoint_name}")
print(f"Target table: {table_name}")
print(f"{'='*70}\n")

last_run_time = None
try:
    last_run_df = spark.sql(f"SELECT last_run_time FROM pipeline_last_run WHERE endpoint_name = '{endpoint_name}' ORDER BY last_run_time DESC LIMIT 1")
    if last_run_df.count() > 0:
        last_run_time = last_run_df.collect()[0]['last_run_time']
        print(f"Last successful run: {last_run_time}")
        print(f"Using incremental loading - only fetching updated data")
except:
    print("No previous run found - fetching all data (initial load)")

url = f"{BASE_URL}/{endpoint_name}"
headers = {"Authorization": f"Bearer {API_TOKEN}"}

offset = 0
all_records = []
consecutive_429_errors = 0

print(f"\nStarting data fetch...")

while True:
    print(f" → Fetching offset {offset} ... ", end="")
    
    params = {
        "limit": LIMIT,
        "offset": offset
    }
    
    if last_run_time:
        try:
            table_simple_name = table_name.split('.')[-1]
            max_date = spark.sql(f"SELECT MAX(date) as max_date FROM {table_simple_name}").collect()[0]['max_date']
            
            if max_date:
                params["date_from"] = max_date.strftime('%Y-%m-%d')
                print(f"Incremental: Getting from {max_date.strftime('%Y-%m-%d')} (last date in table)")
            else:
                params["date_from"] = last_run_time.strftime('%Y-%m-%d')
                print(f"Incremental: Getting from {last_run_time.strftime('%Y-%m-%d')}")
        except:
            print("Initial load - no date filter")
    
    retry_count = 0
    success = False
    page = [] 
    
    while retry_count < MAX_RETRIES and not success:
        try:
	    r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            page = r.json().get("results", [])
            print(f"{len(page)} rows")
            
            if not page:
                success = True
                break
            
            all_records.extend(page)
            offset += LIMIT
            consecutive_429_errors = 0
            success = True
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_count += 1
                consecutive_429_errors += 1
                
                if retry_count < MAX_RETRIES:
                    wait_time = RETRY_DELAY * (2 ** (retry_count - 1))
                    print(f"\nRate limit hit (attempt {retry_count}/{MAX_RETRIES})")
                    print(f"   Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    print(f"   Retrying offset {offset}...")
                else:
                    print(f"\n Max retries reached at offset {offset}")
                    print(f"Continuing with {len(all_records)} records downloaded so far")
                    success = True
                    break
            else:
                print(f"\n HTTP Error: {e}")
                success = True
                break
                
        except Exception as e:
	    print(f"\n Error: {e}")
            success = True
            break
    
    if len(page) == 0 or (retry_count >= MAX_RETRIES and consecutive_429_errors > 0):
        break
    
    time.sleep(0.5)

print(f"\nDownload complete: {len(all_records)} records\n")

if len(all_records) == 0:
    print("No new or updated data found.")
    print("Notebook finished - tables are up to date")
else:

    print("Checking for duplicates in downloaded data...")
    
    pdf = pd.DataFrame(all_records)
    original_count = len(pdf)
    
    if config["merge_key"] and config["merge_key"] in pdf.columns:
        pdf = pdf.drop_duplicates(subset=[config["merge_key"]], keep='last')
        print(f"Removed {original_count - len(pdf)} duplicates based on {config['merge_key']}")
    else:
        if 'asset_id' in pdf.columns and 'date' in pdf.columns:
            pdf = pdf.drop_duplicates(subset=['asset_id', 'date'], keep='last')
            print(f"Removed {original_count - len(pdf)} duplicates based on asset_id + date")
        else:
            pdf = pdf.drop_duplicates(keep='last')
            print(f"Removed {original_count - len(pdf)} exact duplicates")
    
    print(f"Columns loaded: {len(pdf.columns)}")
 for c in pdf.columns:
        if pdf[c].apply(lambda x: isinstance(x, (list, dict))).any():
            pdf[c] = pdf[c].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        if pdf[c].isnull().all():
            pdf[c] = ""
    
    print("Pandas cleanup done\n")
    
    df = spark.createDataFrame(pdf)
    
    print("Cleaning schema...")
  
    for c in config["datetime_cols"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(TimestampType()))
    
    for c in config["long_int_cols"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(LongType()))
    
    for c in config["boolean_cols"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("boolean"))
    
    numeric_double_fields = [
        "utilization_percentage",
        "expected_run_hours",
        "asset_external_rental_rate",
        "asset_internal_rental_rate",
        "asset_target_work_hours",
        "asset_target_work_miles",
        "total_idle_fuel_usage_liters",
        "working_percentage",
        "idle_percentage"
    ]   

for c in numeric_double_fields:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    print("Numeric fields cast to double")
    
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
    
    table_simple_name = table_name.split('.')[-1]
    df.cache()
    
    print(f"\nSaving data to table: {table_simple_name}")
    
    try:
        table_exists = spark.catalog.tableExists(table_simple_name)
        
        if table_exists and config["merge_key"]:
            print(f"Table exists - using MERGE to prevent duplicates")
            
            delta_table = DeltaTable.forName(spark, table_simple_name)
            merge_key = config["merge_key"]

delta_table.alias("target").merge(
                df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()
            
            print(f"MERGED {len(pdf)} records (updated existing + inserted new)")
            
        elif table_exists and not config["merge_key"]:
            print(f"Table exists - using MERGE with composite key")
            
            delta_table = DeltaTable.forName(spark, table_simple_name)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.asset_id = source.asset_id AND target.date = source.date"
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()
            
            print(f"MERGED {len(pdf)} records")
            
        else:
            print(f"Creating new table (initial load)")
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_simple_name)
            print(f"Created table with {len(pdf)} records")
        
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_simple_name}").collect()[0]['cnt']
        
        if config["merge_key"]:
            dup_check = spark.sql(f"""
                SELECT COUNT(*) - COUNT(DISTINCT {config['merge_key']}) as duplicates
                FROM {table_simple_name}
            """).collect()[0]['duplicates']
        else:
            dup_check = spark.sql(f"""
                SELECT COUNT(*) - COUNT(DISTINCT asset_id, date) as duplicates
                FROM {table_simple_name}

 """).collect()[0]['duplicates']
        else:
            dup_check = spark.sql(f"""
                SELECT COUNT(*) - COUNT(DISTINCT asset_id, date) as duplicates
                FROM {table_simple_name}
            """).collect()[0]['duplicates']
        
        print(f"\n{'='*70}")
        print(f"SUCCESS: {table_simple_name}")
        print(f"  - Records processed: {len(pdf):,}")
        print(f"  - Total rows in table: {row_count:,}")
        print(f"  - Duplicates in table: {dup_check}")
        print(f"  - Columns: {len(df.columns)}")
        print(f"{'='*70}\n")
        
        if dup_check > 0:
            print(f"WARNING: Found {dup_check} duplicates in table!")
            print(f"   This shouldn't happen with MERGE logic")
        
        last_run_schema = StructType([
            StructField("endpoint_name", StringType(), False),
            StructField("last_run_time", TimestampType(), False),
            StructField("records_processed", LongType(), False)
        ])
        
        last_run_data = [(endpoint_name, datetime.now(), len(pdf))]
        last_run_df = spark.createDataFrame(last_run_data, schema=last_run_schema)
        
        last_run_df.write.format("delta").mode("append").saveAsTable("pipeline_last_run")
        print(f"Updated last run tracker for {endpoint_name}")
        
    except Exception as e:
        print(f"\n ERROR saving table: {e}")
        import traceback
        traceback.print_exc()
        raise

print("\n Notebook completed")















