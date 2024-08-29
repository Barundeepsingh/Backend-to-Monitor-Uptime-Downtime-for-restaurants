from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import JSONResponse, FileResponse
from uuid import uuid4
import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime, timedelta
from pytz import timezone as pytz_timezone
import logging

app = FastAPI()

reports = {}

# Set the path to your Google Cloud service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/serviceAccountKey/loop-433908-95aa8c378820.json"
client = bigquery.Client(project="loop-433908")

logging.basicConfig(filename='report_debug.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

@app.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid4())
    reports[report_id] = "Running"
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    if report_id not in reports:
        return JSONResponse(status_code=404, content={"error": "Report Not Found"})

    status = reports[report_id]

    if status == "Running":
        return {"status": "Running"}
    elif status == "Complete":
        return FileResponse(f"reports/{report_id}.csv", media_type='text/csv')
    else:
        return JSONResponse(status_code=500, content={"error": "Unexpected Report Error"})

def generate_report(report_id: str):
    # Query to fetch all store data
    polling_query = """
    SELECT store_id, status, timestamp_utc 
    FROM `loop-433908.store_Details.store_Status`
    """
    polling_data = client.query(polling_query).to_dataframe()

    # Fetch unique store IDs
    store_ids = polling_data['store_id'].unique()
    
    # List to hold dataframes for each store
    all_store_reports = []

    for store_id in store_ids:
        store_polling_data = polling_data[polling_data['store_id'] == store_id]

        # Sort polling data by timestamp_utc in descending order
        store_polling_data = store_polling_data.sort_values(by='timestamp_utc', ascending=False)
        store_polling_data['timestamp_utc'] = pd.to_datetime(store_polling_data['timestamp_utc'])

        # Calculate uptime for the last hour in minutes
        latest_hour = store_polling_data['timestamp_utc'].max().floor('H')
        last_hour_start = latest_hour - timedelta(hours=1)
        last_hour_data = store_polling_data[
            (store_polling_data['timestamp_utc'] >= last_hour_start) & 
            (store_polling_data['timestamp_utc'] < latest_hour)
        ]
        
        if not last_hour_data.empty:
            uptime_last_hour = (last_hour_data['timestamp_utc'].max() - last_hour_data['timestamp_utc'].min()).total_seconds() / 60
        else:
            uptime_last_hour = 0
        
        downtime_last_hour = 60 - uptime_last_hour  # Downtime in minutes for last hour

        # Calculate uptime for the last 24 hours in hours
        last_24_hours = latest_hour - timedelta(hours=24)
        last_24_hours_data = store_polling_data[
            store_polling_data['timestamp_utc'] >= last_24_hours
        ]
        
        if not last_24_hours_data.empty:
            uptime_last_day = (last_24_hours_data['timestamp_utc'].max() - last_24_hours_data['timestamp_utc'].min()).total_seconds() / 3600
        else:
            uptime_last_day = 0
        
        downtime_last_day = 24 - uptime_last_day  # Downtime in hours for last 24 hours

        # Calculate uptime for the last 7 days in hours
        today = latest_hour.date()
        seven_days_ago = today - timedelta(days=6)
        last_7_days_data = store_polling_data[
            store_polling_data['timestamp_utc'].dt.date >= seven_days_ago
        ]

        if not last_7_days_data.empty:
            uptime_last_7_days = (last_7_days_data['timestamp_utc'].max() - last_7_days_data['timestamp_utc'].min()).total_seconds() / 3600
        else:
            uptime_last_7_days = 0

        downtime_last_7_days = 168 - uptime_last_7_days  # Downtime in hours for last 7 days

        # Prepare data for CSV
        uptime_downtime_data = {
            "store_id": [store_id],
            "uptime_last_hour(in minutes)": [uptime_last_hour],
            "uptime_last_day(in hours)": [uptime_last_day],
            "uptime_last_week(in hours)": [uptime_last_7_days],
            "downtime_last_hour(in minutes)": [downtime_last_hour],
            "downtime_last_day(in hours)": [downtime_last_day],
            "downtime_last_week(in hours)": [downtime_last_7_days]
        }

        uptime_downtime_df = pd.DataFrame(uptime_downtime_data)
        all_store_reports.append(uptime_downtime_df)

    # Concatenate all store reports into one dataframe
    final_report_df = pd.concat(all_store_reports, ignore_index=True)

    # Save the uptime and downtime data to a CSV file
    final_report_df.to_csv(f"reports/{report_id}.csv", index=False)

    # Mark the report as complete
    reports[report_id] = "Complete"
