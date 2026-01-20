"""
Angular Sheets Dashboard Backend
Fetches data from Google Sheets and serves it via FastAPI endpoints.


Run the app with: uvicorn main:app --reload
"""

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
from fastapi import FastAPI
import os
import json
from fastapi.middleware.cors import CORSMiddleware
import re

# Debug mode - set to False to disable debug print statements
debug = True


# =====================================================
# Google Sheets API Configuration
# =====================================================

# API scopes - read-only access to spreadsheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# ID of the target Google Spreadsheet
SPREADSHEET_ID = "1puAH0mkBse1TjuZBHcY2kAjPpymOKNbLqkDmVhnJ4qA"

# Initialize Google Sheets API service
try:
    creds_json = os.environ.get("GOOGLE_CREDS")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDS environment variable not set")
    creds_dict = json.loads(creds_json)

    # Load credentials from service account file
    creds = Credentials.from_service_account_info(
        creds_dict, 
        scopes=SCOPES
    )
    if debug:
        print(f"DEBUG: Authenticated as {creds.service_account_email}")
    
    # Build the Google Sheets API service
    service = build("sheets", "v4", credentials=creds)
    if debug:
        print("DEBUG: Google Sheets API service initialized successfully")
except Exception as e:
    print(f"ERROR: Failed to initialize Google Sheets API - {str(e)}")
    raise

# ====================================================
# Fetch Role Map
# ====================================================
def get_role_map():

    try:
        ROLE_MAP = pd.read_csv("roles.csv").set_index("role")["team"].to_dict()
        if debug:
            print(f"DEBUG: Loaded role map from roles.csv - {ROLE_MAP}")
    except Exception as e:
        print(f"ERROR: Failed to load role map from roles.csv - {str(e)}")
        ROLE_MAP = {}

    return ROLE_MAP



# =====================================================
# Helper Functions - Google Sheets Data Fetching
# =====================================================

def fetch_sheet(title):
    """
    Fetch data from a specific sheet in the Google Spreadsheet.
    
    Args:
        title (str): The name/title of the sheet to fetch
        
    Returns:
        list: 2D list of values from the sheet, or empty list if no data
        
    Raises:
        Exception: If the API request fails
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=title
        ).execute()
        
        values = result.get("values", [])
        if debug:
            print(f"DEBUG: Successfully fetched sheet '{title}' - {len(values)} rows")
        
        return values
        
    except Exception as e:
        print(f"ERROR: Failed to fetch sheet '{title}' - {str(e)}")
        raise

def sheet_to_df(values):
    """
    Convert raw Google Sheets values into a cleaned Pandas DataFrame.
    Assumes:
    A = date
    B = role
    C = win (1/0)
    D = winrate (ignored)
    """
    if not values or len(values) < 2:
        return pd.DataFrame(columns=["date", "role", "win"])

    # First row is header (but we ignore Google's headers anyway)
    rows = values[1:]

    df = pd.DataFrame(rows, columns=["date", "role", "win", "winrate"])

    # Omit Winrate column
    df = df[["date", "role", "win"]]

    # Clean types
    df["win"] = pd.to_numeric(df["win"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Drop rows where the player didn't play
    df = df.dropna(subset=["role", "win"])

    return df


# =====================================================
# FastAPI Application & Endpoints
# =====================================================

# Initialize FastAPI application
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "https://sheets-dashboard-angular.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health():
    """
    Health check endpoint to verify the API is running.
    
    Returns:
        dict: Status indicator
    """
    if debug:
        print("DEBUG: Health check endpoint called")
    return {"status": "ok"}


@app.get("/sheets")
def get_all_sheets():
    """
    Fetch all sheets from the Google Spreadsheet and return their data.
    Retrieves metadata to identify all sheets, then fetches data from each.
    
    Returns:
        dict: Dictionary with sheet titles as keys and their data as values
        
    Raises:
        Exception: If metadata retrieval or data fetching fails
    """
    try:
        if debug:
            print("DEBUG: Fetching all sheets data")
        
        # Fetch spreadsheet metadata to get list of all sheets
        try:
            metadata = service.spreadsheets().get(
                spreadsheetId=SPREADSHEET_ID
            ).execute()
            if debug:
                print("DEBUG: Successfully retrieved spreadsheet metadata")
                
        except Exception as e:
            print(f"ERROR: Failed to retrieve spreadsheet metadata - {str(e)}")
            raise
        
        # Extract sheet information (title and ID)
        sheets = metadata.get("sheets", [])
        sheet_info = [
            {
                "title": s["properties"]["title"],
                "sheetId": s["properties"]["sheetId"]
            }
            for s in sheets
        ]
        
        if debug:
            print(f"DEBUG: Found {len(sheet_info)} sheets: {[s['title'] for s in sheet_info]}")
        
        # Fetch data from each sheet
        all_data = {}
        
        for sheet in sheet_info:
            try:
                raw_data = fetch_sheet(sheet["title"])
                df = sheet_to_df(raw_data)

                # ---- Calculations ----
                games_played = len(df)
                overall_winrate = df["win"].mean() if games_played > 0 else 0

                winrate_by_role = (
                    df.groupby("role")["win"]
                    .agg(games="count", winrate="mean")
                    .reset_index()
                )

                # ---- Build response ----
                all_data[sheet["title"]] = {
                    "summary": {
                        "games_played": games_played,
                        "overall_winrate": round(overall_winrate, 3)
                    },
                    "by_role": winrate_by_role.to_dict(orient="records"),
                    "log": df.to_dict(orient="records")
                }

            except Exception as e:
                print(f"ERROR: Skipping sheet '{sheet['title']}' due to error - {str(e)}")
                all_data[sheet["title"]] = []
        
        if debug:
            print(f"DEBUG: Successfully fetched data from all {len(all_data)} sheets")
        
        return all_data
        
    except Exception as e:
        print(f"ERROR: Failed to fetch all sheets - {str(e)}")
        raise








