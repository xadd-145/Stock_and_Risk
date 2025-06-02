# Import all required libraries and Airflow decorators
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# 1. CONFIGURATION SECTION
# -----------------------------
API_KEY = "GQNEOFEJUWR9NPNU"  # Your Alpha Vantage API key
TICKERS = ['AAPL', 'MSFT', 'META', 'AMZN', 'TSLA', 'JPM', 'XOM']  # Stocks to fetch
DAYS_BACK = 130  # Days of data to retain
SHEET_NAME = "Stock_Data_Master"  # Google Sheet to update

# -----------------------------
# 2. GOOGLE SHEETS AUTH FUNCTION
# -----------------------------
def authorize_gsheets():
    """
    Authorizes your script using credentials.json to act as the service account
    and access Google Sheets API.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)

# -----------------------------
# 3. DAG METADATA & TIMING
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # If fails, retry in 5 mins
}

# DAG is scheduled daily at 11:00 AM starting Jan 1, 2025, and will not backfill older runs
@dag(
    schedule="0 11 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["stocks"]
)
def stock_data_pipeline():

    # -----------------------------
    # 4. TASK: FETCH + CLEAN FUNCTION
    # -----------------------------
    @task()
    def fetch_and_clean(symbol):
        """
        Fetches daily stock data for one symbol from Alpha Vantage API,
        cleans and reformats it for uploading to Google Sheets.
        """
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "full",
            "apikey": API_KEY
        }

        # API request and JSON parsing
        r = requests.get(url, params=params)
        data = r.json().get("Time Series (Daily)", {})

        if not data:
            print(f"❌ No data for {symbol}")
            return None

        # Convert to dataframe and reformat
        df = pd.DataFrame(data).T
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True)
        df = df.rename(columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume"
        })
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        df["Symbol"] = symbol

        # Reset index, rename to Date
        df.reset_index(inplace=True)
        df = df.rename(columns={"index": "Date"})

        # Filter rows for only last `DAYS_BACK` calendar days
        cutoff = datetime.today() - timedelta(days=DAYS_BACK)
        df = df[df["Date"] >= cutoff]

        # Format and round
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        df = df.round(2)

        return df

    # -----------------------------
    # 5. TASK: MERGE + PUSH FUNCTION
    # -----------------------------
    @task()
    def merge_and_upload(dfs):
        """
        Merges cleaned stock data from all tickers and pushes to Google Sheets.
        """
        # Combine dataframes (skipping None)
        combined_df = pd.concat([df for df in dfs if df is not None], ignore_index=True)

        try:
            client = authorize_gsheets()
            sheet = client.open(SHEET_NAME).sheet1
            sheet.clear()  # Clears old data
            sheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
            print(f"✅ Merged data written to '{SHEET_NAME}'")
        except Exception as e:
            print(f"❌ Error writing to sheet: {e}")

    # -----------------------------
    # 6. RUN TASKS IN SEQUENCE
    # -----------------------------
    # Step 1: Run fetch_and_clean() for each stock
    cleaned_data = [fetch_and_clean.override(task_id=f"fetch_{ticker.lower()}")(ticker) for ticker in TICKERS]

    # Step 2: Merge all and upload
    merge_and_upload(cleaned_data)

# Required to register the DAG in Airflow
stock_data_pipeline = stock_data_pipeline()
