from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# 1. CONFIGURATION
# -----------------------------

API_KEY = "GQNEOFEJUWR9NPNU"  # Your Alpha Vantage API key
TICKERS = ['AAPL', 'MSFT', 'META', 'AMZN', 'TSLA', 'JPM', 'XOM']  # Stock tickers
DAYS_BACK = 130  # approx. 6 months

# -----------------------------
# 2. GOOGLE SHEETS AUTH FUNCTION
# -----------------------------

def authorize_gsheets():
    """
    Authorize access to Google Sheets using credentials.json
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    return gspread.authorize(creds)

# -----------------------------
# 3. DAG CONFIGURATION
# -----------------------------

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    schedule="@once",  # Runs only once when triggered manually
    start_date=datetime(2025, 1, 1),  # Start date for DAG
    catchup=False,
    default_args=default_args,
    tags=["stocks", "trial"]
)
def stock_data_trial_dag():

    # -----------------------------
    # 4. FETCH STOCK DATA
    # -----------------------------

    @task()
    def fetch_stock_data(symbol):
        """
        Calls the Alpha Vantage API and returns stock data for a symbol
        """
        print(f"📥 Fetching data for: {symbol}")
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": API_KEY
        }

        response = requests.get(url, params=params)
        data = response.json()

        if "Time Series (Daily)" not in data:
            print(f"❌ Error fetching data for {symbol}. Response:\n{data}")
            return None

        timeseries = data["Time Series (Daily)"]
        df = pd.DataFrame(timeseries).T
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
        df.reset_index(inplace=True)
        df = df.rename(columns={"index": "Date"})

        # Filter by date range (last 130 days)
        cutoff = datetime.today() - timedelta(days=DAYS_BACK)
        df = df[df["Date"] >= cutoff]

        return df

    # -----------------------------
    # 5. UPDATE GOOGLE SHEET
    # -----------------------------

    @task()
    def update_google_sheet(ticker, df):
        """
        Updates the Google Sheet with the fetched stock data
        """
        try:
            df["Date"] = df["Date"].astype(str)
            client = authorize_gsheets()
            sheet = client.open(ticker).sheet1
            sheet.clear()
            sheet.update([df.columns.values.tolist()] + df.values.tolist())
            print(f"✅ Sheet updated for {ticker}")
        except Exception as e:
            print(f"❌ Failed to update {ticker}: {e}")

    # -----------------------------
    # 6. TASK FLOW FOR ALL TICKERS
    # -----------------------------

    for ticker in TICKERS:
        data = fetch_stock_data.override(task_id=f"fetch_{ticker.lower()}")(ticker)
        update_google_sheet.override(task_id=f"update_{ticker.lower()}")(ticker, data)

# -----------------------------
# 7. INSTANTIATE THE DAG
# -----------------------------

stock_data_trial_dag = stock_data_trial_dag()
