import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# CONFIGURATION
# -----------------------------

API_KEY = "GQNEOFEJUWR9NPNU"                                            #replace with your real api key from Alpha-Vintage
#TICKERS = ['AAPL', 'MSFT', 'META', 'AMZN', 'TSLA', 'JPM', 'XOM']        #apple, microsoft, meta, amazon, tesla, jpmorgan, exxonMobil
TICKERS = ['AAPL']  
DAYS_BACK = 130                                                         #approx. 6 months

# -----------------------------
# Google Sheets Authentication
# -----------------------------

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# -----------------------------
# Fetch the Stock Data
# -----------------------------

def fetch_stock_data(symbol):                                           #calls the api using the symbol
    print(f"Fetching data for: {symbol}")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()                                              #converts json response into dataframe

    if "Time Series (Daily)" not in data:
        print(f"Error fetching data for {symbol}. Response:\n{data}")
        return None

    timeseries = data["Time Series (Daily)"]
    df = pd.DataFrame(timeseries).T                                     #sorts the data by date     
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)

    df = df.rename(columns={
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close",
        "5. volume": "Volume"
    })

    df = df[["Open", "High", "Low", "Close", "Volume"]]                 #selects just the required fields & adds a symbol column to identify the stock
    df["Symbol"] = symbol
    df.reset_index(inplace=True)
    df = df.rename(columns={"index": "Date"})

    # Filter by date range (last 130 calendar days)                     #ensures consistent window size for metrics
    cutoff = datetime.today() - timedelta(days=DAYS_BACK)
    df = df[df["Date"] >= cutoff]

    return df

# -----------------------------
# Update the Google Sheet
# -----------------------------

def update_google_sheet(ticker, df):                                    #opens the google sheets, clears any old data & uploads the new data with headers and rows
    try:
        df["Date"] = df["Date"].astype(str)                             #convert timestamp to string to prevent upload errors
        sheet = client.open(ticker).sheet1
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ Sheet updated for {ticker}")
    except Exception as e:
        print(f"❌ Failed to update {ticker}: {e}")

# -----------------------------
# MAIN
# -----------------------------

for ticker in TICKERS:
    df = fetch_stock_data(ticker)
    if df is not None:
        update_google_sheet(ticker, df)


#Result: For each of your 7 tickers, we get a fresh google sheet filled with 6 months of price data