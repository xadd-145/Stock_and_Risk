import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#1.Configuration
API_KEY = "#Replace with your real api key from Alpha-Vintage"
TICKERS = ['AAPL', 'MSFT', 'META', 'AMZN', 'TSLA', 'JPM', 'XOM']
DAYS_BACK = 130
SHEET_NAME = "Stock_Data_Master"

#2.Google Sheets Authentication
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)     
client = gspread.authorize(creds)                      #Loads credentials.json and authorizes your script to act as your service account

#3.Fetch + Clean Function
def fetch_and_clean(symbol):
    url = "https://www.alphavantage.co/query"       #Calls the api using the symbol
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": API_KEY
    }
    r = requests.get(url, params=params)
    data = r.json().get("Time Series (Daily)", {})

    if not data:
        print(f"❌ No data for {symbol}")
        return None

    #convert and clean the dataframe
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

    #resets the index & renames to date. keep filter out. 
    df.reset_index(inplace=True)
    df = df.rename(columns={"index": "Date"})

    #filter to last 130 calendar days
    cutoff = datetime.today() - timedelta(days=DAYS_BACK)
    df = df[df["Date"] >= cutoff]

    #clean format
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df = df.round(2)

    return df

#4.Merge All
final_df = pd.DataFrame()
for ticker in TICKERS:
    df = fetch_and_clean(ticker)
    if df is not None:
        final_df = pd.concat([final_df, df], ignore_index=True)

#5.Push to Google Sheet
try:
    sheet = client.open(SHEET_NAME).sheet1
    sheet.clear()
    sheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    print(f"✅ Merged data written to '{SHEET_NAME}'")
except Exception as e:
    print(f"❌ Error writing to sheet: {e}")
