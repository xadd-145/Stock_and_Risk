import yfinance as yf

apple = yf.Ticker("AAPL")
data = apple.history(period="5d")
print(data)
