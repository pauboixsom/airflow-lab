import yfinance as yf
import sys

def main():
    tsla = yf.Ticker('TSLA')
    args = sys.argv[1:]
    print("Interval start: ")
    print (args[0])
    print(" Interval end: ")
    print (args[1])
    print(tsla.recommendations["To Grade"].value_counts().keys()[0])

if __name__ == "__main__":
    main()