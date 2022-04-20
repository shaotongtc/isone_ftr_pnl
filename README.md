This is a project to load data from ISONE's FTR auction result, price (Hourly Day-ahead LMP) and calculate the PnL for certain periods.

scripts:
1. scraper.py
    The script contains scrapers to scrape ISONE's website for FTR auction result as well as DA LMP.
2. db_initialize.py 
    The scripts to initialize a sqlite database and load static tables for iso hours, which will be utilized in the PnL calculation.
3. pnl.py 
    1) The functions to load auction data and price data into the sqlite database.
    2) The functions to calculate the monthly pnl based on settlement data (DALMP) and MTA (Mark to Auction) PnL.
4. util.py
    The script contains utility functions and is loaded in scraper.py and pnl.py
5. Sample Script.ipynb
    The sample script to use all the scripts to calculate & display the PnL.
