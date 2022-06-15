import sqlite3
import pandas as pd
#create the database
conn = sqlite3.connect('ftr.db')
#create a cursur
cursor = conn.cursor()

print("dropping tables")
cursor.execute("drop table if exists DailyHours")
cursor.execute("drop table if exists MonthlyHours")
cursor.execute("drop table if exists FtrAuctionResult")
cursor.execute("drop table if exists IsoneDailyDaLmp")

print("Loading Hours tables")
DailyHours = pd.read_csv('DailyHours.csv')
DailyHours.to_sql("DailyHours",con = conn,index = False)
MonthlyHours = pd.read_csv('MonthlyHours.csv')
MonthlyHours.to_sql("MonthlyHours",con = conn,index = False)


