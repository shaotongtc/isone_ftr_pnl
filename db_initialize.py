import sqlite3
#create the database
conn = sqlite3.connect('ftr.db')
#create a cursur
cursor = conn.cursor()
#create tables
#auction result
'''
create_auction_result_query = '''
create table FtrAuctionResult (
"Auction Name" varchar(15),
"Customer ID" int,
"Customer Name" varchar(30),
"Source Location ID" int,
"Source Location Name" varchar(30),
"Source Location Type" varchar(30),
"Sink Location ID" int,
"Sink Location Name" varchar(30),
"Sink Location Type" varchar(30),
"Buy/Sell" varchar(4),
"ClassType"  varchar(7),
"Award FTR MW" float,
"Award FTR Price" float,
"File Name" text,
"Timestamp" text,
"Update Time" text,
"Month Start" text,
"Month End" text
)
'''
cursor.execute(create_auction_result_query)
#daily dalmp
create_hourly_dalmp_query = '''
create table IsoneHourlyDalmp (
"Date" text, 
"Hour Ending" int, 
"Location ID" int, 
"Location Name" varchar(30), 
"Location Type" varchar(30),
"Locational Marginal Price" float, 
"Energy Component" float, 
"Congestion Component" float,
"Marginal Loss Component" float, 
"Update Time" text, 
"ClassType" varchar(7), 
"Month" type
)
'''
'''

query3 = '''select distinct Date,"Hour Ending",ClassType from ISONEHourlyDaLmp where "Location ID" = 336 and "Date" = \'2022-03-04 00:00:00\' group by 1,2'''

query3 = '''select Date,ClassType,count(*) as Hours from ISONEHourlyDaLmp where "Location ID" = 336 group by 1,2'''
cursor.execute("drop table DailyHours")
data = pd.read_sql(query3, conn)
data['Month'] = data['Date'].apply(lambda x:pd.to_datetime(x).date().replace(day = 1))
data2.to_sql("DailyHours",con = conn,index = False)
query4 = '''select * from DailyHours'''
data2 = pd.read_sql(query4, conn)
data2.drop('index',axis = 1,inplace = True)
data2.to_csv("DailyHours.csv",index = False)
DailyHours = pd.read_csv('DailyHours.csv')
DailyHours.to_sql("DailyHours",con = conn,index = False)
MonthlyHours = pd.read_csv('MonthlyHours.csv')
MonthlyHours.to_sql("MonthlyHours",con = conn,index = False)


