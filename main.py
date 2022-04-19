import scraper
import utils
import pandas as pd
from pandasql import sqldf
from db_initialize import conn
pysqldf = lambda q: sqldf(q, globals())


#get the hourly DA price of March22
hourly_da_lmp = pd.DataFrame()
date_list = pd.date_range('2022-3-1','2022-3-31')

for input_date in date_list:
    da_lmp = scraper.get_isone_hourly_dalmp(input_date)
    hourly_da_lmp = hourly_da_lmp.append(da_lmp)

#reformat the table
hourly_da_lmp['Date'] = pd.to_datetime(hourly_da_lmp['Date'])
hourly_da_lmp['Hour Ending'] = pd.to_numeric(hourly_da_lmp['Hour Ending'])
hourly_da_lmp['Locational Marginal Price'] = pd.to_numeric(hourly_da_lmp['Locational Marginal Price'])
hourly_da_lmp['Energy Component'] = pd.to_numeric(hourly_da_lmp['Energy Component'])
hourly_da_lmp['Congestion Component'] = pd.to_numeric(hourly_da_lmp['Congestion Component'])
hourly_da_lmp['Marginal Loss Component'] = pd.to_numeric(hourly_da_lmp['Marginal Loss Component'])
hourly_da_lmp['ClassType'] = hourly_da_lmp.apply (lambda row: utils.pk_offpk(row['Date'],row['Hour Ending']), axis=1)
hourly_da_lmp['Month'] = hourly_da_lmp['Date'].apply(lambda x: x.replace(day = 1))
hourly_da_lmp['Location ID'] = pd.to_numeric(hourly_da_lmp['Location ID'])

#cursor.execute("drop table ISONEHourlyDaLmp")
#hourly_da_lmp.to_sql('ISONEHourlyDaLmp',con=conn)

#aggregate to monthly da_lmp

daily_da_lmp = hourly_da_lmp[
                                ['Date',
                                 'Location ID',
                                 'ClassType',
                                 'Month',
                                 'Locational Marginal Price',
                                 'Energy Component',
                                 'Congestion Component',
                                 'Marginal Loss Component'
                                    ]
                                ].groupby(
                                ['Date',
                                 'Location ID',
                                 'ClassType',
                                 'Month'
                                 ]
                                ).mean().reset_index()

#load the auction result
auction_result = scraper.get_ftr_auction_result()
#cursor.execute("drop table FtrAuctionResult")
#cursor.execute("drop table IsoneDailyDaLmp")
daily_da_lmp.to_sql('IsoneDailyDaLmp',con = conn, index = False, if_exists = 'append')
auction_result.to_sql('FtrAuctionResult',con = conn, index = False, if_exists = 'append')

#1. calculate the mar22 Pnl

query1= '''
select 
    a.*,
    b.date,
    b."Congestion Component" as Congestion_Source,
    c."Congestion Component" as Congestion_Sink,
    c."Congestion Component" - b."Congestion Component"  as End_Price,
    d."Hours" as "Hours",
    (d."Hours" * ((c."Congestion Component" - b."Congestion Component") - a."Award FTR Price")) * a."Award FTR MW" *
    (case when 
    a."Buy/Sell" = 'BUY' 
    then 1
    else -1 
    end)as Pnl
from 
    FtrAuctionResult a
join
    DailyHours d
on
    d.ClassType = a.ClassType and d.Month = a.Month
join
    IsoneDailyDaLmp b
on
    a."Source Location ID" = b."Location ID" and a."ClassType" = b."ClassType" and b.Date = d.Date
join
    IsoneDailyDaLmp c
on
    a."Sink Location ID" = c."Location ID" and a."ClassType" = c."ClassType" and c.Date = b.Date
where 
    a."Month" = \'2022-03-01\'
'''

mar_result = pd.read_sql(query1,con = conn)

#aggregate
pnl_by_company = mar_result[['Customer Name','Pnl']].groupby('Customer Name').sum().reset_index()

#2. calculate the Mar22 MtA (Mark to Auction) pnl
query2= '''
with t_new_auction as
(select * from FtrAuctionResult where FileName = 'month_202204')
select 
    a.*,
    d."Hours" as "Hours",
    (d."Hours" * (b."Award FTR Price" - a."Award FTR Price")) * a."Award FTR MW" *
    (case when 
    a."Buy/Sell" = 'BUY' 
    then 1
    else -1 
    end)as Pnl
from 
    FtrAuctionResult a
join
    MonthlyHours d
on
    d.ClassType = a.ClassType and d.Month = a.Month
join
    t_new_auction b
on
    a."Source Location ID" = b."Source Location ID" and 
    a."Sink Location ID" = b."Sink Location ID" and
    a."ClassType" = b."ClassType" and 
    b.Month = d.Month
'''

apr_result = pd.read_sql(query2,con = conn)

pnl_by_company = apr_result[['Customer Name','Pnl']].groupby('Customer Name').sum().reset_index()

