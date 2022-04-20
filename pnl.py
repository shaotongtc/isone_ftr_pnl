import scraper
import utils
import pandas as pd
from datetime import timedelta


#get the hourly DA price of March22
def f_get_monthly_settlement_price(month):

    hourly_da_lmp = pd.DataFrame()
    date_start = month
    date_end = (month + timedelta(days = 32)).replace(day = 1) + timedelta(days = -1)
    date_list = pd.date_range(date_start,date_end)

    print("Scraping Data...")
    for input_date in date_list:
        da_lmp = scraper.get_isone_hourly_dalmp(input_date)
        hourly_da_lmp = hourly_da_lmp.append(da_lmp)

    print("Formatting the table...")
    #Formatting the table
    hourly_da_lmp['Date'] = pd.to_datetime(hourly_da_lmp['Date'])
    hourly_da_lmp['Hour Ending'] = pd.to_numeric(hourly_da_lmp['Hour Ending'])
    hourly_da_lmp['Locational Marginal Price'] = pd.to_numeric(hourly_da_lmp['Locational Marginal Price'])
    hourly_da_lmp['Energy Component'] = pd.to_numeric(hourly_da_lmp['Energy Component'])
    hourly_da_lmp['Congestion Component'] = pd.to_numeric(hourly_da_lmp['Congestion Component'])
    hourly_da_lmp['Marginal Loss Component'] = pd.to_numeric(hourly_da_lmp['Marginal Loss Component'])
    hourly_da_lmp['ClassType'] = hourly_da_lmp.apply (lambda row: utils.pk_offpk(row['Date'],row['Hour Ending']), axis=1)
    hourly_da_lmp['Month'] = hourly_da_lmp['Date'].apply(lambda x: x.replace(day = 1))
    hourly_da_lmp['Location ID'] = pd.to_numeric(hourly_da_lmp['Location ID'])

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

    return daily_da_lmp

#load the auction result

#1. calculate the mar22 Pnl
def f_calculate_monthly_ftr_settlement_pnl(month,conn):
    '''
    The function returns the entire month's FTR PnL with daily settlement.
    :param month: The month which the PnL needs to be calculated, need to be in 'yyyy-mm-dd' format
    :param conn: The connection to the database
    :return: A dataframe with the detailed Pnl info
    '''
    query = '''
        select 
            a.*,
            b.date,
            b."Congestion Component" as Congestion_Source,
            c."Congestion Component" as Congestion_Sink,
            c."Congestion Component" - b."Congestion Component"  as End_Price,
            d."Hours" as "Hours",
            d."Hours" *
            (
                (c."Congestion Component" - b."Congestion Component") - 
                (a."Award FTR Price" / (case when a.FileName like 'long_term%%' then e.HoursYear else e.Hours end))
            ) * 
            a."Award FTR MW" *
            (
                case when a."Buy/Sell" = 'BUY' then 1 else -1 end
            ) as Pnl
        from 
            FtrAuctionResult a
        join
            DailyHours d
        on
            d.ClassType = a.ClassType and d.Month = a.Month
        join
            MonthlyHours e
        on
            e.Month = d.Month and e.ClassType = a.ClassType 
        join
            IsoneDailyDaLmp b
        on
            a."Source Location ID" = b."Location ID" and a."ClassType" = b."ClassType" and b.Date = d.Date
        join
            IsoneDailyDaLmp c
        on
            a."Sink Location ID" = c."Location ID" and a."ClassType" = c."ClassType" and c.Date = b.Date
        where 
            a."Month" = '%s'
        ''' % month

    output = pd.read_sql(query,con = conn)

    return output



#2. calculate the Mar22 MtA (Mark to Auction) pnl
def f_calculate_monthly_ftr_mta_pnl(month,conn):
    '''
    The function calculates the mta pnl with one auction
    :param month: The input month of an auction, need to be in 'long_term_yyyy' or 'monthly_yyyymm' format
    :param conn: The connection to the database
    :return: A dataframe with the detailed Pnl info
    '''
    query = '''
    with t_new_auction as
    (
        select distinct 
            Month,
            "Award FTR Price",
            "Source Location ID",
            "Sink Location ID",
            ClassType
        from 
            FtrAuctionResult 
        where 
            FileName = '%s'
    )
    select 
        a.*,
        b.Month as new_month,
        b."Award FTR Price" as new_price,
        (b."Award FTR Price"  - a."Award FTR Price" * d.Hours / d.HoursYear) * a."Award FTR MW" *
        (case when a."Buy/Sell" = 'BUY' then 1 else -1 end) as Pnl
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
    where
        a.FileName like 'long_term%%'
    ''' % month

    output = pd.read_sql(query,con = conn)

    return output



