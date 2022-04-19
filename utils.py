from datetime import date,datetime
import pandas as pd

def pk_offpk(input_date,input_hour_ending):
    '''
    This function if a certain hour is peak or offpeak based on the date and hour.
    :param input_date:
    :param input_hour:
    :return: ONPEAK if the input is a peak hour, OFFPEAK if the input is an off peak hour
    '''
    nerc_holiday = ['2022-01-01','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-12-26']
    nerc_holiday = [datetime.strptime(x, '%Y-%m-%d').date() for x in nerc_holiday]
    if input_date in nerc_holiday:
        return 'OFFPEAK'
    elif input_date.weekday() in (5,6):
            return 'OFFPEAK'
    elif input_hour_ending >= 8 and input_hour_ending <= 23:
            return 'ONPEAK'
    else:
        return 'OFFPEAK'

def get_position_start_end_date(row):
    auction_name = row[0]
    year = auction_name.split(' ')[1]
    if auction_name.split(' ')[0] in ('LT1','LT2'):
        month_start = 'JAN'
        month_end = 'DEC'
    else:
        month_start = auction_name.split(' ')[-1]
        month_end = auction_name.split(' ')[-1]
    auction_month_start = pd.to_datetime(year + '-' + month_start + '-01').date()
    auction_month_end = pd.to_datetime(year + '-' + month_end + '-01').date()
    return pd.Series([auction_month_start,auction_month_end])

def get_position_start_end_date(row):
    auction_name = row[0]
    year = auction_name.split(' ')[1]
    if auction_name.split(' ')[0] in ('LT1','LT2'):
        month_start = 'JAN'
        month_end = 'DEC'
    else:
        month_start = auction_name.split(' ')[-1]
        month_end = auction_name.split(' ')[-1]
    auction_month_start = pd.to_datetime(year + '-' + month_start + '-01').date()
    auction_month_end = pd.to_datetime(year + '-' + month_end + '-01').date()
    return pd.Series([auction_month_start,auction_month_end])

def get_auction_month(auction_name):
    year = auction_name.split(' ')[1]
    month_start = auction_name.split(' ')[-1]
    auction_month = pd.to_datetime(year + '-' + month_start + '-01').date()
    return auction_month

def get_position_month(file_type,df):
    if file_type == 'monthly':
        df['Month'] = df['Auction Name'].apply(get_auction_month)
    else:
        x = list(pd.date_range(date(2022, 1, 1), date(2023, 1, 1), freq='1M'))

        y = pd.DataFrame([i.replace(day=1).date() for i in x], columns=['Date'])

        z = data.merge(y, how='cross')

