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

def get_auction_month(auction_name):
    '''
    The function get the auction month based on the values in auction_name column
    :param auction_name: the values in auction_name column in the FTR auction result
    :return: the month of the contract i.e APR 2022 MAY -> 2022-05-01
    '''
    year = auction_name.split(' ')[1]
    month_start = auction_name.split(' ')[-1]
    auction_month = pd.to_datetime(year + '-' + month_start + '-01').date()
    return auction_month

