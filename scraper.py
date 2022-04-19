import requests
import re
from io import BytesIO
from datetime import date,datetime
import pandas as pd
from lxml.etree import HTML
from utils import get_auction_month

def get_isone_hourly_dalmp(report_date):
    '''
    The function scrapes the isone's website to get the hourly da lmp.
    https://www.iso-ne.com/isoexpress/web/reports/pricing/-/tree/lmps-da-hourly
    :param report_date: datetime.date format, which is the date to request the prices
    :return: a dataframe containing the prices
    '''
    price_file_link = 'https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_' + report_date.strftime('%Y%m%d') + '.csv'
    print(price_file_link)
    response = requests.get(price_file_link)
    df = pd.read_csv(BytesIO(response.content),skiprows=4)
    df = df[df.iloc[:, 0] != 'T']
    df = df.iloc[1:,1:]
    df['Update Time'] = datetime.now()
    df.dropna(inplace = True)
    print(df.shape)
    return df

def get_ftr_auction_result():
    '''
    The function scrapes the isone's auction result from the following website:
    https://www.iso-ne.com/isoexpress/web/reports/auctions/-/tree/auction-results-ftr
    and aggregate all the csvs into a dataframe
    :param: n/a
    :return: a dataframe containing the auction result
    '''
    url_link = 'https://www.iso-ne.com/isoexpress/web/reports/auctions/-/tree/auction-results-ftr'
    root_url = 'https://www.iso-ne.com'
    print('Creating a Session.')
    s = requests.Session()
    print('Requesting the url.')
    response = s.get(url_link)
    print('Processing the page.')
    tree = HTML(response.text)
    link_list = tree.xpath('//*[@class = "csvlink"]/@href')
    update_time_list = tree.xpath('//*[@class = "col-3"]/text()')[1:]
    file_idx = [i for i in range(len(link_list)) if bool(re.search('result', link_list[i]))]
    link_list = [root_url + link_list[i] for i in file_idx]
    update_time_list = [update_time_list[i] for i in file_idx]
    download_info = pd.DataFrame(list(zip(link_list,update_time_list)),columns = ['Link','UpdateTime'])
    download_info.drop_duplicates(inplace=True)

    print('CSV download list created.')
    output = pd.DataFrame()

    print('Start Combining CSVs.')
    for row in download_info.iterrows():
        link = row[1][0]
        update_time = row[1][1]
        print("Downloading CSV in the link %s." %link)
        response = s.get(link)
        df = pd.read_csv(BytesIO(response.content),skiprows=4)
        df = df[df.iloc[:,0] != 'T']
        df = df.iloc[1:,1:]
        file_type = re.search('&(.*)=',link).group(1)
        file_date = link.split("=")[-1]
        df['FileName'] = file_type + '_' + file_date
        df['TimeStamp'] = update_time
        df['UpdateTime'] = datetime.now()
        print(df.shape,file_type,file_date)
        if file_type == 'month':
            df['Month'] = df['Auction Name'].apply(get_auction_month)
        else:
            date_range = list(pd.date_range(date(int(file_date),1,1),date(int(file_date) + 1,1,1),freq='1M'))
            date_range = pd.DataFrame([i.replace(day=1).date() for i in date_range],columns = ['Month'])
            df['tmp'] = 1
            date_range['tmp'] = 1
            df = df.merge(date_range,on = ['tmp'],how = 'left')
            df.drop('tmp',axis = 1,inplace = True)
        print(df.shape)
        output = output.append(df)
    return output