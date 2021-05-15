import requests
import random
from time import sleep
from datetime import datetime, date, timezone, timedelta
import numpy as np
import pandas as pd
import json
import re

headers = {
    'Host': 'www.cninfo.com.cn',
    'Connection': 'keep-alive',
    'Content-Length': '200',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'http://www.cninfo.com.cn',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cookie': ''}

form_data = {
    'pageNum': '1',
    'pageSize': '30',
    'column': '',
    'tabName': 'fulltext',
    'plate': '', 
    'stock': '601628,9900001881',
    'searchkey': '',
    'secid': '',
    'category': '',
    'trade': '',
    'seDate': '2020-11-13~2021-05-14',
    'sortName': '',
    'sortType': '', 
    'isHLtitle': 'true'
}

request_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
max_attempts = 3
 
def stock_formatter():
    '''
    Parameters:
        None
        
    Returns:
        a function that accepts a stock code or a stock name and 
        returns the stock_code orgId pair for cninfo query
    '''
    with open('szse_stock.json', 'rb') as szse_stock:
            reference_list = pd.DataFrame(json.load(szse_stock))
    
    def format_stock(input):
        try:
            if re.match(r'\d{6}', input):
                orgId = reference_list.set_index('code').at[input, 'orgId']
                return input + ',' + orgId
            else:
                code = reference_list.set_index('zwjc').at[input, 'code']
                orgId = reference_list.set_index('zwjc').at[input, 'orgId']
                return code + ',' + orgId
        except Exception:
            raise ValueError('Please check your input.\nStock codes and stock names are supported, e.g. "000001", "平安银行".')
    return format_stock
            
                
        
def calculable_date(date=None):
    try:
        bj_date = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        bj_date = datetime.strptime(date, '%Y%m%d') 
    except TypeError:
        utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        bj_date = utc.astimezone(timezone(timedelta(hours=8)))
        
    def from_date(n_days=0):
        return (bj_date + timedelta(days=n_days)).strftime('%Y-%m-%d')
    
    return from_date

def format_seDate(from_date=None, to_date=None):
    calculable_to_date = calculable_date(to_date)
    calculable_from_date = calculable_date(from_date)
    
    # The to_date will be "tommorow" if not specified so as to get the latest notices,
    # since a notice is usually released on the night just before the stated date.
    query_to_date = calculable_to_date(1) if to_date is None else calculable_to_date(0)
    
    # The from_date will be one year before the to_date if not specified.
    query_from_date = calculable_to_date(-365) if from_date is None else calculable_from_date(0)
    return query_from_date + '~' + query_to_date
    
def notice_query(input_list, searchkey=None, from_date=None, to_date=None):
    input_list = list(input_list)
    seDate = format_seDate(from_date, to_date)
    formatter = stock_formatter()
    
    stock_list = [formatter(input) for input in input_list]
    df = pd.DataFrame(columns=['secName', 'secCode', 'announcementId', 'announcementTime', 'announcementTitle', 'adjunctUrl'])
    
    for stock in stock_list:
        temp_df = get_query_page(stock=stock, searchkey=searchkey, seDate=seDate)
        df = df.append(temp_df, ignore_index=True)
    
    return df
        
        
def get_query_page(stock, searchkey, seDate):
    query_form_data = form_data.copy()
    query_form_data['pageNum'] = 1
    query_form_data['stock'] = stock
    query_form_data['searchkey'] = searchkey
    query_form_data['seDate'] = seDate
    
    attempt = 0
    while True:
        attempt += 1
        sleep(random.uniform(1, 2))
        if attempt > max_attempts:
            print(f'Failed to fetch any data in {max_attempts} attempt(s), please check your parameters.')
            break
        else:
            try:
                first_query = requests.post(url=request_url, data=query_form_data, headers=headers)
            except Exception as e:
                print(e)
                continue
            if first_query.status_code == requests.codes.ok and first_query.text != '':
                break
    first_query_result = first_query.json()
    record_num = first_query_result['totalAnnouncement']
    result_list = first_query_result['announcements']
    total_page = record_num // 30 + 1
    
    for pageNum in range(2, total_page + 1):
        query_form_data['pageNum'] = pageNum
        try:
            query = requests.post(url=request_url, data=query_form_data, headers=headers)
        except Exception as e:
            print(f'Error when fetching page {pageNum}:')
            print(e)
        result_list.extend(query.json()['announcements'])
    
    cols_to_preserve = ['secName', 'secCode', 'announcementId', 'announcementTime', 'announcementTitle', 'adjunctUrl']
    result_df = pd.DataFrame(result_list)[cols_to_preserve]
    result_df['announcementTime'] = result_df['announcementTime'].map(lambda x: date.fromtimestamp(int(str(x)[:10])))
    result_df['adjunctUrl'] = result_df['adjunctUrl'].map(lambda x: 'http://static.cninfo.com.cn/' + x)
        
    return result_df
            
    
    
    
