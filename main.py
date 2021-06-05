import requests
import random
from time import sleep
from datetime import datetime, date, timezone, timedelta
import numpy as np
import pandas as pd
import json
import re
import pickle
import os
from textwrap import dedent

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
    'pageNum': '',
    'pageSize': '30',
    'column': '',
    'tabName': 'fulltext',
    'plate': '', 
    'stock': '',
    'searchkey': '',
    'secid': '',
    'category': '',
    'trade': '',
    'seDate': '',
    'sortName': '',
    'sortType': '', 
    'isHLtitle': 'true'
}

cwd = os.getcwd()
saved_query_path = os.path.join(cwd, '.saved_query')
request_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
max_attempts = 3


class Query:
    def __init__(self, query_name, searchkey, code_list,
                 from_date, to_date=None,
                 stock_list=None, last_update_time=None, record_num=None):
        self.query_name = query_name
        self.searchkey = searchkey
        self.from_date =from_date
        self.to_date = to_date
        self._stock_list = stock_list
        self._code_list = code_list
        self._last_update_time = last_update_time
        self._record_num = record_num

    @property
    def status(self):
        status = f'''
        Query name: {self.query_name}
        Search keyword: {self.searchkey}
        Stock list length: {len(self._stock_list)}
        Search keyword: {self.searchkey}
        Time range: {str(self.from_date) + "~" + str(self.to_date)}
        Most recent update: {self._record_num} records in total on {self._last_update_time}
        
        Instructions:
        - Use ".stock_list" attribute to access the stocks involved in the query.
        - Use ".result" attribute to access the query result DataFrame.
        - Use ".update()" method to update the query.
        - Use ".download()" method to download the PDF notices in the query result.
        '''
        print(dedent(status))
    
    def save(self):
        os.makedirs('.saved_query', exist_ok=True)
        output = os.path.join(saved_query_path, self.query_name+'.json')
        with open(output, 'w') as f:
            json.dump(self, f, default=lambda obj: obj.__dict__, indent=4)
    
    def update(self, first_update=False):
        if self.to_date:
            print(f'This query has a specific cut-off date {self.to_date}, there will be no updates.')

        else:
            query_result_df = notice_query(self._code_list, self.searchkey, self.from_date, use_converter=False)
            new_record_num = query_result_df.shape[0]
            if first_update:
                self._stock_list = list(query_result_df['secName'].unique())
            else:
                print(f'{new_record_num - self._record_num} new notice(s) since last update on {self._last_update_time}.')
            self._last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_num = new_record_num
            print(f'Your query has been updated.\nThe query result now has {new_record_num} records. You can access them by ".result" attribute.')
        
    def download(self):
        pass
        
        
def converter():
    '''
    Parameters:
        None
        
    Returns:
        a function that accepts a stock code or a stock name and 
        returns the stock_code orgId pair for cninfo query.
    '''
    with open('szse_stock.json', 'rb') as szse_stock:
        rf_list = pd.DataFrame(json.load(szse_stock))
    
    def convert(input):
        possible_stock_code = re.compile(r'\d{6}')
        try:
            if possible_stock_code.match(input):
                orgId = rf_list.set_index('code').at[input, 'orgId']
                return input + ',' + orgId
            else:
                code = rf_list.set_index('zwjc').at[input, 'code']
                orgId = rf_list.set_index('zwjc').at[input, 'orgId']
                return code + ',' + orgId   
        except Exception:
            raise ValueError('Please check your input.\nStock codes and stock names are supported, e.g. "000001", "平安银行".')
    
    return convert
            
global_converter = converter()         
        
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
    
def notice_query(input_list, searchkey=None, from_date=None, to_date=None, use_converter=True):
    if isinstance(input_list, str):
        input_list = [input_list]
    else:
        pass
    seDate = format_seDate(from_date, to_date)
    if use_converter:
        query_code_list = [global_converter(input) for input in input_list]
    else:
        query_code_list = input_list
    
    df = pd.DataFrame(columns=['secName', 'secCode', 'announcementId', 
                               'announcementTime', 'announcementTitle', 'adjunctUrl'])
    
    for query_code in query_code_list:
        temp_df = get_query_page(stock=query_code, searchkey=searchkey, seDate=seDate)
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
    
    if total_page >= 2:
        for pageNum in range(2, total_page + 1):
            query_form_data['pageNum'] = pageNum
            try:
                query = requests.post(url=request_url, data=query_form_data, headers=headers)
                result_list.extend(query.json()['announcements'])
            except:
                pass
    else:
        pass
    
    cols_to_preserve = ['secName', 'secCode', 'announcementId', 'announcementTime', 'announcementTitle', 'adjunctUrl']
    result_df = pd.DataFrame(result_list)[cols_to_preserve]
    result_df['announcementTime'] = result_df['announcementTime'].map(lambda x: date.fromtimestamp(int(str(x)[:10])))
    result_df['adjunctUrl'] = result_df['adjunctUrl'].map(lambda x: 'http://static.cninfo.com.cn/' + x)
        
    return result_df
            
    
def new_query(query_name, input_list, from_date, to_date=None, searchkey=None):
    if isinstance(input_list, str):
        input_list = [input_list]
    else:
        pass
    code_list = [global_converter(input) for input in input_list]
    query = Query(query_name=query_name, searchkey=searchkey, 
                  code_list=code_list, from_date=from_date, to_date=to_date)
    print(f'Query {query_name} has been created.\nUpdating...')
    query.update(first_update=True)
    query.status
    query.save()
    return query
    
    
def download_pdf_notices(url_list):
    pass    