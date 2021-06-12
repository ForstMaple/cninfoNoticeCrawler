import requests
import random
from time import sleep
from datetime import datetime, date, timezone, timedelta
import numpy as np
import pandas as pd
import json
import re
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

# cninfo only has data after this date
base_date = datetime.strptime('2000-01-01', "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=8)))


class Query:
    def __init__(self, query_name, searchkey, query_code_list, stock_list,
                 from_date, to_date=None,
                 last_update_time=None, record_num=None, result=None):
        self.query_name = query_name
        self.searchkey = searchkey
        self._query_code_list = query_code_list
        self._stock_list = stock_list
        self.from_date =from_date
        self.to_date = to_date
        self._last_update_time = last_update_time
        self._record_num = record_num
        self._result = result

    @property
    def status(self):
        time_range = format_seDate(from_date=self.from_date, to_date=self.to_date)
        status = f'''
        Query name: {self.query_name}
        Search keyword: {self.searchkey}
        Stock list length: {len(self._query_code_list)}
        Search keyword: {self.searchkey}
        Time range: {time_range}
        Most recent update: {self._record_num} records in total on {self._last_update_time}
        
        Instructions:
        - Use ".stock_list" attribute to access the stocks involved in the query.
        - Use ".result" attribute to access the query result DataFrame.
        - Use ".update()" method to update the query.
        - Use ".download()" method to download the PDF notices in the query result.
        '''
        print(dedent(status))
    
    @property
    def stock_code_list(self):
        return [query_code[0:6] for query_code in self._query_code_list]
    
    @property
    def stock_list(self):
        return self._stock_list
    
    @property
    def result(self):
        return self._result
    
    def save(self):
        os.makedirs('.saved_query', exist_ok=True)
        output = os.path.join(saved_query_path, self.query_name+'.json')
        result_json = self._result.to_json(orient='index', force_ascii=False, indent=4)
        dict_to_save = {'query_name': self.query_name, 
                        'searchkey': self.searchkey,
                        'query_code_list': self._query_code_list,
                        'from_date': self.from_date,
                        'to_date': self.to_date,
                        'stock_list': self._stock_list,
                        'last_update_time': self._last_update_time, 
                        'record_num': self._last_update_time}
        dict_to_save['result'] = json.loads(result_json)
        with open(output, 'w') as f:
            json.dump(dict_to_save, f, ensure_ascii=False, indent=4)
    
    def update(self, first_update=False, save_after_update=True):
        if self.to_date:
            print(f'This query has a specific cut-off date {self.to_date}, there will be no updates.')

        else:
            query_result_df = notice_query(self._query_code_list, self.searchkey, self.from_date, use_converter=False)
            new_record_num = query_result_df.shape[0]
            if not first_update:
                print(f'{new_record_num - self._record_num} new notice(s) since last update on {self._last_update_time}.')
            self._last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_num = new_record_num
            self._result = query_result_df
            print(f'Your query has been updated.\nThe query result now has {new_record_num} records. You can access them by ".result" attribute.')
        if save_after_update:
            self.save()
        else:
            pass
        
    def edit(target_list=None, from_date=None, to_date=None):
        pass
        
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
        code_for_company_name = re.compile(r'\d{6},') # For retreiving company names only
        try:
            if code_for_company_name.match(input):
                code = input[0:6]
                zwjc = rf_list.set_index('code').at[code, 'zwjc']
                return zwjc
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
    # Maybe exception should be better designed.
    bj = timezone(timedelta(hours=8))
    try:
        bj_date = datetime.strptime(date, '%Y-%m-%d')
        bj_date = bj_date.replace(tzinfo=bj)
    except ValueError:
        bj_date = datetime.strptime(date, '%Y%m%d') 
        bj_date = bj_date.replace(tzinfo=bj)
    except TypeError:
        utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        bj_date = utc.astimezone(bj)
        
    def from_date(n_days=0):
        return bj_date + timedelta(days=n_days)
    
    return from_date

def format_seDate(from_date=None, to_date=None):

    calculable_to_date = calculable_date(to_date)
    calculable_from_date = calculable_date(from_date)
    
    if calculable_from_date(0) < base_date:
        raise ValueError('cninfo has no record before 2000-01-01, please provide a later date.')
    
    if calculable_from_date(0) > calculable_to_date(0):
        raise ValueError('"from_date" should be no later than "to_date".')
    
    # The to_date will be "tommorow" if not specified so as to get the latest notices,
    # since a notice is usually released on the night just before the stated date.
    query_to_date = calculable_to_date(1) if to_date is None else calculable_to_date(0)
    
    # The from_date will be one year before the to_date if not specified.
    query_from_date = calculable_to_date(-365) if from_date is None else calculable_from_date(0)
    
    return query_from_date.strftime('%Y-%m-%d') + '~' + query_to_date.strftime('%Y-%m-%d')
    
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
        try:
            df = df.append(temp_df, ignore_index=True)
        except:
            pass
    
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
    if record_num == 0:
        print(f'[Warning] Found 0 record for {global_converter(stock[0:7])}.')
        return None
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
    
    cols_to_preserve = ['secName', 'secCode', 'announcementId', 'announcementTime', 'announcementTitle', 'adjunctUrl']
    result_df = pd.DataFrame(result_list)[cols_to_preserve]
    # Some extra records end with ".js". No clue of their occurrence yet, but they seem to be created by cninfo.
    result_df = result_df[~result_df['adjunctUrl'].str.endswith('.js')]
    result_df['announcementTime'] = result_df['announcementTime'].map(lambda x: date.fromtimestamp(int(str(x)[:10])))
    result_df['adjunctUrl'] = result_df['adjunctUrl'].map(lambda x: 'http://static.cninfo.com.cn/' + x)
    if searchkey:
        # to remove the <em> tag in the title, which highlighted the given "searchkey".
        result_df['announcementTitle'] = result_df['announcementTitle'].map(lambda x: re.sub(r'(<|</)em>', '', x))
    print(f'Found {result_df.shape[0]} record(s) for {global_converter(stock[0:7])}.')
        
    return result_df
            
    
def new_query(query_name, input_list, from_date, to_date=None, searchkey=None):
    if isinstance(input_list, str):
        input_list = [input_list]
    
    query_code_list = [global_converter(input) for input in input_list]
    
    calculable_from_date = calculable_date(from_date)
    from_date = calculable_from_date(0).strftime('%Y-%m-%d')
    
    if to_date:
        calculable_to_date = calculable_date(to_date)
        to_date = calculable_to_date(0).strftime('%Y-%m-%d')
    
    # When a code corresponds with no records, it will not appear in the .result
    temp_list = [query_code[0:7] for query_code in query_code_list]
    stock_list = [global_converter(temp_code) for temp_code in temp_list]
    
    query = Query(query_name=query_name, searchkey=searchkey, 
                  query_code_list=query_code_list, from_date=from_date, to_date=to_date, stock_list=stock_list)
    print(f'Query "{query_name}" has been created.\nUpdating...')
    query.update(first_update=True)
    query.status
    query.save()
    return query
    
    
def download_pdf_notices(url_list):
    pass    