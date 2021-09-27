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
from tqdm import tqdm

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
downloaded_notice_path = os.path.join(cwd, 'Downloads')
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
        查询名称: {self.query_name}
        搜索关键词: {self.searchkey}
        涉及公司数量: {len(self._query_code_list)}
        时间范围: {time_range}
        最近一次更新: {self._last_update_time} 共 {self._record_num} 条记录
        
        帮助:
        - 使用 ".stock_list" 属性可以获得该查询所涉及的公司列表；
        - 使用 ".result" 属性可以获得DataFrame格式的查询结果；
        - 使用 ".update()" 方法可以对该查询进行更新；
        - 使用 ".download()" 方法可以下载该查询结果中的公告的PDF文件。
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
                        'stock_list': self._stock_list,
                        'from_date': self.from_date,
                        'to_date': self.to_date,
                        'last_update_time': self._last_update_time, 
                        'record_num': self._record_num}
        dict_to_save['result'] = json.loads(result_json)
        with open(output, 'w') as f:
            json.dump(dict_to_save, f, ensure_ascii=False, indent=4)
    
    def update(self, first_update=False, save_after_update=True):
        if self.to_date:
            print(f'该查询有明确截至日期 {self.to_date}，更新将不会进行。')

        else:
            query_result_df = notice_query(self._query_code_list, self.searchkey, self.from_date, use_converter=False)
            new_record_num = query_result_df.shape[0]
            if not first_update:
                print(f'自 {self._last_update_time} 上一次更新后，有 {new_record_num - self._record_num} 条新记录。')
            self._last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._record_num = new_record_num
            self._result = query_result_df
            print(f'更新完成！\n该查询目前有 {new_record_num} 条记录，你可以通过 ".result" 属性来获得它们。')
        if save_after_update:
            self.save()
        else:
            pass
        
    def edit(target_list=None, from_date=None, to_date=None):
        pass
        
    def download(self, overwrite=False):
        download_pdf_notices(result_df=self._result, folder=self.query_name, overwrite=overwrite)
        
def converter():
    '''
    Parameters:
        None
        
    Returns:
        a function that accepts a stock code or a stock name and 
        returns the stock_code orgId pair for cninfo query.
    '''
    with open('szse_stock.json', 'rb') as szse_stock:
        
        rf_file = json.load(szse_stock)
        rf_list = pd.DataFrame(rf_file['stockList'])
    
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
            raise ValueError('请检查你的输入。\n股票代码和中文简称均支持，例如 "000001", "平安银行"。')
    
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
        raise ValueError('巨潮信息没有2000年1月1日以前的数据，请重新输入。')
    
    if calculable_from_date(0) > calculable_to_date(0):
        raise ValueError('"from_date" 参数必须不晚于 "to_date" 参数，请重新输入。')
    
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
            print(f'在一共 {max_attempts} 次尝试中未能获得任何数据，请检查你的参数。')
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
        print(f'[Warning] 在给定条件下没有找到 {global_converter(stock[0:7])} 的任何记录。')
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
    result_df['announcementTime'] = result_df['announcementTime'].map(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d'))
    result_df['adjunctUrl'] = result_df['adjunctUrl'].map(lambda x: 'http://static.cninfo.com.cn/' + x)
    if searchkey:
        # to remove the <em> tag in the title, which highlighted the given "searchkey".
        result_df['announcementTitle'] = result_df['announcementTitle'].map(lambda x: re.sub(r'(<|</)em>', '', x))
    print(f'找到 {global_converter(stock[0:7])} 的 {result_df.shape[0]} 条记录。')
        
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
    print(f'查询 "{query_name}" 已创建.\n正在更新...')
    query.update(first_update=True)
    query.status
    query.save()
    return query
    
    
def download_pdf_notices(result_df, folder=None, overwrite=False):
    try:
        dl_path = os.path.join(downloaded_notice_path, folder)
    except:
        dl_path = downloaded_notice_path
        
    os.makedirs(dl_path, exist_ok=True)
    info_df = result_df.copy()
    file_names = info_df['secName'] + '_' \
                 + info_df['announcementTime'].map(lambda x: x.replace('-', '')) + '_' \
                 + info_df['announcementId'] + '_' \
                 + info_df['announcementTitle'] + '.PDF'
    download_urls = info_df['adjunctUrl']
    total_records = info_df.shape[0]

    print(f'将尝试下载 {total_records} 个公告文件。 ')
    option = input('如需取消请输入n，留空或输入其他值将继续下载。  ')
    if option.lower() == 'n':
        print('用户取消下载！')
        return
    else:
        print('开始下载...')
        
    failure_num = 0
        
    sleep(1)
        
    for i in range(0, total_records):
        url = download_urls[i]
        name = file_names[i]
        file_path = os.path.join(dl_path, name)
        if os.path.exists(file_path) and (not overwrite):
            print(f'公告 {name} 已存在，将跳过下载。')
            continue
        else:
            try:
                response = requests.get(url=url, stream=True)
                with tqdm.wrapattr(open(file_path, "wb"), 
                                    "write", miniters=1, desc=name, 
                                    total=int(response.headers.get('content-length', 0))) as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
            except Exception as e:
                print(f'下载第 {i + 1} 个公告 {name} 时出错！')
                print(e)
                failure_num += 1
                continue
                        
    print(f'下载完成！下载成功 {total_records - failure_num} 个， 下载失败 {failure_num} 个。')

def display_saved_queries():
    try:
        saved_query_df = pd.DataFrame(columns=['文件名', '文件大小', '修改时间'])
        saved_query_df['文件名'] = os.listdir(saved_query_path)
        saved_query_df = saved_query_df[saved_query_df['文件名'].str.endswith('.json')]
        saved_query_df['文件大小'] = saved_query_df['文件名'].map(lambda x: str(round(float(os.path.getsize(os.path.join(saved_query_path, x))) / 1024, 2)) + ' KB')
        saved_query_df['修改时间'] = saved_query_df['文件名'].map(lambda x: datetime.fromtimestamp(os.path.getmtime(os.path.join(saved_query_path, x))).strftime('%Y/%m/%d %H:%M:%S'))
        return saved_query_df
    except:
        print('当前没有已保存的查询或文件格式有误！')
        return None

def load_query(file_name):
    if not file_name.endswith('.json'):
        file_name = file_name + '.json'
        
    file_path = os.path.join(saved_query_path, file_name)
    if not os.path.exists(file_path):
        raise ValueError('没有找到对应的文件！')
        
    try:
        with open(file_path, 'rb') as f:
            args = json.load(f)
        query = Query(query_name=args['query_name'], 
                      searchkey=args['searchkey'], 
                      query_code_list=args['query_code_list'],
                      stock_list=args['stock_list'],
                      from_date=args['from_date'],
                      to_date=args['to_date'],
                      last_update_time=args['last_update_time'],
                      record_num=args['record_num'],
                      result=pd.DataFrame(args['result']).T)
        return query
    except Exception as e:
        print('文件格式有误或文件已损坏！')
        print(e)
        return None