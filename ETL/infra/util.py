from datetime import datetime, timedelta
import requests

def cal_std_day(before_day):
    x = datetime.now() - timedelta(before_day)
    year = x.year
    month = x.month if x.month >= 10 else '0' + str(x.month)
    day = x.day if x.day >= 10 else '0' + str(x.day)
    return str(year) + str(month) + str(day)  # 20220926 형태로 반환

def cal_std_month(before_month):
    x = datetime.now() - timedelta(30 * before_month)
    year = x.year # 연도
    month = x.month if x.month >= 10 else '0' + str(x.month) # 월
    return str(year) + str(month)  # 202209 형태로 반환

def cal_std_month2(before_month):
    x = datetime.now() - timedelta(30 * before_month)
    year = x.year # 연도
    month = x.month if x.month >= 10 else '0' + str(x.month) # 월
    return str(year) + '-' + str(month)  # 2022-09 형태로 반환

# api를 호출하기 위한 base함수
def execute_rest_api(method, url, headers, params):
    if method == 'get':
        res = requests.get(url, params=params, headers=headers, verify=False)
    elif method == 'post':
        res = requests.post(url, params=params, headers=headers, verify=False)
    
    if res is None or res.status_code != 200:
        raise Exception('응답코드 : ' + str(res.status_code))

    return res.text
