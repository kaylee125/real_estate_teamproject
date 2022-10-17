import json
import pandas as pd
from infra.hdfs_client import get_client
from infra.jdbc import DataWarehouse, find_data
from infra.logger import get_logger
from infra.util import cal_std_month, execute_rest_api
from bs4 import BeautifulSoup

class ApartmentSalePrice:
    URL = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?"
    SERVICE_KEY = 'NvoJpM4nyxzXkb5F8hffSDHLrfuCyIcVBqBSDCgTa+/7CtQnsrFwE8y/a0lLPVxN1AESAPkiMkfoS7KYrck13A=='
    SERVICE_KEY1 = 'lWLaCFJasDo6vpnFordk0ZVBBDk0eu0yL+KUjEF56K+w78w4lsKg7BJANNWeWbL2mzl72Q4LfFyIygL5qCGGkA=='
    SERVICE_KEY2 = '8jy/djLDfwp5c1AqB5n9l10ZWmC4JLuurk79RH8bK3dMzEr42QBi//FALBtxojuuDyhrwzLvewwgU8mH0sfHMw=='
    SERVICE_KEY3 = '7ConDA0CbBnYPevhhNhcTnvl65yZZghRa1yNNS+E0eHJLCuBNXG8SoXWbT4gNJ3l+TFLc6Vi3v7IeDIC0/Jg6A=='
    SERVICE_KEY4 = 'AAWZ8YYk+UVVj9tf9VP7GniCMCSJ0lUHm/0JMAeuV/YeR1vtMRK1xwhHf0dNcDeDjGVOZv/DFyxMTDtOjOd5Ag=='
    FILE_DIR = '/real_estate/apartment_price/'

    @classmethod
    def extract_data(cls, before_cnt=1):
        params = cls.__create_params()
        log_dict = cls.__create_log_dict(params)

        # DW LOC테이블에서 지역코드정보 가져옴
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_codes = df_loc.select('LOC_CODE').collect()

        # 지역코드 갯수만큼 반복
        for i in range(len(loc_codes)):
            data = []
            loc_code = loc_codes[i][0]
            #print("loc_codes:", loc_code)
            try:
                # 날짜만큼 반복
                for i in range(1, before_cnt + 1):
                    params['LAWD_CD'] = loc_code
                    params['DEAL_YMD'] = cal_std_month(i)
                    res = execute_rest_api('get', cls.URL, {}, params)
                    cls.__parse_and_save_data(data, res)
                #print("len(data):", len(data))
                if len(data) > 0:
                    cls.__write_to_csv(data, params)
            except Exception as e:
                cls.__dump_log(log_dict, e)
            
    # 파라미터 생성
    @classmethod
    def __create_params(cls):
        params = {'ServiceKey': cls.SERVICE_KEY,
                'pageNo': '1',
                'numOfRows': '2000',
                'LAWD_CD': '41190',
                'DEAL_YMD': '202209'
            }
        return params

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('apartment_sale_price_extract').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
                "is_success": "Fail",
                "type": "apartment_sale_price_extract",
                "std_month": params['DEAL_YMD'],
                "params": params
            }
        return log_dict

    # 연,월,일 받아서 2022-09-30 형식으로 리턴하는 함수
    @classmethod
    def __create_date(cls, year, month, day):
        if len(month) < 2:
            month = '0' + str(month)
        if len(day) < 2:
            day = '0' + str(day)
        res = year + '-' + month + '-' + day
        return res

    # bs이용해서 파싱한 후 데이터에 삽입하는 함수
    @classmethod
    def __parse_and_save_data(cls, data, res):
        soup = BeautifulSoup(res, 'xml')

        res_code = soup.find('resultCode').text
        if res_code == '99':
            res_msg = soup.find('resultMsg').text
            raise Exception('주의:', res_msg)

        sp_price = soup.findAll('거래금액')
        sp_year = soup.findAll('년')
        sp_month = soup.findAll('월')
        sp_day = soup.findAll('일')
        sp_area = soup.findAll('전용면적')
        sp_localcode = soup.findAll('지역코드')
        for k in range(len(sp_price)):
            tmp_date = cls.__create_date(sp_year[k].text, sp_month[k].text, sp_day[k].text)
            data.append([sp_price[k].text.strip(), tmp_date, sp_area[k].text, sp_localcode[k].text])

    # 추출한 데이터를 csv파일로 저장하는 함수
    @classmethod
    def __write_to_csv(cls, data, params):
        df = pd.DataFrame(data)
        file_name = cls.FILE_DIR + 'apart_price_data_' + params['LAWD_CD'] + '_' + params['DEAL_YMD'] + '.csv'
        with get_client().write(file_name, overwrite=True, encoding='cp949') as writer:
            df.to_csv(writer, header=['거래금액(만원)', '거래날짜', '전용면적',  '지역코드'], index=False)

