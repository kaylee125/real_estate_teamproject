import requests
import json
import datetime as dt
import json
from infra.hdfs_client import get_client
from infra.logger import get_logger
from infra.util import cal_std_month


class RealEstateOwnExtractor:

    FILE_DIR = '/real_estate/own/'
    URL = "https://data.iros.go.kr/rp/ds/selectDatasetInqDetl.do"
    SERVICE_KEY = '746cac395d3d43f2bc4ba51f1fe56a68'


    #hdfs에 json파일 형태로 저장함수
    @classmethod
    def extract_data(cls):

        data={
            'pageIndex':'1'
            ,'sort_cond':''
            ,'sort_ord':''
            ,'ris_menu_seq':'0000000007'
            ,'title':'%EB%B6%80%EB%8F%99%EC%82%B0 %EC%86%8C%EC%9C%A0%EA%B6%8C %EC%B7%A8%EB%93%9D%ED%98%84%ED%99%A9(%EC%A7%80%EC%97%AD%2C %EC%86%8C%EC%9C%A0%EC%9E%90%EB%B3%84)'
            ,'search_date':cal_std_month(1)
            ,'rdata_seq':'0000000252'
            ,'search_type':'037001'
            ,'search_word':'%EB%B6%80%EB%8F%99%EC%82%B0 %EC%86%8C%EC%9C%A0%EA%B6%8C %EC%B7%A8%EB%93%9D%ED%98%84%ED%99%A9'
            ,'pagePerCount':'10'
            ,'search_order':'031003'
            ,'dts_prmm_ofr_yn':'Y'
            ,'returnUrl':''
            ,'search_regn_name':''
            ,'search_year':''
            ,'search_mon':''

        }

        headers = {
            'Content-Type':'application/x-www-form-urlencoded'
            ,'-User-Agent':'PostmanRuntime/7.29.2'
            ,'Accept':'*/*'
            ,'Accept-Encoding':'gzip, deflate, br'
            ,'Connection':'keep-alive'
        }

        log_dict = {
                "is_success": "Fail",
                "type": "ownership_transfer_by_address",
                "std_day": data['search_date'],
                "data": data
            }

        try:
            res = requests.post(cls.URL, data, headers, verify=False, timeout=10)
            file_name = 'realestate_own_' + data['search_date'] + '.json'
            get_client().write(cls.FILE_DIR + file_name, json.dumps(res.json()), overwrite=True, encoding='utf-8')
        
        except Exception as e:
            log_dict['err_msg'] = e.__str__()
            log_json = json.dumps(log_dict, ensure_ascii=False) 
            print(log_dict['err_msg'])
            get_logger('realestate_own').error(log_json)
