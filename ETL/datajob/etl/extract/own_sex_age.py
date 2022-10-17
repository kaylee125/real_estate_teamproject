import json
from infra.hdfs_client import get_client
from infra.logger import get_logger
from infra.util import cal_std_day, execute_rest_api


class OwnTransferByGenderAge:
    URL = 'https://data.iros.go.kr/openapi/cr/rs/selectCrRsRgsCsOpenApi.rest?id=0000000024'
    SERVICE_KEY = '90555d6ea59f4c6bb4132fa2da1f65ca'
    FILE_DIR = '/real_estate/gender_age/'

    @classmethod
    def extract_data(cls, before_cnt=1):
        for i in range(1, before_cnt + 1):
            params = {
                'reqtype': 'json',
                'key': cls.SERVICE_KEY,
                'search_type_api': '03',
                'search_start_date_api': cal_std_day(i),
                'search_end_date_api': cal_std_day(i)
            }

            log_dict = {
                "is_success": "Fail",
                "type": "ownership_transfer_by_gender_age",
                "std_day": params['search_start_date_api'],
                "params": params
            }
            #print(params)

            try:
                res = execute_rest_api('get', cls.URL, {}, params)
                file_name = 'ownership_by_gender_age_' + params['search_start_date_api'] + '.json'
                get_client().write(hdfs_path=cls.FILE_DIR + file_name, data=res, overwrite=True, encoding='utf-8')
            except Exception as e:
                log_dict['err_msg'] = e.__str__()
                log_json = json.dumps(log_dict, ensure_ascii=False) # python 딕셔너리를 json문자열로 만들기
                #print(log_dict['err_msg'])
                get_logger('ownership_transfer_by_gender_age').error(log_json)
