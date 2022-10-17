import json
from infra.hdfs_client import get_client
from infra.logger import get_logger
from infra.util import cal_std_day, execute_rest_api


class OwnTransferByAddress:
    URL = 'http://data.iros.go.kr/openapi/cr/rs/selectCrRsRgsCsOpenApi.rest?id=0000000025'
    SERVICE_KEY = 'bb5c91262bb74bc8a953f62e8732fea7'
    FILE_DIR = '/real_estate/address/'

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
                "type": "ownership_transfer_by_address",
                "std_day": params['search_start_date_api'],
                "params": params
            }
            #print(params)
            try:
                res = execute_rest_api('get', cls.URL, {}, params)
                file_name = 'ownership_by_address_' + params['search_start_date_api'] + '.json'
                get_client().write(hdfs_path=cls.FILE_DIR + file_name, data=res, overwrite=True, encoding='utf-8')
            except Exception as e:
                log_dict['err_msg'] = e.__str__()
                log_json = json.dumps(log_dict, ensure_ascii=False)
                #print(log_dict['err_msg'])
                get_logger('ownership_transfer_by_address').error(log_json)
