import json
from pyspark.sql.types import *
from pyspark.sql.functions import col
from infra.logger import get_logger
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class OwnTypeTransformer:

    @classmethod
    def transform(cls, before_cnt=1):
        for i in range(1, before_cnt + 1): 
            try:
                # extract데이터 불러오기
                path = '/real_estate/local_foreigner_corp/ownership_by_local_foreigner_corp_' + cal_std_day(i) + '.json'
                tmp = get_spark_session().read.json(path, encoding='UTF-8')
                tmp2 = tmp.select('result').first()
                df = get_spark_session().createDataFrame(tmp2)
                tmp3 = df.select('items').first()
                tmp4 = get_spark_session().createDataFrame(tmp3).first()
                df2 = get_spark_session().createDataFrame(tmp4['item'])
            
                df_type = df2.select(df2.adminRegn1Name.alias('SIDO'),df2.resDate.alias('RES_DATE'),df2.enrNoCls.alias('OWNER_CLS'),df2.tot.alias('TOT'))
                
                # DW에서 지역코드 불러오기
                df_loc = find_data(DataWarehouse, 'LOC')
                loc_code = df_loc.select(['SIDO','LOC_CODE']).filter(df_loc.SIGUNGU.isNull()).collect()
                df_loc_code = get_spark_session().createDataFrame(loc_code)
                
                own_type = df_type.join(df_loc_code, on='SIDO')
                own_type = own_type.select(col('LOC_CODE').alias('RES_REGN_CODE'),col('TOT').cast('int'),col('OWNER_CLS'),col('RES_DATE').cast(DateType()))
                save_data(DataWarehouse, own_type, "OWN_TYPE")
            except Exception as e:
                log_dict = cls.__create_log_dict()
                cls.__dump_log(log_dict, e)


     # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('own_type_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "own_type_transform"
            }
        return log_dict