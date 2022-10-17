import json
from pyspark.sql.types import *
from pyspark.sql.functions import col
from infra.logger import get_logger
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class OwnAddrTransformer:

    @classmethod
    def transform(cls, before_cnt=1):
        for i in range(1, before_cnt + 1):
            try:
                file_name = '/real_estate/address/ownership_by_address_' + cal_std_day(i) + '.json'
                tmp = get_spark_session().read.json(file_name, encoding='UTF-8')
                tmp2 = tmp.select('result').first()
                df = get_spark_session().createDataFrame(tmp2)
                tmp3 = df.select('items').first()
                tmp4 = get_spark_session().createDataFrame(tmp3).first()
                df2 = get_spark_session().createDataFrame(tmp4['item'])
                df_addr = df2.select(df2.adminRegn1Name.alias('SIDO'), df2.resDate.alias('RES_DATE'), df2.adminRegn1NamePerson.alias('SIDO2'),df2.tot.alias('TOT'))
                
                # DW에서 지역코드 불러오기
                df_loc = find_data(DataWarehouse, 'LOC')
                loc_code = df_loc.select(['SIDO','LOC_CODE']).filter(df_loc.SIGUNGU.isNull()).collect()
                df_loc_code = get_spark_session().createDataFrame(loc_code)

                # loc테이블조인
                own_addr = df_addr.join(df_loc_code, on='SIDO').drop(col('SIDO'))
                own_addr = own_addr.select(col('LOC_CODE').alias('RES_REGN_CODE'),col('SIDO2').alias('SIDO'),col('TOT').cast('int'),col('RES_DATE').cast(DateType()) )
                own_addr = own_addr.join(df_loc_code, on='SIDO').drop(col('SIDO'))
                own_addr = own_addr.select(col('LOC_CODE').alias('BUYER_REGN_CODE'),col('TOT'),col('RES_REGN_CODE'),col('RES_DATE'))

                save_data(DataWarehouse, own_addr, "OWN_ADDR")
            except Exception as e:
                log_dict = cls.__create_log_dict()
                cls.__dump_log(log_dict, e)



    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('own_addr_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "own_addr_transform"
            }
        return log_dict