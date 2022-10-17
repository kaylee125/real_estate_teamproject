
import json
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *
from infra.jdbc import DataWarehouse, find_data, overwrite_data, save_data
from infra.logger import get_logger
from infra.spark_context import get_spark_context
from infra.spark_session import get_spark_session
from infra.util import cal_std_month


class ApartmentSalePriceTransformer:
    FILE_DIR = '/real_estate/apartment_price/'

    @classmethod
    def transform(cls):
        loc_codes = cls.__fetch_localcodes()

        for i in range(len(loc_codes)):
            try:
                df_apt_prc = cls.__read_csv_file(loc_codes, i)
                
                prc_list_trans = cls.__cast_amount(df_apt_prc)
                
                rdd_prc_list_trans = cls.__rdd_transform(prc_list_trans)

                df_prc = cls.__create_df_with_rdd(rdd_prc_list_trans)

                df_apt_prc = cls.__select_columns(df_apt_prc)

                df_apt_prc = cls.__join_df(df_apt_prc, df_prc)

                # DW에 Load
                save_data(DataWarehouse, df_apt_prc, 'REAL_PRC_APT')
                #overwrite_data(DataWarehouse, df_apt_prc, 'REAL_PRC_APT')
            except Exception as e:
                log_dict = cls.__create_log_dict(loc_codes[i][0])
                cls.__dump_log(log_dict, e)

    @classmethod
    def __fetch_localcodes(cls):
        # DW LOC테이블에서 지역코드정보 가져옴
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_codes = df_loc.select('LOC_CODE').collect()
        return loc_codes

    @classmethod
    def __join_df(cls, df_apt_prc, df_prc):
        # 두 데이터프레임을 합하기 위해서 두 DF에서 가상으로 idx만든다음, 그 idx로 join
        df_prc = df_prc.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
        df_apt_prc = df_apt_prc.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
        df_apt_prc = df_apt_prc.join(df_prc, on=["row_index"]).drop("row_index", "name")
        # df_apt_prc.show(3)
        # print(df_apt_prc.dtypes)
        return df_apt_prc

    @classmethod
    def __select_columns(cls, df_apt_prc):
        # select columns and cast type
        df_apt_prc = df_apt_prc.select(col('거래날짜').cast(DateType()).alias('res_date'), col('전용면적').cast('float').alias('area'), col('지역코드').alias('regn_code'))
        return df_apt_prc

    @classmethod
    def __create_df_with_rdd(cls, rdd_prc_list_trans):
        # 거래금액 리스트를 데이터프레임으로 생성
        schema = StructType([StructField("amount", IntegerType(), False)])
        df_prc = get_spark_session().createDataFrame(data=rdd_prc_list_trans, schema=schema)
        return df_prc

    @classmethod
    def __rdd_transform(cls, prc_list_trans):
        # 데이터프레임으로 생성하기 전 rdd이용해 transform
        rdd_prc_list_trans = get_spark_context().parallelize(c=prc_list_trans)
        rdd_prc_list_trans = rdd_prc_list_trans.map(lambda x: [x])  # transform the rdd
        return rdd_prc_list_trans

    @classmethod
    def __cast_amount(cls, df_apt_prc):
        # 거래금액을 리스트로 받아서 int형으로 casting 진행
        prc_list = df_apt_prc.select(col('거래금액(만원)').alias('amount')).collect()
        prc_list_trans = []
        for prc in prc_list:
            prc_split = prc.amount.split(',')
            tmp = ''
            for s in prc_split:
                tmp += s
            tmp = int(tmp)
            prc_list_trans.append(tmp)
        #print(prc_list_trans[:3], type(prc_list_trans[0]))
        return prc_list_trans

    @classmethod
    def __read_csv_file(cls, loc_codes, i):
        # 지역코드를 이용해 csv파일 읽기
        loc_code = loc_codes[i][0]
        #file_name = 'apart_price_data_' + loc_code + '.csv'
        deal_ymd = cal_std_month(1)  # 저번달 : 202209
        file_name = 'apart_price_data_' + loc_code + '_' + deal_ymd + '.csv'
       
        df_apt_prc = get_spark_session().read.csv(cls.FILE_DIR + file_name, encoding='CP949', header=True)
        # df_apt_prc.show(3)
        # print(df_apt_prc.dtypes)
        return df_apt_prc
            
    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('apartment_sale_price_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls, loc_code):
        log_dict = {
                "is_success": "Fail",
                "type": "apartment_sale_price_transform",
                "loc_code": loc_code
            }
        return log_dict