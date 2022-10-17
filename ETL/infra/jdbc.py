from enum import Enum
from infra.spark_session import get_spark_session

# 데이터웨어하우스 ENUM
class DataWarehouse(Enum):
    URL='jdbc:oracle:thin:@realestate_high?TNS_ADMIN=/home/big/study/db/Wallet_REALESTATE'
    PROPS={
        'user':'dw_realestate',
        'password':'123qwe!@#QWE'
    }

# 데이터마트 ENUM
class DataMart(Enum):
    URL='jdbc:oracle:thin:@realestate_high?TNS_ADMIN=/home/big/study/db/Wallet_REALESTATE'
    PROPS={
        'user': 'dm_realestate',
        'password': '123qwe!@#QWE'
    }

# 데이터웨어하우스, 데이터마트에 저장하기 위한 함수
def save_data(config, dataframe, table_name):
    dataframe.write.jdbc(url=config.URL.value,
                        table=table_name,
                        mode='append',
                        properties=config.PROPS.value)

# 데이터웨어하우스, 데이터마트에 덮어쓰기 위한 함수
def overwrite_data(config, dataframe, table_name):
    dataframe.write.jdbc(url=config.URL.value,
                        table=table_name,
                        mode='overwrite',
                        properties=config.PROPS.value)

def overwrite_trunc_data(config, dataframe, table_name):
    dataframe.write.option("truncate", "true").jdbc(url=config.URL.value,
                                                    table=table_name,
                                                    mode='overwrite',
                                                    properties=config.PROPS.value)

# 데이터웨어하우스, 데이터마트에서 데이터 가져오기 위한 함수
def find_data(config, table_name):
    return get_spark_session().read.jdbc(url=config.URL.value,
                                        table=table_name,
                                        properties=config.PROPS.value)