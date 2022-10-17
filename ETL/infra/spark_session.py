import findspark  # python 실행 시 spark객체를 알아서 찾기 위한 라이브러리
from pyspark.sql import SparkSession

def get_spark_session():
    findspark.init()  # 앱에서 spark를 찾기 위한 진입점 호출
    return SparkSession.builder.getOrCreate() # SparkSession객체 반환 / 이미 있으면 그냥 가져오고(get), 없으면 새로만들어서(create) 반환
