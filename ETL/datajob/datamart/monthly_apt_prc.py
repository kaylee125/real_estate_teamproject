from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from pyspark.sql.functions import date_format, col, sum, count, round, broadcast
from infra.spark_session import get_spark_session
from infra.util import cal_std_month2


class MonthlyAptPrc:

    @classmethod
    def save(cls):
        df_apt_prc = find_data(DataWarehouse, "REAL_PRC_APT")
        df_apt_prc.createOrReplaceTempView('REAL_PRC_APT')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_apt_prc = df_apt_prc.select(date_format(col('RES_DATE'), 'yyyy-MM').alias('DATE_YM'),
                                        col('AMOUNT'), 
                                        col('REGN_CODE'),
                                        (col('AMOUNT') / col('AREA')).alias('TMP')) \
                                .where(col('DATE_YM') == cal_std_month2(2))
        
        df_loc = df_loc.select(col('LOC_CODE'), col('SIDO_CODE'), col('SIDO').alias('REGN'))

        df_fin = df_apt_prc.join(broadcast(df_loc), df_apt_prc.REGN_CODE == df_loc.LOC_CODE)

        df_fin = df_fin.groupBy([col("DATE_YM"), col('SIDO_CODE'), col("REGN")]) \
                        .agg(sum("AMOUNT"), count("AMOUNT"), sum("TMP"))
        df_fin = df_fin.select(col("DATE_YM"), col("REGN"),
                                round((col("sum(AMOUNT)") / col("count(AMOUNT)")), 0).alias("AVG_PRICE"),
                                round((col("sum(TMP)") / col("count(AMOUNT)")), 0).alias("AVG_PRICE_M2"))

        overwrite_trunc_data(DataMart, df_fin, "MONTHLY_APT_PRC")

        # get_spark_session().sql('''SELECT TO_CHAR(RES_DATE, 'YYYY-MM') AS DATE_YM, SIDO AS REGN,
        #                             ROUND(SUM(AMOUNT) / COUNT(AMOUNT), 0) AS AVG_PRICE,
        #                             ROUND(SUM(AMOUNT / AREA) / COUNT(AMOUNT), 0) AVG_PRICE_M2
        #                         FROM REAL_PRC_APT RPA INNER JOIN LOC ON LOC.LOC_CODE = RPA.REGN_CODE
        #                         GROUP BY TO_CHAR(RES_DATE, 'YYYY-MM'), SIDO_CODE, SIDO
        #                         ORDER BY SIDO_CODE ASC, TO_CHAR(RES_DATE, 'YYYY-MM') DESC''').show(5)
        
