from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session


class SidoRegist:

    @classmethod
    def save(cls):
        df_re_own = find_data(DataWarehouse, "REALESTATE_OWN")
        df_re_own.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT LOC.SIDO AS REGN,
                                                SUM(TOT) AS TOT,
                                                ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM REALESTATE_OWN) * 100), 1)  AS RATE
                                            FROM REALESTATE_OWN RO INNER JOIN LOC ON LOC.LOC_CODE = RO.REGN_CODE
                                            GROUP BY SIDO''')
        overwrite_trunc_data(DataMart, df_fin, "SIDO_REGIST")