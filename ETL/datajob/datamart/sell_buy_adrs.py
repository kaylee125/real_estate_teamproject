from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session


class AccSellBuyAdrs:
    @classmethod
    def save(cls):
        df_own_addr = find_data(DataWarehouse, "OWN_ADDR")
        df_own_addr.createOrReplaceTempView('OWN_ADDR')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT S1.REGN, SELL_TOT, SELL_RATE, BUY_TOT, BUY_RATE
                                            FROM
                                                (SELECT SIDO AS REGN, SUM(TOT) AS SELL_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS SELL_RATE
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.RES_REGN_CODE = LOC.LOC_CODE
                                                GROUP BY SIDO) S1,
                                                (SELECT SIDO AS REGN, SUM(TOT) AS BUY_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS BUY_RATE
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.BUYER_REGN_CODE = LOC.LOC_CODE
                                                GROUP BY SIDO) S2
                                            WHERE S1.REGN = S2.REGN''')
        #df_fin.show()

        #save_data(DataMart, df_fin, "ACC_SELL_BUY_ADRS")
        overwrite_trunc_data(DataMart, df_fin, "ACC_SELL_BUY_ADRS")


class SellBuySudo:
    @classmethod
    def save(cls):
        df_own_addr = find_data(DataWarehouse, "OWN_ADDR")
        df_own_addr.createOrReplaceTempView('OWN_ADDR')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT S1.SUDO, SELL_TOT, SELL_RATE, BUY_TOT, BUY_RATE
                                            FROM 
                                            (SELECT SUDO, SUM(TOT) AS SELL_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS SELL_RATE
                                            FROM (SELECT CASE WHEN SIDO IN ('서울특별시', '인천광역시', '경기도') THEN '수도권'
                                                    ELSE '비수도권' END AS SUDO, RES_REGN_CODE, TOT
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.RES_REGN_CODE = LOC.LOC_CODE) S1
                                                GROUP BY SUDO) S1,
                                            (SELECT SUDO, SUM(TOT) AS BUY_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS BUY_RATE
                                            FROM (SELECT CASE WHEN SIDO IN ('서울특별시', '인천광역시', '경기도') THEN '수도권'
                                                    ELSE '비수도권' END AS SUDO, BUYER_REGN_CODE, TOT
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.BUYER_REGN_CODE = LOC.LOC_CODE)
                                                GROUP BY SUDO) S2
                                            WHERE S1.SUDO = S2.SUDO''')
        
        #df_fin.show()

        #save_data(DataMart, df_fin, "SELL_BUY_SUDO")
        overwrite_trunc_data(DataMart, df_fin, "SELL_BUY_SUDO")


class SellBuySudoYear:
    @classmethod
    def save(cls):
        df_own_addr = find_data(DataWarehouse, "OWN_ADDR")
        df_own_addr.createOrReplaceTempView('OWN_ADDR')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT S1.SIDO AS REGN, S1.YEAR, SELL_TOT, BUY_TOT
                                        FROM (SELECT SIDO, EXTRACT(YEAR FROM RES_DATE) AS YEAR, SUM(TOT) AS SELL_TOT
                                            FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.RES_REGN_CODE = LOC.LOC_CODE
                                            WHERE SIDO IN ('서울특별시', '인천광역시', '경기도')
                                            GROUP BY SIDO, EXTRACT(YEAR FROM RES_DATE)) S1,
                                            (SELECT SIDO, EXTRACT(YEAR FROM RES_DATE) AS YEAR, SUM(TOT) AS BUY_TOT
                                            FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.BUYER_REGN_CODE = LOC.LOC_CODE
                                            WHERE SIDO IN ('서울특별시', '인천광역시', '경기도')
                                            GROUP BY SIDO, EXTRACT(YEAR FROM RES_DATE)) S2
                                        WHERE S1.SIDO = S2.SIDO AND S1.YEAR = S2.YEAR''')
    
        #df_fin.show()

        #save_data(DataMart, df_fin, "SELL_BUY_SUDO_YEAR")
        overwrite_trunc_data(DataMart, df_fin, "SELL_BUY_SUDO_YEAR")