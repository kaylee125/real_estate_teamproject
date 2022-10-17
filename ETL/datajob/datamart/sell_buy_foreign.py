from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

class AccSellBuyForeign:
    @classmethod
    def save(cls):
        foreigner = find_data(DataWarehouse, "OWN_FOREIGNER")
        foreigner.createOrReplaceTempView("foreigner")
        own_foreigner = get_spark_session().sql("""select BUYER_NATION as FOREIGNER, sum(TOT) as BUY_TOT , round((sum(TOT)/(select sum(TOT) from foreigner)*100),1) as BUY_RATE 
                                                from foreigner 
                                                group by BUYER_NATION""")
        #save_data(DataMart, own_foreigner, 'ACC_SELL_BUY_FOREIGN')
        overwrite_trunc_data(DataMart, own_foreigner, 'ACC_SELL_BUY_FOREIGN')


class SellBuyForeignYear:
    @classmethod
    def save(cls):
        foreigner = find_data(DataWarehouse, "OWN_FOREIGNER")
        foreigner.createOrReplaceTempView("foreigner")
        foreigner_year = get_spark_session().sql("""select BUYER_NATION AS FOREIGNER , DATE_FORMAT(RES_DATE,'y') AS YEAR , SUM(TOT) AS BUY_TOT
                                                from foreigner 
                                                GROUP BY DATE_FORMAT(RES_DATE,'y'), BUYER_NATION""")
        #save_data(DataMart, foreigner_year, 'SELL_BUY_FOREIGN_YEAR')
        overwrite_trunc_data(DataMart, foreigner_year, 'SELL_BUY_FOREIGN_YEAR')


class AccSellBuyForeignSido:
    @classmethod
    def save(cls):
        foreigner = find_data(DataWarehouse, "OWN_FOREIGNER")
        foreigner.createOrReplaceTempView("foreigner")

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')
        
        foreigner_sido = get_spark_session().sql("""select BUYER_NATION as FOREIGNER, SIDO as REGN, sum(TOT) as BUY_TOT
                                                    from foreigner INNER JOIN LOC ON foreigner.RES_REGN_CODE = LOC.LOC_CODE
                                                    group by BUYER_NATION, SIDO order by FOREIGNER""")
        #save_data(DataMart, foreigner_sido , 'ACC_SELL_BUY_FOREIGN_SIDO')
        overwrite_trunc_data(DataMart, foreigner_sido, 'ACC_SELL_BUY_FOREIGN_SIDO')
  