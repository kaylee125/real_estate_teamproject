from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

class AccSellBuyType:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")
        own_type = get_spark_session().sql("""select OWNER_CLS as CLS, sum(TOT) as BUY_TOT ,
                                                round((sum(TOT)/(select sum(TOT) from types)*100),1) as BUY_RATE
                                                from types group by OWNER_CLS""")
        #save_data(DataMart, own_type, "ACC_SELL_BUY_TYPE")
        overwrite_trunc_data(DataMart, own_type, "ACC_SELL_BUY_TYPE")

class SellBuyTypeYear:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")
        types_year = get_spark_session().sql("""select OWNER_CLS as CLS , DATE_FORMAT(RES_DATE,'y') AS YEAR , SUM(TOT) AS BUY_TOT
                                            from types
                                            GROUP BY DATE_FORMAT(RES_DATE,'y'), OWNER_CLS""")
        #save_data(DataMart, types_year, "SELL_BUY_TYPE_YEAR")
        overwrite_trunc_data(DataMart, types_year, "SELL_BUY_TYPE_YEAR")


class AccSellBuyTypeSido:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')
        
        type_sido = get_spark_session().sql("""select OWNER_CLS as CLS, sum(TOT) as BUY_TOT , SIDO as REGN
                                            from types INNER JOIN LOC ON types.RES_REGN_CODE = LOC.LOC_CODE
                                            group by OWNER_CLS, SIDO
                                            order by CLS;""")
        #save_data(DataMart, type_sido, "ACC_SELL_BUY_TYPE_SIDO")
        overwrite_trunc_data(DataMart, type_sido, "ACC_SELL_BUY_TYPE_SIDO")
