from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

class AccSellBuySex:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        sex = get_spark_session().sql("""select BUYER_SEX as SEX, sum(TOT) as BUY_TOT ,
                                        round((sum(TOT)/(select sum(TOT) from sex_ages)*100),1) as BUY_RATE
                                        from sex_ages group by BUYER_SEX""")
        #save_data(DataMart, sex, "ACC_SELL_BUY_SEX")
        overwrite_trunc_data(DataMart, sex, "ACC_SELL_BUY_SEX")


class SellBuySexYear:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        # sex_year = get_spark_session().sql("""select BUYER_SEX as SEX, SUM(TOT) AS BUY_TOT,
        #                                 (select year(res_date) from sex_ages group by year(res_date)) as YEAR
        #                                 from sex_ages group by BUYER_SEX""")
        sex_year = get_spark_session().sql("""select BUYER_SEX as SEX , DATE_FORMAT(RES_DATE,'y') AS YEAR , SUM(TOT) AS BUY_TOT
                                            from sex_ages 
                                            GROUP BY DATE_FORMAT(RES_DATE,'y'), BUYER_SEX""")

        #save_data(DataMart, sex_year, "SELL_BUY_SEX_YEAR")
        overwrite_trunc_data(DataMart, sex_year, "SELL_BUY_SEX_YEAR")


class AccSellBuySexSido:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        sex_sido = get_spark_session().sql("""select BUYER_SEX as SEX, sum(TOT) as BUY_TOT, SIDO as REGN
                                                from sex_ages inner join LOC on sex_ages.RES_REGN_CODE = LOC.LOC_CODE
                                                group by BUYER_SEX, SIDO 
                                                order by sex""")
        #save_data(DataMart, sex_sido, "ACC_SELL_BUY_SEX_SIDO")
        overwrite_trunc_data(DataMart, sex_sido, "ACC_SELL_BUY_SEX_SIDO")