from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

class AgesRegist:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')  

        df_age = get_spark_session().sql('''select OWNER_AGES as AGES, sum(tot)as TOT
                                        ,round((sum(tot)/(select sum(tot) from REALESTATE_OWN)*100),2) as RATE
                                        from REALESTATE_OWN
                                        group by OWNER_AGES''')
                                            
        overwrite_trunc_data(DataMart, df_age,'AGES_REGIST')


class SeoulAgesRegist:
    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_seoul_age=get_spark_session().sql('''select OWNER_AGES as AGES, sum(tot)as TOT
                                            ,round((sum(tot)/(select sum(tot) from REALESTATE_OWN where regn_code like '11%')*100),2) as RATE
                                            from REALESTATE_OWN ro inner join loc l on l.loc_code=ro.regn_code
                                            where sido='서울특별시'
                                            group by OWNER_AGES''')
                                    
        overwrite_trunc_data(DataMart, df_seoul_age, 'SEOUL_AGES_REGIST')
