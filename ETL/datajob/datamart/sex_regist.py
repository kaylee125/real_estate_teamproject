from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

class SexRegist:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_sex = get_spark_session().sql('''select OWNER_SEX as sex, sum(tot)as tot
                                            ,round((sum(tot)/(select sum(tot) from REALESTATE_OWN)*100),2) as rate
                                            from REALESTATE_OWN
                                            group by OWNER_SEX''')
                                            
        overwrite_trunc_data(DataMart, df_sex,'SEX_REGIST')


class SeoulSexRegist:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_seoul_sex_type = get_spark_session().sql('''select OWNER_SEX as sex,
                                                    sum(tot)as tot, round((sum(tot)/(select sum(tot) from REALESTATE_OWN where regn_code like '11%')*100),2) as rate
                                                    from REALESTATE_OWN ro inner join loc l on l.loc_code=ro.regn_code
                                                    where sido='서울특별시'
                                                    group by OWNER_SEX''')

        overwrite_trunc_data(DataMart, df_seoul_sex_type, 'SEOUL_SEX_REGIST')
