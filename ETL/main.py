#!/usr/bin/env python3
import sys
from datajob.datamart.ages_regist import AgesRegist, SeoulAgesRegist
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc
from datajob.datamart.sell_buy_adrs import AccSellBuyAdrs, SellBuySudo, SellBuySudoYear
from datajob.datamart.sell_buy_ages import AccSellBuyAges, AccSellBuyAgesSido, SellBuyAgesYear
from datajob.datamart.sell_buy_foreign import AccSellBuyForeign, AccSellBuyForeignSido, SellBuyForeignYear
from datajob.datamart.sell_buy_sex import AccSellBuySex, AccSellBuySexSido, SellBuySexYear
from datajob.datamart.sell_buy_type import AccSellBuyType, AccSellBuyTypeSido, SellBuyTypeYear
from datajob.datamart.seoul_gu_regist import SeoulGuRegist
from datajob.datamart.sex_regist import SeoulSexRegist, SexRegist
from datajob.datamart.sido_regist import SidoRegist
from datajob.datamart.type_regist import SeoulTypeRegist, TypeRegist
from datajob.etl.extract.apartment_sale_price import ApartmentSalePrice

from datajob.etl.extract.local_code import LocalCode
from datajob.etl.extract.own_addr import OwnTransferByAddress
from datajob.etl.extract.own_sex_age import OwnTransferByGenderAge
from datajob.etl.extract.own_type import OwnTransferByLocalForeignerCorp
from datajob.etl.extract.own_foreigner import OwnTransferByNationality
from datajob.etl.extract.real_estate_own import RealEstateOwnExtractor
from datajob.etl.transform.apartment_sale_price import ApartmentSalePriceTransformer
from datajob.etl.transform.local_code import LocalCodeTransformer
from datajob.etl.transform.own_addr import OwnAddrTransformer
from datajob.etl.transform.own_foreigner import OwnForeignerTransformer
from datajob.etl.transform.own_sex_age import OwnSexAgeTransformer
from datajob.etl.transform.own_type import OwnTypeTransformer
from datajob.etl.transform.real_estate_own import RealEstateOwnTransformer


def transform_execute():
    ApartmentSalePriceTransformer.transform()
    RealEstateOwnTransformer.transform()
    OwnAddrTransformer.transform()
    OwnSexAgeTransformer.transform()
    OwnForeignerTransformer.transform()
    OwnTypeTransformer.transform()

def datamart_execute():
    MonthlyAptPrc.save()
    SeoulGuRegist.save()
    SidoRegist.save

# 모듈이름과, 모듈에서 호출하는 함수를 key-value로 매칭
def main():
    works = {
        'extract': {
            'local_code': LocalCode.extract_data,  # 함수객체 저장
            'apartment_sale_price': ApartmentSalePrice.extract_data,
            'real_estate_own': RealEstateOwnExtractor.extract_data,
            'own_addr': OwnTransferByAddress.extract_data,
            'own_sex_age': OwnTransferByGenderAge.extract_data,
            'own_type': OwnTransferByLocalForeignerCorp.extract_data,
            'own_foreigner': OwnTransferByNationality.extract_data
        },
        'transform': {
            'execute': transform_execute,
            'local_code': LocalCodeTransformer.transform,  # 함수객체 저장
            'apartment_sale_price': ApartmentSalePriceTransformer.transform,
            'real_estate_own': RealEstateOwnTransformer.transform,
            'own_addr': OwnAddrTransformer.transform,
            'own_sex_age': OwnSexAgeTransformer.transform,
            'own_foreigner': OwnForeignerTransformer.transform,
            'own_type': OwnTypeTransformer.transform
        },
        'datamart': {
            'execute': datamart_execute,
            'monthly_apt_prc': MonthlyAptPrc.save,

            'seoul_gu_regist': SeoulGuRegist.save,
            'sido_regist': SidoRegist.save,

            'type_regist': TypeRegist.save,
            'seoul_type_regist': SeoulTypeRegist.save,

            'ages_regist': AgesRegist.save,
            'seoul_ages_regist': SeoulAgesRegist.save,

            'sex_regist': SexRegist.save,
            'seoul_sex_regist': SeoulSexRegist.save,

            'acc_sell_buy_adrs': AccSellBuyAdrs.save,
            'sell_buy_sudo': SellBuySudo.save,
            'sell_buy_sudo_year': SellBuySudoYear.save,

            'acc_sell_buy_ages': AccSellBuyAges.save,
            'sell_buy_ages_year': SellBuyAgesYear.save,
            'acc_sell_buy_ages_sido': AccSellBuyAgesSido.save,

            'acc_sell_buy_foreign': AccSellBuyForeign.save,
            'sell_buy_foreign_year': SellBuyForeignYear.save,
            'acc_sell_buy_foreign_sido': AccSellBuyForeignSido.save,

            'acc_sell_buy_sex': AccSellBuySex.save,
            'sell_buy_sex_year': SellBuySexYear.save,
            'acc_sell_buy_sex_sido': AccSellBuySexSido.save,

            'acc_sell_buy_type': AccSellBuyType.save,
            'sell_buy_type_year': SellBuyTypeYear.save,
            'acc_sell_buy_type_sido': AccSellBuyTypeSido.save
        }
    }
    return works

#works = main(transform_execute, datamart_execute)
works = main()

if __name__ == "__main__":
    # python3 main.py arg1 arg2
    # 값을 받을 인자는 2개임
    # 인자1 : 작업(extract, transform, datamart)
    # 인자2 : 저장할 위치(테이블)
    # ex) python3 main.py extract corona_api

    args = sys.argv
    
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다')
    
    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    if args[2] not in works[args[1]].keys():
        raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    work = works[args[1]][args[2]]
    work()  # 함수객체를 이용해 함수 호출
    
