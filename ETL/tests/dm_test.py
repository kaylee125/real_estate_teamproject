import unittest
from datajob.datamart.sell_buy_adrs import AccSellBuyAdrs
from datajob.datamart.ages_regist import AgesRegist
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc
from datajob.datamart.sell_buy_adrs import SellBuySudo
from datajob.datamart.sell_buy_adrs import SellBuySudoYear
from datajob.datamart.sell_buy_foreign import AccSellBuyForeignSido, SellBuyForeignYear
from datajob.datamart.sell_buy_sex import SellBuySexYear
from datajob.datamart.sell_buy_type import AccSellBuyType, AccSellBuyTypeSido, SellBuyTypeYear
from datajob.datamart.seoul_gu_regist import SeoulGuRegist
from datajob.datamart.sex_regist import SexRegist
from datajob.datamart.sido_regist import SidoRegist
from datajob.datamart.type_regist import TypeRegist


class MTest(unittest.TestCase):

    def test1(self):
        MonthlyAptPrc.save()
    
    def test2(self):
        TypeRegist.save()

    def test3(self):
        SexRegist.save()

    def test4(self):
        AgesRegist.save()

    def test2(self):
        AccSellBuyAdrs.save()

    def test3(self):
        SellBuySudo.save()

    def test4(self):
        SellBuySudoYear.save()

    def test5(self):
        SeoulGuRegist.save()

    def test6(self):
        SidoRegist.save()

    def test7(self):
        SellBuySexYear.save()

    def test8(self):
        SellBuySexYear.save()

    def test9(self):
        AccSellBuyForeignSido.save()

    def test10(self):
        AccSellBuyType.save()

    def test11(self):
        SellBuyTypeYear.save()

    def test12(self):
        AccSellBuyTypeSido.save()

    def test13(self):
        SellBuyForeignYear.save()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
