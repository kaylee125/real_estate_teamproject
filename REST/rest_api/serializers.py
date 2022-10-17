from rest_framework import serializers
from rest_api.models import *


class MonthlyAptPrcSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = MonthlyAptPrc
        fields = ['regn', 'date_ym', 'avg_price', 'avg_price_m2']



class SidoRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SidoRegist
        fields = ['regn', 'tot', 'rate']

class SeoulGuRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SeoulGuRegist
        fields = ['regn', 'tot', 'rate']



class OwnRegistTypeSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = OwnRegistType
        fields = ['cls', 'tot', 'rate']

class SeoulOwnRegistTypeSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SeoulOwnRegistType
        fields = ['cls', 'tot', 'rate']



class AgesRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AgesRegist
        fields = ['ages', 'tot', 'rate']

class SeoulAgesRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SeoulAgesRegist
        fields = ['ages', 'tot', 'rate']



class SexRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SexRegist
        fields = ['sex', 'tot', 'rate']

class SeoulSexRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SeoulSexRegist
        fields = ['sex', 'tot', 'rate']



class AccSellBuyAdrsSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyAdrs
        fields = ['regn', 'sell_tot', 'sell_rate', 'buy_tot', 'buy_rate']

class SellBuySudoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuySudo
        fields = ['sudo', 'buy_tot', 'buy_rate', 'sell_tot', 'sell_rate']

class SellBuySudoYearSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuySudoYear
        fields = ['regn', 'year', 'sell_tot', 'buy_tot']



class AccSellBuyTypeSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyType
        fields = ['cls', 'buy_tot', 'buy_rate']

class SellBuyTypeYearSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuyTypeYear
        fields = ['cls', 'year', 'buy_tot']

class AccSellBuyTypeSidoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyTypeSido
        fields = ['cls', 'regn', 'buy_tot']



class AccSellBuyAgesSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyAges
        fields = ['ages', 'buy_tot', 'buy_rate']

class SellBuyAgesYearSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuyAgesYear
        fields = ['ages', 'year', 'buy_tot']

class AccSellBuyAgesSidoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyAgesSido
        fields = ['ages', 'regn', 'buy_tot']



class AccSellBuySexSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuySex
        fields = ['sex', 'buy_tot', 'buy_rate']

class SellBuySexYearSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuySexYear
        fields = ['sex', 'year', 'buy_tot']

class AccSellBuySexSidoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuySexSido
        fields = ['sex', 'regn', 'buy_tot']



class AccSellBuyForeignSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyForeign
        fields = ['foreigner', 'buy_tot', 'buy_rate']

class SellBuyForeignYearSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SellBuyForeignYear
        fields = ['foreigner', 'year', 'buy_tot']

class AccSellBuyForeignSidoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AccSellBuyForeignSido
        fields = ['foreigner', 'regn', 'buy_tot']



# class CoFacilitySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoFacility
#         fields = ['loc', 'fac_popu', 'qur_rate', 'std_day']


# class CoPopuDensitySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoPopuDensity
#         fields = ['loc', 'popu_density', 'qur_rate', 'std_day']


# class CoVaccineSerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoVaccine
#         fields = ['loc', 'v1th_rate', 'v2th_rate', 'v3th_rate', 'v4th_rate', 'qur_rate', 'std_day']


# class CoWeekdaySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoWeekday
#         fields = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri','sat','std_day']
