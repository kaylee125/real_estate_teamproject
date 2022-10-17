from datetime import datetime, timedelta
from rest_framework import viewsets, permissions
from rest_api.models import *
from rest_api.serializers import *
from django.http import JsonResponse
from drf_yasg.utils import swagger_auto_schema
from drf_yasg.openapi import Parameter, Schema, IN_QUERY, TYPE_STRING, TYPE_OBJECT, TYPE_INTEGER, TYPE_NUMBER
from django.core import serializers



def get_queryset_by_date(model, query_params):
    if 'location' not in query_params:
        loc = '서울특별시'
    else:
        loc = query_params['location']

    if ~('start_date' in query_params) and ~('end_date' in query_params):
        queryset = model.objects.filter(regn=loc) \
                                .order_by('-date_ym')
        return queryset

    if 'start_date' in query_params and 'end_date' in query_params:
        start_date = query_params['start_date']
        end_date = query_params['end_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__range=(start_date, end_date)) \
                                .order_by('-date_ym')
        return queryset

    if 'start_date' in query_params:
        start_date = query_params['start_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__gt=start_date) \
                                .order_by('-date_ym')
        return queryset

    if 'end_date' in query_params:
        end_date = query_params['end_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__lt=end_date) \
                                .order_by('-date_ym')
        return queryset

    return queryset





class MonthlyAptPrcViewSet(viewsets.ReadOnlyModelViewSet):

    queryset = MonthlyAptPrc.objects.all()
    serializer_class = MonthlyAptPrcSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="월간 아파트 매매 실거래 가격",
        operation_description="""지역이름 생략 시 전체 데이터를 반환합니다.""",
        #  <br>
        #     시작년월와 끝년월을 모두 생략하면 최근 1년 데이터를 반환합니다. <br>
        #     시작년월만 입력하면 시작날짜부터 저번달까지의 데이터를 반환합니다.<br>
        #     끝년월만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False),
            # Parameter("start_date", IN_QUERY, type=TYPE_STRING,
            #           description="시작년월, (format : yyyy-MM)", required=False),
            # Parameter("end_date", IN_QUERY, type=TYPE_STRING,
            #           description="끝년월, (format : yyyy-MM)", required=False),
        ],
        responses={
            200: Schema(
                'MonthlyAptPrc',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'date_ym': Schema('년월', type=TYPE_STRING),
                    'avg_price': Schema('평균가격', type=TYPE_INTEGER),
                    'avg_price': Schema('제곱면적당 평균가격', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        if 'location' not in query_params:
            queryset = MonthlyAptPrc.objects.all()
        else:
            loc = query_params['location']
            queryset = MonthlyAptPrc.objects.filter(regn=loc).order_by('-date_ym')
        #queryset = get_queryset_by_date(MonthlyAptPrc, query_parmas)
        #print(query_parmas)
        #serialized = serializers.serialize('json', queryset)
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class SidoRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SidoRegist.objects.all()
    serializer_class = SidoRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="전국 17개 광역시도별 등기 수",
        operation_description="""지역이름 생략 시 전체 데이터를 반환합니다. """,
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        ],
        responses={
            200: Schema(
                'SidoRegist',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        if 'location' not in query_params:
            queryset = SidoRegist.objects.all()
        else:
            loc = query_params['location']
            queryset = SidoRegist.objects.filter(regn=loc)
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SeoulGuRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SeoulGuRegist.objects.all()
    serializer_class = SeoulGuRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="서울시 자치구별 부동산 등기 수",
        operation_description="""지역이름 생략 시 전체 데이터를 반환합니다. """,
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(서울시 자치구명) ex) 종로구, 광진구, 강남구...", required=False)
        ],
        responses={
            200: Schema(
                'SeoulGuRegist',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('서울시 자치구명', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        if 'location' not in query_params:
            queryset = SeoulGuRegist.objects.all()
        else:
            loc = query_params['location']
            queryset = SeoulGuRegist.objects.filter(regn=loc)
        #queryset = SeoulGuRegist.objects.filter(regn=loc)
        #queryset = SeoulGuRegist.objects.all()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class OwnRegistTypeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = OwnRegistType.objects.all()
    serializer_class = OwnRegistTypeSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="전국 소유유형별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'OwnRegistType',
                type = TYPE_OBJECT,
                properties={
                    'cls': Schema('소유유형', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = OwnRegistType.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SeoulOwnRegistTypeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SeoulOwnRegistType.objects.all()
    serializer_class = SeoulOwnRegistTypeSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="서울시 소유유형별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SeoulOwnRegistType',
                type = TYPE_OBJECT,
                properties={
                    'cls': Schema('소유유형', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SeoulOwnRegistType.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AgesRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AgesRegist.objects.all()
    serializer_class = AgesRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="전국 연령대별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AgesRegist',
                type = TYPE_OBJECT,
                properties={
                    'ages': Schema('연령대', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AgesRegist.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SeoulAgesRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SeoulAgesRegist.objects.all()
    serializer_class = SeoulAgesRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="서울시 연령대별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SeoulAgesRegist',
                type = TYPE_OBJECT,
                properties={
                    'ages': Schema('연령대', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SeoulAgesRegist.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class SexRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SexRegist.objects.all()
    serializer_class = SexRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="전국 성별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SexRegist',
                type = TYPE_OBJECT,
                properties={
                    'sex': Schema('성별', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SexRegist.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SeoulSexRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SeoulSexRegist.objects.all()
    serializer_class = SeoulSexRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="서울시 성별 등기 수",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SeoulSexRegist',
                type = TYPE_OBJECT,
                properties={
                    'sex': Schema('성별', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SeoulSexRegist.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AccSellBuyAdrsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyAdrs.objects.all()
    serializer_class = AccSellBuyAdrsSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="광역시도별 누적 매도매수량",
        operation_description="""지역이름 생략 시 전체 데이터를 반환합니다. """,
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        ],
        responses={
            200: Schema(
                'AccSellBuyAdrs',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'sell_tot': Schema('매도건수', type=TYPE_INTEGER),
                    'sell_rate': Schema('매도비율', type=TYPE_NUMBER),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        if 'location' not in query_params:
            queryset = AccSellBuyAdrs.objects.all()
        else:
            loc = query_params['location']
            queryset = AccSellBuyAdrs.objects.filter(regn=loc)
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuySudoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuySudo.objects.all()
    serializer_class = SellBuySudoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="수도권 비수도권 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuySudo',
                type = TYPE_OBJECT,
                properties={
                    'sudo': Schema('수도권/비수도권', type=TYPE_STRING),
                    'sell_tot': Schema('매도건수', type=TYPE_INTEGER),
                    'sell_rate': Schema('매도비율', type=TYPE_NUMBER),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuySudo.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuySudoYearViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuySudoYear.objects.all()
    serializer_class = SellBuySudoYearSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="수도권 연도별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuySudoYear',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('서울/경기/인천', type=TYPE_STRING),
                    'year': Schema('연도', type=TYPE_STRING),
                    'sell_tot': Schema('매도건수', type=TYPE_INTEGER),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuySudoYear.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AccSellBuyTypeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyType.objects.all()
    serializer_class = AccSellBuyTypeSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="소유유형별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyType',
                type = TYPE_OBJECT,
                properties={
                    'cls': Schema('소유유형', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyType.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuyTypeYearViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuyTypeYear.objects.all()
    serializer_class = SellBuyTypeYearSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="소유유형별 연도별 누적 매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuyTypeYear',
                type = TYPE_OBJECT,
                properties={
                    'cls': Schema('소유유형', type=TYPE_STRING),
                    'year': Schema('연도', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuyTypeYear.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class AccSellBuyTypeSidoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyTypeSido.objects.all()
    serializer_class = AccSellBuyTypeSidoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="소유유형별 광역시도별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyTypeSido',
                type = TYPE_OBJECT,
                properties={
                    'cls': Schema('소유유형', type=TYPE_STRING),
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyTypeSido.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AccSellBuyAgesViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyAges.objects.all()
    serializer_class = AccSellBuyAgesSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="연령대별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyAges',
                type = TYPE_OBJECT,
                properties={
                    'ages': Schema('연령대', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyAges.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuyAgesYearViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuyAgesYear.objects.all()
    serializer_class = SellBuyAgesYearSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="연령대별 연도별 누적매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuyAgesYear',
                type = TYPE_OBJECT,
                properties={
                    'ages': Schema('연령대', type=TYPE_STRING),
                    'year': Schema('연도', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuyAgesYear.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class AccSellBuyAgesSidoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyAgesSido.objects.all()
    serializer_class = AccSellBuyAgesSidoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="연령대별 광역시도별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyAgesSido',
                type = TYPE_OBJECT,
                properties={
                    'ages': Schema('연령대', type=TYPE_STRING),
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyAgesSido.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AccSellBuySexViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuySex.objects.all()
    serializer_class = AccSellBuySexSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="성별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuySex',
                type = TYPE_OBJECT,
                properties={
                    'sex': Schema('성별', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuySex.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuySexYearViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuySexYear.objects.all()
    serializer_class = SellBuySexYearSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="성별 연도별 누적매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuySexYear',
                type = TYPE_OBJECT,
                properties={
                    'sex': Schema('성별', type=TYPE_STRING),
                    'year': Schema('연도', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuySexYear.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class AccSellBuySexSidoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuySexSido.objects.all()
    serializer_class = AccSellBuySexSidoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="성별 광역시도별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuySexSido',
                type = TYPE_OBJECT,
                properties={
                    'sex': Schema('성별', type=TYPE_STRING),
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuySexSido.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class AccSellBuyForeignViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyForeign.objects.all()
    serializer_class = AccSellBuyForeignSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="외국인국적별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyForeign',
                type = TYPE_OBJECT,
                properties={
                    'foreigner': Schema('외국인국적', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                    'buy_rate': Schema('매수비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyForeign.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class SellBuyForeignYearViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SellBuyForeignYear.objects.all()
    serializer_class = SellBuyForeignYearSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="외국인 국적별 연도별 누적매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'SellBuyForeignYear',
                type = TYPE_OBJECT,
                properties={
                    'foreigner': Schema('외국인국적', type=TYPE_STRING),
                    'year': Schema('연도', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = SellBuyForeignYear.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass

class AccSellBuyForeignSidoViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AccSellBuyForeignSido.objects.all()
    serializer_class = AccSellBuyForeignSidoSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="외국인국적별 광역시도별 누적 매도매수량",
        # operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        # manual_parameters=[
        #     Parameter("location", IN_QUERY, type=TYPE_STRING,
        #               description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시...", required=False)
        # ],
        responses={
            200: Schema(
                'AccSellBuyForeignSido',
                type = TYPE_OBJECT,
                properties={
                    'foreigner': Schema('외국인 국적', type=TYPE_STRING),
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'buy_tot': Schema('매수건수', type=TYPE_INTEGER),
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        # if 'location' not in query_params:
        #     loc = '종로구'
        # else:
        #     loc = query_params['location']
        queryset = AccSellBuyForeignSido.objects.filter()
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass



# # 2022-09-23 조건문안에 return문 안내해야함!!!!!!!!!!!!!!!!!!!
# def get_queryset_by_date(model, query_params):
#     if ~('start_date' in query_params) and ~('end_date' in query_params):
#         queryset = model.objects.filter(std_day__gt=(datetime.today() - timedelta(7)))
#         return queryset
#     if 'start_date' in query_params and 'end_date' in query_params:
#         start_date = query_params['start_date']
#         end_date = query_params['end_date']
#         queryset = model.objects.filter(std_day__gt=start_date).filter(std_day__lt=end_date)
#         return queryset

#     if 'start_date' in query_params:
#         start_date = query_params['start_date']
#         queryset = model.objects.filter(std_day__gt=start_date)
#         return queryset

#     if 'end_date' in query_params:
#         end_date = query_params['end_date']
#         queryset = model.objects.filter(std_day__lt=end_date)
#         return queryset

#     return queryset

# class CoFacilityViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoFacility.objects.all().order_by('-std_day')
#     serializer_class = CoFacilitySerializer
#     permission_classes = [permissions.IsAuthenticated]

#     @swagger_auto_schema(
#         operation_summary="10만명당 다중이용시설의 개수와 10만명 당 코로나 발생자 수",
#         operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
#             시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
#             끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br> """,
#         manual_parameters=[
#             Parameter("start_date", IN_QUERY, type=TYPE_STRING,
#                       description="시작 날짜, (format :yyyy-MM-dd)", required=True),
#             Parameter("end_date", IN_QUERY, type=TYPE_STRING,
#                       description="끝날짜, (format :yyyy-MM-dd)", required=False),
#         ],
#     )
#     def list(self, request):
#         query_parmas = request.query_params
#         queryset = get_queryset_by_date(CoFacility, query_parmas)
#         print(query_parmas)
#         serializer = self.get_serializer(queryset, many=True)
#         return JsonResponse(serializer.data)

#     @swagger_auto_schema(auto_schema=None)
#     def retrieve(self, request):
#         pass

# class CoPopuDensityViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoPopuDensity.objects.all().order_by('-std_day')
#     serializer_class = CoPopuDensitySerializer
#     permission_classes = [permissions.IsAuthenticated]

#     @swagger_auto_schema(
#         operation_summary="인구밀도와 10만명당 코로나 발생 환자 간의 상관관계",
#         operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
#         시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
#         끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
#          """,
#         manual_parameters=[
#             Parameter("start_date", IN_QUERY, type=TYPE_STRING,
#                       description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
#             Parameter("end_date", IN_QUERY, type=TYPE_STRING,
#                       description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
#         ],
#     )

#     def list(self, request):
#         query_parmas = request.query_params
#         queryset = get_queryset_by_date(CoPopuDensity, query_parmas)
#         serializer = self.get_serializer(queryset, many=True)
#         return JsonResponse(serializer.data, safe=False)


# class CoVaccineViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoVaccine.objects.all().order_by('-std_day')
#     serializer_class = CoVaccineSerializer
#     permission_classes = [permissions.IsAuthenticated]

# class CoWeekdayViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoWeekday.objects.all().order_by('-std_day')
#     serializer_class = CoWeekdaySerializer
#     permission_classes = [permissions.IsAuthenticated]
    