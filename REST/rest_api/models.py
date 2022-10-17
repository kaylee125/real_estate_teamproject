# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AccSellBuyAdrs(models.Model):
    asba_idx = models.BigAutoField(primary_key=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    sell_tot = models.BigIntegerField(blank=True, null=True)
    sell_rate = models.FloatField(blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_adrs'


class AccSellBuyAges(models.Model):
    asba_idx = models.BigAutoField(primary_key=True)
    ages = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_ages'


class AccSellBuyAgesSido(models.Model):
    asbas_idx = models.BigAutoField(primary_key=True)
    ages = models.CharField(max_length=30, blank=True, null=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_ages_sido'


class AccSellBuyForeign(models.Model):
    asbf_idx = models.BigAutoField(primary_key=True)
    foreigner = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_foreign'


class AccSellBuyForeignSido(models.Model):
    asbfs_idx = models.BigAutoField(primary_key=True)
    foreigner = models.CharField(max_length=30, blank=True, null=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_foreign_sido'


class AccSellBuySex(models.Model):
    asbs_idx = models.BigAutoField(primary_key=True)
    sex = models.CharField(max_length=10, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_sex'


class AccSellBuySexSido(models.Model):
    asbss_idx = models.BigAutoField(primary_key=True)
    sex = models.CharField(max_length=10, blank=True, null=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_sex_sido'


class AccSellBuyType(models.Model):
    asbt_idx = models.BigAutoField(primary_key=True)
    cls = models.CharField(max_length=100, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_type'


class AccSellBuyTypeSido(models.Model):
    asbts_idx = models.BigAutoField(primary_key=True)
    cls = models.CharField(max_length=100, blank=True, null=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'acc_sell_buy_type_sido'


class AgesRegist(models.Model):
    ar_idx = models.BigAutoField(primary_key=True)
    ages = models.CharField(max_length=30, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ages_regist'


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    name = models.CharField(max_length=255, blank=True, null=True)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128, blank=True, null=True)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150, blank=True, null=True)
    first_name = models.CharField(max_length=150, blank=True, null=True)
    last_name = models.CharField(max_length=150, blank=True, null=True)
    email = models.CharField(max_length=254, blank=True, null=True)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class AuthtokenToken(models.Model):
    key = models.CharField(primary_key=True, max_length=40)
    created = models.DateTimeField()
    user = models.OneToOneField(AuthUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'authtoken_token'


class DjangoAdminLog(models.Model):
    action_time = models.DateTimeField()
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200, blank=True, null=True)
    action_flag = models.IntegerField()
    change_message = models.TextField(blank=True, null=True)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100, blank=True, null=True)
    model = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    id = models.BigAutoField(primary_key=True)
    app = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField(blank=True, null=True)
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'


class MonthlyAptPrc(models.Model):
    map_idx = models.BigAutoField(primary_key=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    date_ym = models.CharField(max_length=10, blank=True, null=True)
    avg_price = models.BigIntegerField(blank=True, null=True)
    avg_price_m2 = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'monthly_apt_prc'


class OwnRegistType(models.Model):
    ort_idx = models.BigAutoField(primary_key=True)
    cls = models.CharField(max_length=100, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'own_regist_type'


class SellBuyAgesYear(models.Model):
    sbay_idx = models.BigAutoField(primary_key=True)
    ages = models.CharField(max_length=30, blank=True, null=True)
    year = models.CharField(max_length=5, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_ages_year'


class SellBuyForeignYear(models.Model):
    sbfy_idx = models.BigAutoField(primary_key=True)
    foreigner = models.CharField(max_length=30, blank=True, null=True)
    year = models.CharField(max_length=5, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_foreign_year'


class SellBuySexYear(models.Model):
    sbsy_idx = models.BigAutoField(primary_key=True)
    sex = models.CharField(max_length=10, blank=True, null=True)
    year = models.CharField(max_length=5, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_sex_year'


class SellBuySudo(models.Model):
    sbs_idx = models.BigAutoField(primary_key=True)
    sudo = models.CharField(max_length=20, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)
    buy_rate = models.FloatField(blank=True, null=True)
    sell_tot = models.BigIntegerField(blank=True, null=True)
    sell_rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_sudo'


class SellBuySudoYear(models.Model):
    sbsy_idx = models.BigAutoField(primary_key=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    year = models.CharField(max_length=5, blank=True, null=True)
    sell_tot = models.BigIntegerField(blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_sudo_year'


class SellBuyTypeYear(models.Model):
    sbty_idx = models.BigAutoField(primary_key=True)
    cls = models.CharField(max_length=100, blank=True, null=True)
    year = models.CharField(max_length=5, blank=True, null=True)
    buy_tot = models.BigIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sell_buy_type_year'


class SeoulAgesRegist(models.Model):
    sar_idx = models.BigAutoField(primary_key=True)
    ages = models.CharField(max_length=30, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'seoul_ages_regist'


class SeoulGuRegist(models.Model):
    sgr_idx = models.BigAutoField(primary_key=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'seoul_gu_regist'


class SeoulOwnRegistType(models.Model):
    sort_idx = models.BigAutoField(primary_key=True)
    cls = models.CharField(max_length=100, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'seoul_own_regist_type'


class SeoulSexRegist(models.Model):
    ssr_idx = models.BigAutoField(primary_key=True)
    sex = models.CharField(max_length=20, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'seoul_sex_regist'


class SexRegist(models.Model):
    sr_idx = models.BigAutoField(primary_key=True)
    sex = models.CharField(max_length=20, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sex_regist'


class SidoRegist(models.Model):
    sr_idx = models.BigAutoField(primary_key=True)
    regn = models.CharField(max_length=30, blank=True, null=True)
    tot = models.BigIntegerField(blank=True, null=True)
    rate = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'sido_regist'
