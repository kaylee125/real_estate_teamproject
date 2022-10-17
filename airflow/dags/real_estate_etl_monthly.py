from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'real_estate_etl_monthly',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['eunj9920@naver.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,  # 재시도 횟수
        'retry_delay': timedelta(minutes=30),  # 재시도 딜레이 - 3분
    },
    description = 'Real Estate ETL Project',
    schedule = relativedelta(months=1), # 반복날짜 - 1달마다
    start_date = datetime(2022, 10, 6, 4, 30),  # 시작날짜
    catchup=False,
    tags=['real_estate_etl_monthly'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # 태스크들 추가

    ########### Extract ###########
    t1 = BashOperator(
        task_id='extract_apartment_sale_price',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract apartment_sale_price',
    )

    t2 = BashOperator(
        task_id='extract_real_estate_own',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract real_estate_own',
    )

    ########### Transform ###########
    t5 = BashOperator(
        task_id='transform_apartment_sale_price',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform apartment_sale_price',
    )

    t6 = BashOperator(
        task_id='transform_real_estate_own',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform real_estate_own',
    )

    ########### DataMart ###########
    t9 = BashOperator(
        task_id='datamart_monthly_apt_prc',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart monthly_apt_prc',
    )

    t10 = BashOperator(
        task_id='datamart_seoul_gu_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart seoul_gu_regist',
    )

    t11 = BashOperator(
        task_id='datamart_sido_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sido_regist',
    )

    t12 = BashOperator(
        task_id='datamart_type_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart type_regist',
    )

    t13 = BashOperator(
        task_id='datamart_seoul_type_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart seoul_type_regist',
    )

    t14 = BashOperator(
        task_id='datamart_ages_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart ages_regist',
    )

    t15 = BashOperator(
        task_id='datamart_seoul_ages_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart seoul_ages_regist',
    )

    t16 = BashOperator(
        task_id='datamart_sex_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sex_regist',
    )

    t17 = BashOperator(
        task_id='datamart_seoul_sex_regist',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart seoul_sex_regist',
    )


    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    # 태스크 우선순위 설정
    # extract는 병렬로, transform과 datamart는 직렬로
    # task_1 >> [task_2 , task_3]
    # task_2 >> [task_4, task_5]
    # task_3 >> [task_4, task_5]
    # [task_4 , task_5 ] >> task_6
    t1 >> t5 >> t9
    t2 >> t6 >> [t10, t11, t12, t13, t14, t15, t16, t17]
    #[t1, t2, t3, t4] >> [t5, t6, t7, t8] >> [t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23]