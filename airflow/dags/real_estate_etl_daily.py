from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'real_estate_etl_daily',
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
    schedule = timedelta(days=1), # 반복날짜 - 1일마다
    start_date = datetime(2022, 10, 6, 0, 30),  # 시작날짜
    catchup=False,
    tags=['real_estate_etl_daily'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # 태스크들 추가

    start = BashOperator(
        task_id='start', 
        bash_command="date"
    )

    ########### Extract ###########
    t1 = BashOperator(
        task_id='extract_own_addr',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract own_addr',
    )

    t2 = BashOperator(
        task_id='extract_own_sex_age',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract own_sex_age',
    )

    t3 = BashOperator(
        task_id='extract_own_foreigner',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract own_foreigner',
    )

    t4 = BashOperator(
        task_id='extract_own_type',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py extract own_type',
    )

    ########### Transform ###########
    t5 = BashOperator(
        task_id='transform_own_addr',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform own_addr',
    )

    t6 = BashOperator(
        task_id='transform_own_sex_age',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform own_sex_age',
    )

    t7 = BashOperator(
        task_id='transform_own_foreigner',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform own_foreigner',
    )

    t8 = BashOperator(
        task_id='transform_own_type',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py transform own_type',
    )

    ########### DataMart ###########
    t9 = BashOperator(
        task_id='datamart_acc_sell_buy_adrs',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_adrs',
    )

    t10 = BashOperator(
        task_id='datamart_sell_buy_sudo',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_sudo',
    )

    t11 = BashOperator(
        task_id='datamart_sell_buy_sudo_year',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_sudo_year',
    )

    t12 = BashOperator(
        task_id='datamart_acc_sell_buy_ages',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_ages',
    )

    t13 = BashOperator(
        task_id='datamart_sell_buy_ages_year',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_ages_year',
    )

    t14 = BashOperator(
        task_id='datamart_acc_sell_buy_ages_sido',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_ages_sido',
    )

    t15 = BashOperator(
        task_id='datamart_acc_sell_buy_foreign',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_foreign',
    )

    t16 = BashOperator(
        task_id='datamart_sell_buy_foreign_year',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_foreign_year',
    )

    t17 = BashOperator(
        task_id='datamart_acc_sell_buy_foreign_sido',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_foreign_sido',
    )

    t18 = BashOperator(
        task_id='datamart_acc_sell_buy_sex',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_sex',
    )

    t19 = BashOperator(
        task_id='datamart_sell_buy_sex_year',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_sex_year',
    )

    t20 = BashOperator(
        task_id='datamart_acc_sell_buy_sex_sido',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_sex_sido',
    )

    t21 = BashOperator(
        task_id='datamart_acc_sell_buy_type',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_type',
    )

    t22 = BashOperator(
        task_id='datamart_sell_buy_type_year',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart sell_buy_type_year',
    )

    t23 = BashOperator(
        task_id='datamart_acc_sell_buy_type_sido',
        cwd='/home/big/study/ETL',
        bash_command='python3 main.py datamart acc_sell_buy_type_sido',
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
    #t1
    start >> [t1, t2, t3, t4]
    t1 >> t5 >> [t9, t10, t11]
    t2 >> t6 >> [t12, t13, t14, t18, t19, t20]
    t3 >> t7 >> [t15, t16, t17]
    t4 >> t8 >> [t21, t22, t23]
    #[t1, t2, t3, t4] >> [t5, t6, t7, t8] >> [t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23]