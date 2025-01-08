from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from datetime import datetime

def insert_data(table_name):
    try:
        df = pd.read_csv(rf'/files/{table_name}.csv', sep = ';').drop_duplicates()
    except:
        df = pd.read_csv(rf'/files/{table_name}.csv', sep = ';', encoding = 'cp1252').drop_duplicates()
    for i in df.columns:
        df.rename(columns={i:i.lower()},inplace=True)
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema='stage', if_exists='append',index=False)

days = [str(i).split(' ')[0] for i in pd.date_range(start='1/1/2018', periods=31)]

default_args = {
    "owner" : "postgres",
    "start_date" : datetime(2024, 12, 29),
    "retries" : 2
}

with DAG(
    "insert_stg",
    default_args=default_args,
    description = "test",
    catchup = False,
    schedule = "0 0 * * *"
) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )
    
    ft_balance_f = PythonOperator(
        task_id = "ft_balance_f",
        python_callable = insert_data,
        op_kwargs = {"table_name": "ft_balance_f"}
    )
    
    split = DummyOperator(
        task_id = 'split'
    )
    
    ft_posting_f = PythonOperator(
        task_id = "ft_posting_f",
        python_callable = insert_data,
        op_kwargs = {"table_name": "ft_posting_f"}
    )
    
    
    md_account_d = PythonOperator(
        task_id = "md_account_d",
        python_callable = insert_data,
        op_kwargs = {"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
            task_id = "md_currency_d",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_currency_d"}
        )

    md_exchange_rate_d = PythonOperator(
            task_id = "md_exchange_rate_d",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_exchange_rate_d"}
        )

    md_ledger_account_s = PythonOperator(
            task_id = "md_ledger_account_s",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_ledger_account_s"}
        )
    

    etl_stage = SQLExecuteQueryOperator(
        task_id = 'etl_stage',
        conn_id = 'postgres-db',
        sql = 'CALL ds.etl_stage()'
    )
    
    etl_turnover = SQLExecuteQueryOperator(
        task_id = 'etl_turnover',
        conn_id = 'postgres-db',
        sql = '''CALL dm.fill_account_turnover_t_january()'''
    )
   
    ddl_1 = SQLExecuteQueryOperator(
        task_id = 'ddl_1',
        conn_id = 'postgres-db',
        sql = 'sql/ddl_1.sql'
    )

    # call_procedure_etl_md_account_d = SQLExecuteQueryOperator(
    #     task_id = 'etl_md_account_d',
    #     conn_id = 'postgres-db',
    #     sql = 'CALL ds.etl_md_account_d()'
    # )

    # call_procedure_etl_md_currency_d = SQLExecuteQueryOperator(
    #     task_id = 'etl_md_currency_d',
    #     conn_id = 'postgres-db',
    #     sql = 'CALL ds.etl_md_currency_d()'
    # )

    # call_procedure_etl_ds_postng_f = SQLExecuteQueryOperator(
    #     task_id = 'etl_ds_postng_f',
    #     conn_id = 'postgres-db',
    #     sql = 'CALL ds.etl_ds_postng_f()'
    # )
    
    # call_procedure_etl_ds_balance_f = SQLExecuteQueryOperator(
    #     task_id = 'etl_ds_balance_f',
    #     conn_id = 'postgres-db',
    #     sql = 'CALL ds.etl_ds_balance_f()'
    # )

    # call_procedure_etl_md_ledger_account_s = SQLExecuteQueryOperator(
    #     task_id = 'etl_md_ledger_account_s',
    #     conn_id = 'postgres-db',
    #     sql = 'CALL ds.etl_md_ledger_account_s()'
    # )

    end = DummyOperator(
        task_id = "end"
    )
    
    (
        start
        >> ddl_1 
        >> [md_ledger_account_s, ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_exchange_rate_d]
        >> split
        >> etl_stage 
        >> etl_turnover
        # >> [call_procedure_etl_md_exchange_rate_d, call_procedure_etl_md_account_d
        #     , call_procedure_etl_md_currency_d, call_procedure_etl_ds_postng_f, call_procedure_etl_ds_balance_f
        #     , call_procedure_etl_md_ledger_account_s]
        >> end
    )