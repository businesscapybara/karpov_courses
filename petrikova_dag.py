from datetime import datetime, timedelta, date
import pandas as pd
import pandahouse as ph
import config

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator

connection_simulator = {'host': config.db_host,
                      'database':config.db_database,
                      'user': config.db_user, 
                      'password': config.db_password
                     }

connection_test = {
    'host': config.db_test_host,
    'password': config.db_test_password,
    'user': config.db_test_user,
    'database': config.db_test_database
}


def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

default_args = {
'owner': 'a-petrikova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 11, 5),
}

schedule_interval = '0 13 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_apetrikova():

    @task()
    def extract_actions():
        query = """
            select 
            toDate(time) as event_date
            , user_id as uid
            , countIf(action = 'like') as likes
            , countIf(action = 'view') as views
            , if(gender = 1, 'male', 'female') as gender
            , age
            , os
            from simulator_20221020.feed_actions
            where toDate(time) = today() - 1
            group by event_date, uid, gender, age, os
            """
        df_actions = ch_get_df(query, connection_simulator)
        return df_actions
    
    @task()
    def extract_messages():
        query = """
            with

            t1 as
            (
            select 
            toDate(time) as event_date
            , user_id as uid
            , count(distinct reciever_id) as users_sent
            , count(reciever_id) as messages_sent 
            from simulator_20221020.message_actions
            where toDate(time) = today() - 1
            group by event_date, uid
            order by event_date asc
            ),

            t2 as 
            (
            select 
            toDate(time) as event_date
            , reciever_id as uid
            , count(distinct user_id) as users_received
            , count(user_id) as messages_received 
            from simulator_20221020.message_actions
            where toDate(time) = today() - 1
            group by event_date, uid
            order by event_date asc
            )

            select 
            event_date
            , uid
            , users_sent
            , messages_sent
            , users_received
            , messages_received
            from t1
            left join t2 on t1.uid = t2.uid and t1.event_date = t2.event_date
        """
        df_messages = ch_get_df(query, connection_simulator)
        return df_messages

    @task
    def merge_df (df_actions, df_messages):
        df = pd.merge(df_actions, df_messages, how = 'left', on = ['uid', 'event_date'])
        df = df.fillna(0)
        return df
    
    @task
    def transfrom_os(df):
        df_os = df[['event_date', 'os', 'users_sent', 'messages_sent', 'users_received', 'messages_received', 'likes', 'views']]\
                                .groupby(['event_date', 'os']).sum().reset_index()
        return df_os

    @task
    def transfrom_gender(df):
        df_gender = df[['event_date', 'gender', 'users_sent', 'messages_sent', 'users_received', 'messages_received', 'likes', 'views']].groupby(['event_date', 'gender']).sum().reset_index()
        return df_gender
    
    @task
    def transfrom_age(df):
        df_age = df[['event_date', 'age', 'users_sent', 'messages_sent', 'users_received', 'messages_received', 'likes', 'views']].groupby(['event_date', 'age']).sum().reset_index()
        return df_age
    
    @task
    def loadable_table(df_os, df_gender, df_age):
        df_os['dimension'] = 'os'
        df_gender['dimension'] = 'gender'
        df_age['dimension'] = 'age'
        
        df_os.rename(columns={'os':'dimension_value'}, inplace=True)
        df_gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        df_age.rename(columns={'age':'dimension_value'}, inplace=True)
        
        df_loadable_table = pd.concat([df_os, df_gender, df_age], ignore_index = True)
        
        #print(df_loadable_table.head())
        return df_loadable_table

    @task
    def create_load(df_loadable_table):
        query = '''
        CREATE TABLE IF NOT EXISTS test.a_petrikova
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''
        ph.execute(query, connection=connection_test)
        
        query = '''
                ALTER TABLE test.a_petrikova 
                DELETE 
                WHERE event_date = today() - 1
                '''
        ph.execute(query, connection=connection_test)
        print(df_loadable_table)
        df_loadable_table = df_loadable_table.astype({'views':'int64',
                                              'likes':'int64',
                                              'messages_received':'int64',
                                              'messages_sent':'int64',
                                              'users_received':'int64',
                                              'users_sent':'int64'},errors='ignore')

        df_loadable_table = pd.DataFrame(df_loadable_table, columns=['event_date','dimension',
                                                             'dimension_value','views','likes',
                                                             'messages_received','messages_sent'
                                                             ,'users_received','users_sent'])
        
        ph.to_clickhouse(df_loadable_table,'a_petrikova',
                         connection=connection_test, index=False)

        print('Данные загружены: ')
        print(df_loadable_table)
        
#extract
    df_actios = extract_actions()
    df_messages = extract_messages()
    
#transform
    df  = merge_df(df_actios, df_messages)
    df_os = transfrom_os(df)
    df_age = transfrom_age(df)
    df_gender = transfrom_gender(df)
    df_loadable_table = loadable_table(df_os, df_gender, df_age)
    create_load(df_loadable_table)
    
    
dag_apetrikova = dag_apetrikova()