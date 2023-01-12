import datetime as dt
from datetime import datetime, timedelta, date

import telegram
import pandas as pd
import pandahouse as ph
import config


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator


connection = {'host': config.db_host,
                      'database':config.db_database,
                      'user': config.db_user, 
                      'password': config.db_password
                     }

bot = telegram.Bot(token = config.bot_token)

def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

default_args = {
'owner': 'a-petrikova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes = 5),
'start_date': datetime(2022, 11, 20, 11, 0),
}

schedule_interval = '10,25,40,55 * * * *'

def execute_dau():
    query = '''
        with

        feed_msg as

        (select 
            user_id
            , time
            , source
        from simulator_20221020.feed_actions 
        union all 
        select 
            user_id
            , time
            , source
        from simulator_20221020.message_actions)

        select 
             toDayOfWeek(time) as week_day
            , toHour(time) as hour
            , toMinute(toStartOfFifteenMinutes(time)) as minute
            , toStartOfFifteenMinutes(time) as date
            , count(distinct user_id) as metric
        from feed_msg
        group by week_day, hour, minute, date
        having toDayOfWeek(time) = toDayOfWeek(now())
        and toHour(time) * 60 + toMinute(time) >= toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now()))
        and toHour(time) * 60 + toMinute(time) < toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now())) + 15
        order by week_day, hour, minute, date
                '''
    dau = ch_get_df(query, connection)
    return dau

def execute_likes():
    query = '''
        select 
             toDayOfWeek(time) as week_day
            , toHour(time) as hour
            , toMinute(toStartOfFifteenMinutes(time)) as minute
            , toStartOfFifteenMinutes(time) as date
            , countIf(action = 'like') as metric
         from simulator_20221020.feed_actions
         group by week_day, hour, minute, date
         having toDayOfWeek(time) = toDayOfWeek(now())
         and toHour(time) * 60 + toMinute(time) >= toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now()))
         and toHour(time) * 60 + toMinute(time) < toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now())) + 15
         order by week_day, hour, minute, date
                '''
    likes = ch_get_df(query, connection)
    return likes

def execute_views():
    query = '''
        select 
             toDayOfWeek(time) as week_day
            , toHour(time) as hour
            , toMinute(toStartOfFifteenMinutes(time)) as minute
            , toStartOfFifteenMinutes(time) as date
            , countIf(action = 'view') as metric
         from simulator_20221020.feed_actions
         group by week_day, hour, minute, date
         having toDayOfWeek(time) = toDayOfWeek(now())
         and toHour(time) * 60 + toMinute(time) >= toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now()))
         and toHour(time) * 60 + toMinute(time) < toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now())) + 15
         order by week_day, hour, minute, date
                '''
    views = ch_get_df(query, connection)
    return views


def execute_ctr():
    query = '''
        select 
             toDayOfWeek(time) as week_day
            , toHour(time) as hour
            , toMinute(toStartOfFifteenMinutes(time)) as minute
            , toStartOfFifteenMinutes(time) as date
            , countIf(action = 'like') / countIf(action = 'view') as metric
         from simulator_20221020.feed_actions
         group by week_day, hour, minute, date
         having toDayOfWeek(time) = toDayOfWeek(now())
         and toHour(time) * 60 + toMinute(time) >= toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now()))
         and toHour(time) * 60 + toMinute(time) < toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now())) + 15
         order by week_day, hour, minute, date
                '''
    ctr = ch_get_df(query, connection)
    return ctr

def execute_messages():
    query = '''
        select 
             toDayOfWeek(time) as week_day
            , toHour(time) as hour
            , toMinute(toStartOfFifteenMinutes(time)) as minute
            , toStartOfFifteenMinutes(time) as date
            , count(user_id) as metric
         from simulator_20221020.message_actions
         group by week_day, hour, minute, date
         having toDayOfWeek(time) = toDayOfWeek(now())
         and toHour(time) * 60 + toMinute(time) >= toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now()))
         and toHour(time) * 60 + toMinute(time) < toHour(toStartOfFifteenMinutes(now())) * 60 + toMinute(toStartOfFifteenMinutes(now())) + 15
         order by week_day, hour, minute, date
            '''
    messages = ch_get_df(query, connection)
    return messages

def names_metric(dau, likes, views, ctr, messages):
    dau['name'] = 'dau'
    likes['name'] = 'likes count'
    views['name'] = 'views count'
    ctr['name'] = 'ctr'
    messages['name'] = 'messages count'
    return dau, likes, views, ctr, messages

def calc_outliers(df):    
    q1 = df.groupby(['week_day', 'hour', 'minute'])['metric'].quantile(0.25).reset_index()
    q3 = df.groupby(['week_day', 'hour', 'minute'])['metric'].quantile(0.75).reset_index()

    df_stat = pd.merge(q1, q3, how = 'left', on = ['week_day', 'hour', 'minute'])
    df_stat = df_stat.rename({'metric_x': 'q1', 'metric_y': 'q3'}, axis=1) 

    df_stat['iqr'] = df_stat['q3'] - df_stat['q1']

    df_stat['inner_fence'] = df_stat['iqr'] * 1.5
    df_stat['outer_fence'] = df_stat['iqr'] * 3

    df_stat['inner_fence_le'] = df_stat['q1'] - df_stat['inner_fence']
    df_stat['inner_fence_ue'] = df_stat['q3'] + df_stat['inner_fence']

    df_stat['outer_fence_le'] = df_stat['q1'] - df_stat['outer_fence']
    df_stat['outer_fence_ue'] = df_stat['q3'] + df_stat['outer_fence']
    df['median'] = df['metric'].rolling(7).median()

    df_check = pd.merge(df, df_stat, how = 'left', on = ['week_day', 'hour', 'minute'])
    df_check['med_diff'] = df_check['metric'] / df_check['median'] - 1
    return df_check

def catch_outliers(df):
    
    outliers_prob = []
    outliers_poss = []  
    
    df_outliers_prob = pd.DataFrame()
    df_outliers_poss = pd.DataFrame()
   
    i = df['metric'].iloc[-1]

    if (i <= df['outer_fence_le'].iloc[-1] or i >= df['outer_fence_ue'].iloc[-1]) \
    and (df['med_diff'].iloc[-1] <= -0.5 or df['med_diff'].iloc[-1] >= 0.5):
        outliers_prob.append(df.iloc[-1])
        df_outliers_prob = pd.DataFrame(outliers_prob)
        
    elif (i <= df['inner_fence_le'].iloc[-1] or i >= df['inner_fence_ue'].iloc[-1])\
    and (df['med_diff'].iloc[-1] <= -0.5 or df['med_diff'].iloc[-1] >= 0.5):
        outliers_poss.append(df.iloc[-1])
        df_outliers_poss = pd.DataFrame(outliers_poss)
        
    return df_outliers_prob, df_outliers_poss

def alert(df_outliers_prob, df_outliers_poss):
    if df_outliers_prob.empty and df_outliers_poss.empty:
        pass
    elif df_outliers_prob.empty == False:
        msg_prob = '''Метрика {metric}:\nТекущее значение = {current_value:.2f}, вероятно является отклонением.
        \nМедиана периода =  {median:.2f}, отклонение на {diff:.2%}
        \nСсылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/2044/'''\
        .format(metric = df_outliers_prob['name'].iloc[-1], current_value = df_outliers_prob['metric'].iloc[-1], median = df_outliers_prob['median'].iloc[-1], diff = df_outliers_prob['med_diff'].iloc[-1])
        bot.sendMessage(chat_id = config.chat_id, text = msg_prob)
    elif df_outliers_poss.empty == False: 
        msg_poss = '''Метрика {metric}:\nТекущее значение = {current_value:.2f}, вероятно является отклонением.
        \nМедиана периода =  {median:.2f}, отклонение на {diff:.2%}
        \nСсылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/2044/'''\
        .format(metric = df_outliers_poss['name'].iloc[-1], current_value = df_outliers_poss['metric'].iloc[-1], median = df_outliers_poss['median'].iloc[-1], diff = df_outliers_poss['med_diff'].iloc[-1])
        bot.sendMessage(chat_id = config.chat_id, text = msg_poss)

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup=False)
def dag_alert_apetrikova():
    
    @task
    def report():
        dau = execute_dau()
        likes = execute_likes()
        views = execute_views()
        ctr = execute_ctr()
        messages = execute_messages()

        dau, likes, views, ctr, messages = names_metric(dau, likes, views, ctr, messages)

        df_check_dau = calc_outliers(dau)
        df_check_likes = calc_outliers(likes)
        df_check_views = calc_outliers(views)
        df_check_ctr = calc_outliers(ctr)
        df_check_messages = calc_outliers(messages)

        dau_prob, dau_poss = catch_outliers(df_check_dau)
        likes_prob, likes_poss = catch_outliers(df_check_likes)
        views_prob, views_poss = catch_outliers(df_check_views)
        ctr_prob, ctr_poss = catch_outliers(df_check_ctr)
        messages_prob, messages_poss = catch_outliers(df_check_messages)

        dau_alert = alert(dau_prob, dau_poss)
        likes_alert = alert(likes_prob, likes_poss)
        views_alert = alert(views_prob, views_poss)
        ctr_alert = alert(ctr_prob, ctr_poss)
        messages_alert = alert(messages_prob, messages_poss)
    
    report()

dag_alert_apetrikova = dag_alert_apetrikova()