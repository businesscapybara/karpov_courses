import datetime as dt
from datetime import datetime, timedelta, date

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
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
'start_date': datetime(2022, 11, 16, 11, 0),
}


schedule_interval = '0 11 * * *'

def extract_mau():
    query = '''
        select
        toStartOfMonth(toDateTime(time)) as date
        , count(DISTINCT user_id) as mau
        from simulator_20221020.feed_actions
        group by date
        order by date
            '''
    mau = ch_get_df(query, connection)
    return mau

def extract_wau():
    query = '''
        select 
        toMonday(toDateTime(time)) as date
        ,count(DISTINCT user_id) as wau
        from simulator_20221020.feed_actions
        where toDate(time) >= toMonday(yesterday() - toIntervalWeek(1))
        group by date
        order by date
            '''
    wau = ch_get_df(query, connection)
    return wau

def extract_dau():
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
        toISOWeek(time) as iso_week
        , toDayOfWeek(time) as day_of_week
        , toDate(time) as date
        , count(distinct user_id) as dau
    from feed_msg
    group by iso_week, day_of_week, date
    having toDate(time) >= toMonday(yesterday() - toIntervalWeek(1))
    order by iso_week, day_of_week, date
            '''
    dau = ch_get_df(query, connection)
    return dau

def extract_new_users():
    query =  '''
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
    from simulator_20221020.message_actions),

    users as

    (select 
        user_id
        , min(toDate(time)) as reg_day
        , source
    from feed_msg
    group by user_id, source)

    select 
        toDayOfWeek(reg_day) as day_of_week
        , toISOWeek(reg_day) as iso_week
        , reg_day
        , count(distinct user_id) as cnt
        , source
    from users
    group by reg_day, day_of_week, source, day_of_week
    having reg_day >= toMonday(yesterday() - toIntervalWeek(1))
    order by reg_day
    '''
    new_users = ch_get_df(query, connection)
    return new_users

def extract_app_share():
    query =  '''

    with 

    feed as
    (select 
    distinct user_id,
    toDate(time) as dt
    from simulator_20221020.feed_actions
    where toDate(time) >= toMonday(yesterday() - toIntervalWeek(1))),

    message as
    (select 
    distinct user_id,
    toDate(time) as dt
    from simulator_20221020.message_actions
    where toDate(time) >= toMonday(yesterday() - toIntervalWeek(1))),

    users as
    (select 
    distinct user_id,
    toDate(time) as dt
    from

    (select 
    distinct user_id,
    toDate(time) as time
    from simulator_20221020.message_actions
    where toDate(time) >= toMonday(yesterday() - toIntervalWeek(1))
    union all 
    select 
    distinct user_id,
    toDate(time) as time
    from simulator_20221020.feed_actions
    where toDate(time) >= toMonday(yesterday() - toIntervalWeek(1)))
    )

    select 
      u.dt as dt,
      count(distinct f.user_id) / count(distinct u.user_id) as feed_share,
      count(distinct m.user_id) / count(distinct u.user_id) as message_share
    from users as u
    left join feed as f on f.user_id = u.user_id
    and f.dt = u.dt
    left join message as m on m.user_id = u.user_id
    and m.dt = u.dt
    group by u.dt

    '''

    app_share = ch_get_df(query, connection)
    return app_share

def extract_sticky_factor():
    query = '''
    select toDate(dt) as date,
           sum(sticky_factor) AS sticky_factor
    from
      (with
        (select 
         count(DISTINCT user_id) as mau
         from simulator_20221020.feed_actions
         where time >= date_add(DAY, -30, now())
         and time < toDate(now()) ) 

       as t1 select toDate(time) as dt,
            count(DISTINCT user_id) as dau,
            round((dau / t1), 2) as sticky_factor
       from simulator_20221020.feed_actions
       where time >= date_add(DAY, -30, now())
         and time < toDate(now())
       group by toDate(time)) as table
    group by date
    order by date 
            '''
    sticky_factor = ch_get_df(query, connection)
    return sticky_factor

def metrics_plot(dau, new_users, app_share):
    with sns.color_palette("Set2"):  
        sns.set(rc={'figure.figsize':(20.7,11.27)})
        fig, axes = plt.subplots(2, 2, dpi = 300)

        sns.barplot(data = dau, ax=axes[0][0], x = 'day_of_week', y = 'dau', hue = 'iso_week').set(title='App DAU')

        sns.barplot(data = new_users[new_users['source'] == 'organic'], ax=axes[1][0], x = 'day_of_week', \
                y = 'cnt', hue = 'iso_week', ci = None).set(title='Organic: new users by day')

        sns.barplot(data = new_users[new_users['source'] == 'ads'], ax=axes[1][1], x = 'day_of_week', \
                y = 'cnt', hue = 'iso_week', ci = None).set(title='Ads: new users by day')

        sns.lineplot(data = app_share[-8:], x = 'dt', y = 'feed_share', ax=axes[0][1],  \
                     label = 'use feed', markers = True).set(title='Share of users who using feed and message')
        sns.lineplot(data = app_share[-8:], x = 'dt', y = 'message_share', ax=axes[0][1],  \
                     label = 'use message', markers = True).set(title='Share of users who using feed and message')

        fig.tight_layout()
        plt_object = io.BytesIO()
        plt.savefig(plt_object)
        plt_object.seek(0)
        plt.object_name = 'metrics.png'
        plt.close()
    return plt_object

def report_text(mau, wau, dau, new_users, sticky_factor, app_share):
    report = \
    "Отчёт по приложению {date}:\
    \nMAU в этом месяце {mau_today}, в прошлом {mau_yest}, отличается на {diff_mau:.0%}\
    \nWAU на этой неделе {wau_today}, на прошлой {wau_yest}, отличается на {diff_wau:.0%}\
    \nDAU сегодня {dau_today}, вчера {dau_yest}, отличается на {diff_dau:.0%}\
    \nНовые пользователи (реклама): сегодня {new_ads_today}, вчера {new_ads_yest}, отличается на {diff_ads:.0%}\
    \nНовые пользователи (органика): сегодня {new_org_today}, вчера {new_org_yest}, отличается на {diff_org:.0%}\
    \nSticky factor {sf_today:.0%}\
    \nДоля использования ленты {share_feed:.0%}\
    \nДоля использования сообщений {share_msg:.0%}"\
    .format(
    date = dau['date'].dt.date.iloc[-1],
        
    mau_today = mau['mau'].iloc[-1],
    mau_yest =  mau['mau'].iloc[-2],
    diff_mau = (mau['mau'].iloc[-1] / mau['mau'].iloc[-2]) - 1,
        
    wau_today = wau['wau'].iloc[-1],
    wau_yest =  wau['wau'].iloc[-2],
    diff_wau = (wau['wau'].iloc[-1] / wau['wau'].iloc[-2]) - 1,
        
    dau_today = dau['dau'].iloc[-1],
    dau_yest =  dau['dau'].iloc[-2],
    diff_dau = (dau['dau'].iloc[-1] / dau['dau'].iloc[-2]) - 1,

    new_ads_today = new_users[new_users['source'] == 'ads']['cnt'].iloc[-1],
    new_ads_yest = new_users[new_users['source'] == 'ads']['cnt'].iloc[-2],
    diff_ads = (new_users[new_users['source'] == 'ads']['cnt'].iloc[-1] / new_users[new_users['source'] == 'ads']['cnt'].iloc[-2]) - 1,


    new_org_today = new_users[new_users['source'] == 'organic']['cnt'].iloc[-1],
    new_org_yest = new_users[new_users['source'] == 'organic']['cnt'].iloc[-2],
    diff_org = (new_users[new_users['source'] == 'organic']['cnt'].iloc[-1] / new_users[new_users['source'] == 'organic']['cnt'].iloc[-2]) - 1,

    sf_today = sticky_factor['sticky_factor'].iloc[-1],    
    share_feed = app_share['feed_share'].iat[-1],
    share_msg = app_share['message_share'].iat[-1]
    )
    return report

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup=False)
def dag_report_app_apetrikova():
    
    @task
    def report():
        mau = extract_mau()
        wau = extract_wau()
        dau = extract_dau()
        new_users = extract_new_users()
        app_share = extract_app_share()
        sticky_factor = extract_sticky_factor()
        
        message = report_text(mau, wau, dau, new_users, sticky_factor, app_share)
        bot.sendMessage(chat_id = config.chat_id, text = message)

        picture = metrics_plot(dau, new_users, app_share)
        bot.sendPhoto(chat_id = config.chat_id, photo = picture)
    
    report()

dag_report_app_apetrikova = dag_report_app_apetrikova()