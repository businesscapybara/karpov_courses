import datetime as dt
from datetime import datetime, timedelta, date

import telegram
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
'start_date': datetime(2022, 11, 15, 11, 0),
}

schedule_interval = '0 11 * * *'

query = '''
select 
    toDate(time) as date
    , count(distinct user_id) as dau
    , countIf(action = 'like') as likes
    , countIf(action = 'view') as views
    , likes / views as ctr
 from simulator_20221020.feed_actions
 where date >= yesterday() - toIntervalWeek(1) and date <= today() - toIntervalDay(1) 
 group by date
 order by date
'''

def extract_merics():
    metrics = ch_get_df(query, connection)
    return metrics 


def previous_day_metrics(metrics):
    report = "Метрики за предыдущий день {0}:\nDAU {1:,}\nПросмотры {2:,}\nЛайки {3:,}\nCTR {4:.0%}"\
    .format(metrics['date'].dt.date.iloc[-1], metrics['dau'].iloc[-1], metrics['likes'].iloc[-1],\
            metrics['views'].iloc[-1], round(metrics['ctr'].iloc[-1], 3))
    text = report
    return text


def metrics_plot(metrics):
    sns.set(rc={'figure.figsize':(20.7,11.27)})
    fig, axes = plt.subplots(2, 2, dpi = 300)
    fig.suptitle('Метрики ленты новостей за неделю', fontsize=20)
    colors = sns.color_palette()
    sns.lineplot(data = metrics, ax=axes[0][0], x = 'date', y = 'dau', color = 'black', label = 'dau', marker='o')\
    .set(title='DAU')
    sns.lineplot(data = metrics, ax=axes[1][0], x = 'date', y = 'ctr', color = 'green', label = 'ctr', marker='o')\
    .set(title='CTR')
    sns.lineplot(data = metrics, ax=axes[1][1], x = 'date', y = 'likes', color = 'orange', label = 'likes',\
                 marker='o').set(title='Likes')
    sns.lineplot(data = metrics, ax=axes[0][1], x = 'date', y = 'views', color = 'navy', label = 'views',\
                 marker='o').set(title='Views')
    fig.tight_layout()

    plt_object = io.BytesIO()
    plt.savefig(plt_object)
    plt_object.seek(0)
    plt.object_name = 'metrics.png'
    plt.close()
    return plt_object


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup=False)
def dag_report_apetrikova():
    
    @task
    def report():
        metrics = extract_merics()
        
        message = previous_day_metrics(metrics)
        bot.sendMessage(chat_id = config.chat_id, text = message)

        picture = metrics_plot(metrics)
        bot.sendPhoto(chat_id = config.chat_id, photo = picture)
    
    report()

dag_report_apetrikova = dag_report_apetrikova()