import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'd-nadezhkin-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 3, 30),
    'schedule_interval': '0 12 * * *'
}

y = 1994 + hash(f'd-nadezhkin-18') % 23                 # год данных


@dag(default_args=default_args, catchup=False)          # наш даг
def d_nadezhkin_lesson_03():
    @task(retries=3)                                    # таск 1 читаем данные
    def get_data():                                   
        df = pd.read_csv('vgsales.csv')
        df_vgsales = df[df.Year == y]
        return df_vgsales

    @task()                                             # таск 2 продаваемая игра в мире
    def get_top_global(df_vgsales):
        top_global = df_vgsales[df_vgsales.Global_Sales == df_vgsales.Global_Sales.max()].Name
        top_global = ', '.join(top_global)
        return top_global

    @task()                                             # таск 3 продаваемый жанр в европе
    def get_top_eu_genre(df_vgsales):
        top_eu_genre = df_vgsales.groupby('Genre', as_index=False)\
                                 .agg({'EU_Sales':'sum'})\
                                 .sort_values('EU_Sales', ascending=False)\
                                 .head(1).Genre
        top_eu_genre = ', '.join(top_eu_genre)
        return top_eu_genre

    @task()                                             # таск 4 платформа с макс продажами в сев америке более 1млн тиражом
    def get_top_na_platform(df_vgsales):
        top_na_platform = df_vgsales.query('NA_Sales > 1')\
                                    .groupby('Platform', as_index=False)\
                                    .agg({'NA_Sales':'sum'})\
                                    .sort_values('NA_Sales', ascending=False)\
                                    .head(1).Platform
        top_na_platform = ', '.join(top_na_platform)
        return top_na_platform

    @task()                                             # таск 5 топ издателей японии по средним продажам
    def get_top_jp_publisher(df_vgsales):
        top_jp_publisher = df_vgsales.groupby('Publisher', as_index=False)\
                                     .agg({'JP_Sales':'mean'})\
                                     .sort_values('JP_Sales', ascending=False)\
                                     .head(1).Publisher
        top_jp_publisher = ', '.join(top_jp_publisher)
        return top_jp_publisher

    @task()                                             # таск 6 сколько игр продалось в европе лучше чем в японии
    def get_games_eu_more_jp(df_vgsales):
        games_eu_more_jp = df_vgsales.query('EU_Sales > JP_Sales').shape[0]
        games_eu_more_jp = ', '.join(games_eu_more_jp)
        return games_eu_more_jp

    @task()
    def print_data(top_global, top_eu_genre, top_na_platform, top_jp_publisher, games_eu_more_jp):

        context = get_current_context()
        date = context['ds']
        
        print(f'Sales game analysis on {date}')
        print(f'''Report for {y}
                   Top Global sales: {top_global}
                   Top Genre by EU region: {top_eu_genre}
                   Top Platform by NA region: {top_na_platform}
                   Top Publisher by JP region (avg sales): {top_jp_publisher}
                   How many games have sold better in Europe than Japan: {games_eu_more_jp} games''')


    df_vgsales = get_data()

    top_global = get_top_global(df_vgsales)
    top_eu_genre = get_top_eu_genre(df_vgsales)
    top_na_platform = get_top_na_platform(df_vgsales)
    top_jp_publisher = get_top_jp_publisher(df_vgsales)
    games_eu_more_jp = get_games_eu_more_jp(df_vgsales)

    print_data(top_global, top_eu_genre, top_na_platform, top_jp_publisher, games_eu_more_jp)

d_nadezhkin_18_lesson_03 = d_nadezhkin_18_lesson_03()