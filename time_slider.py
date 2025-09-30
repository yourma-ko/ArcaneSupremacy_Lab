import os
import pandas as pd
import numpy as np
import plotly.express as px
from pathlib import Path
import psycopg2

# Настройки подключения к БД
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "231367")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "ou_analytics")

# Создание директории для результатов
charts_dir = Path("charts")
charts_dir.mkdir(exist_ok=True, parents=True)

# SQL-запрос для временного слайдера
TIME_SLIDER_QUERY = """
    SELECT sv.code_module, sv.code_presentation, sv.date, 
           COUNT(DISTINCT sv.id_student) AS active_students,
           SUM(sv.sum_click) AS total_clicks
    FROM studentvle sv
    WHERE sv.date >= 0 AND sv.date <= 100
    GROUP BY sv.code_module, sv.code_presentation, sv.date
    ORDER BY sv.date;
"""

def execute_query(query):
    """Выполняет SQL запрос и возвращает результаты в виде DataFrame"""
    dsn = {
        "host": PG_HOST, "port": PG_PORT, "dbname": PG_DB,
        "user": PG_USER, "password": PG_PASS,
    }
    
    try:
        with psycopg2.connect(**dsn) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Ошибка при выполнении SQL-запроса: {e}")
        return None

def create_plotly_time_slider():
    """Создает интерактивный график с временным слайдером: активность по дням с разбивкой по модулям"""

    df = execute_query(TIME_SLIDER_QUERY)
    if df is None or df.empty:
        print("Нет данных для построения интерактивного графика")
        return

    
    # Создаем интерактивный график Plotly с временным слайдером
    fig = px.scatter(df, x='total_clicks', y='active_students', 
                    animation_frame='date',
                    animation_group='code_module',
                    size='total_clicks',
                    color='code_module',
                    hover_name='code_module',
                    text='code_presentation',
                    size_max=60,
                    range_x=[0, df['total_clicks'].max() * 1.1],
                    range_y=[0, df['active_students'].max() * 1.1])
    
    fig.update_layout(
        title='Динамика активности студентов по дням курса',
        xaxis_title='Общее количество кликов',
        yaxis_title='Количество активных студентов',
        legend_title='Код модуля',
        height=600
    )
    
    # Настройка анимации
    fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 300
    fig.layout.updatemenus[0].buttons[0].args[1]['transition']['duration'] = 300
    
    # Показываем график
    fig.show()
    
    # Для записи HTML-версии графика
    html_path = charts_dir / "time_slider_daily_activity.html"
    fig.write_html(html_path)


def main():
    """Основная функция для запуска интерактивной визуализации с временным слайдером"""

    
    create_plotly_time_slider()
    


if __name__ == "__main__":
    main()