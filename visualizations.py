import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import psycopg2
import psycopg2.extras
from datetime import datetime

# Настройки подключения к БД (те же, что в main.py)
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "231367")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "ou_analytics")

# Создание директорий для результатов
charts_dir = Path("charts")
charts_dir.mkdir(exist_ok=True, parents=True)

# Настройка стиля графиков
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12

# SQL запросы с JOIN для создания визуализаций
VISUALIZATION_QUERIES = {
    # 1. Круговая диаграмма: распределение активности студентов по типам материалов
    "pie_chart_activity_by_material_type": """
        SELECT v.activity_type,
               SUM(sv.sum_click) as total_clicks
        FROM studentvle sv
        JOIN vle v USING (id_site)
        GROUP BY v.activity_type
        ORDER BY total_clicks DESC;
    """,
    
    # 2. Столбчатая диаграмма: средний балл по типам заданий и модулям
    "bar_chart_avg_score_by_module_and_type": """
        SELECT a.code_module, a.assessment_type,
               ROUND(AVG(sa.score)::numeric, 2) as avg_score,
               COUNT(sa.id_student) as submissions
        FROM studentassessment sa
        JOIN assessments a USING (id_assessment)
        GROUP BY a.code_module, a.assessment_type
        ORDER BY a.code_module, avg_score DESC;
    """,
    
    # 3. Горизонтальная столбчатая диаграмма: процент отчислений по образовательному бэкграунду
    "hbar_chart_dropout_by_education": """
        SELECT highest_education,
               COUNT(*) as students,
               ROUND(100.0 * SUM((final_result = 'Withdrawn')::int) / COUNT(*), 2) as dropout_pct
        FROM studentinfo
        GROUP BY highest_education
        ORDER BY dropout_pct DESC;
    """,
    
    # 4. Линейный график: активность студентов по неделям курса с разбивкой по итоговому результату
    "line_chart_weekly_activity_by_outcome": """
        SELECT sv.date as week_num, si.final_result,
               COUNT(DISTINCT sv.id_student) as active_students
        FROM studentvle sv
        JOIN studentinfo si ON sv.id_student = si.id_student 
                           AND sv.code_module = si.code_module 
                           AND sv.code_presentation = si.code_presentation
        WHERE sv.code_module = 'CCC' AND sv.code_presentation = '2014B'
        GROUP BY sv.date, si.final_result
        ORDER BY sv.date;
    """,
    
    # 5. Гистограмма: распределение баллов за экзаменационные задания
    "histogram_exam_scores": """
        SELECT sa.score
        FROM studentassessment sa
        JOIN assessments a USING (id_assessment)
        WHERE a.assessment_type = 'Exam'
        ORDER BY sa.score;
    """,
    
    # 6. Диаграмма рассеяния: взаимосвязь между кликами на VLE и итоговым баллом
    "scatter_plot_clicks_vs_score": """
        WITH student_clicks AS (
            SELECT sv.id_student, SUM(sv.sum_click) AS total_clicks
            FROM studentvle sv
            GROUP BY sv.id_student
        ),
        student_scores AS (
            SELECT sa.id_student, AVG(sa.score) AS avg_score
            FROM studentassessment sa
            GROUP BY sa.id_student
        )
        SELECT sc.id_student, sc.total_clicks, ss.avg_score
        FROM student_clicks sc
        JOIN student_scores ss ON sc.id_student = ss.id_student
        ORDER BY sc.id_student;
    """
}

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

def create_pie_chart():
    """Создает круговую диаграмму распределения активности по типам материалов"""
    df = execute_query(VISUALIZATION_QUERIES["pie_chart_activity_by_material_type"])
    if df is None or df.empty:
        print("Нет данных для построения круговой диаграммы")
        return
    

    plt.figure(figsize=(10, 10))
    explode = [0.05] * len(df)  # Небольшой выступ для всех сегментов
    colors = plt.cm.tab20(np.arange(len(df)) / len(df))
    
    plt.pie(df['total_clicks'], labels=df['activity_type'], autopct='%1.1f%%', 
            startangle=90, explode=explode, colors=colors, shadow=True)
    
    plt.title('Распределение активности студентов по типам материалов', fontsize=16)
    plt.axis('equal')  # Круговая, а не овальная диаграмма
    
    # Сохраняем график
    chart_path = charts_dir / "pie_chart_activity_by_material_type.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    

def create_bar_chart():
    """Создает столбчатую диаграмму: средний балл по типам заданий и модулям"""

    df = execute_query(VISUALIZATION_QUERIES["bar_chart_avg_score_by_module_and_type"])
    if df is None or df.empty:
        print("Нет данных для построения столбчатой диаграммы")
        return
    

    
    # Создаем сгруппированную столбчатую диаграмму
    plt.figure(figsize=(14, 10))
    
    # Используем seaborn для более красивого отображения
    ax = sns.barplot(x='code_module', y='avg_score', hue='assessment_type', data=df, palette='viridis')
    
    # Добавляем подписи значений на столбцы
    for p in ax.patches:
        ax.annotate(f"{p.get_height():.1f}", 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'bottom',
                   fontsize=9)
    
    plt.title('Средний балл по типам заданий и модулям', fontsize=16)
    plt.xlabel('Модуль курса')
    plt.ylabel('Средний балл')
    plt.legend(title='Тип задания')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "bar_chart_avg_score_by_module_and_type.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    

def create_horizontal_bar_chart():
    """Создает горизонтальную столбчатую диаграмму: процент отчислений по образовательному бэкграунду"""

    df = execute_query(VISUALIZATION_QUERIES["hbar_chart_dropout_by_education"])
    if df is None or df.empty:
        print("Нет данных для построения горизонтальной столбчатой диаграммы")
        return

    # Сортируем по проценту отчислений
    df = df.sort_values('dropout_pct')
    
    plt.figure(figsize=(12, 8))
    
    # Создаем горизонтальную столбчатую диаграмму
    bars = plt.barh(df['highest_education'], df['dropout_pct'], 
                   color=plt.cm.YlOrRd(df['dropout_pct']/max(df['dropout_pct'])))
    
    # Добавляем подписи со значениями и количеством студентов
    for i, bar in enumerate(bars):
        plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                f"{df.iloc[i]['dropout_pct']}% ({df.iloc[i]['students']} студентов)", 
                va='center')
    
    plt.title('Процент отчислений по образовательному бэкграунду', fontsize=16)
    plt.xlabel('Процент отчислений, %')
    plt.ylabel('Уровень образования')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "hbar_chart_dropout_by_education.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    
    print(f"Горизонтальная столбчатая диаграмма сохранена: {chart_path}")

def create_line_chart():
    """Создает линейный график: активность студентов по неделям с разбивкой по итоговому результату"""

    df = execute_query(VISUALIZATION_QUERIES["line_chart_weekly_activity_by_outcome"])
    if df is None or df.empty:
        print("Нет данных для построения линейного графика")
        return
    

    
    plt.figure(figsize=(14, 8))
    
    # Создаем линейный график для каждой группы студентов по итоговому результату
    for result, group in df.groupby('final_result'):
        plt.plot(group['week_num'], group['active_students'], 
                marker='o', linestyle='-', linewidth=2, label=result)
    
    plt.title('Активность студентов по неделям курса CCC-2014B с разбивкой по итоговому результату', fontsize=16)
    plt.xlabel('Неделя курса')
    plt.ylabel('Количество активных студентов')
    plt.legend(title='Итоговый результат')
    plt.grid(True)
    
    # Добавляем аннотацию к графику
    plt.annotate('Начало курса', xy=(0, df[df['week_num'] == 0]['active_students'].sum()), 
                xytext=(5, df[df['week_num'] == 0]['active_students'].sum() + 20),
                arrowprops=dict(arrowstyle="->"))
    
    # Сохраняем график
    chart_path = charts_dir / "line_chart_weekly_activity_by_outcome.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    
    print(f"Линейный график сохранен: {chart_path}")

def create_histogram():
    """Создает гистограмму: распределение баллов за экзаменационные задания"""

    df = execute_query(VISUALIZATION_QUERIES["histogram_exam_scores"])
    if df is None or df.empty:
        print("Нет данных для построения гистограммы")
        return
    

    
    plt.figure(figsize=(12, 8))
    
    # Создаем гистограмму с 20 бинами
    n, bins, patches = plt.hist(df['score'], bins=20, edgecolor='black', alpha=0.7)
    
    # Раскрашиваем бины в зависимости от значения (красный - низкие баллы, зеленый - высокие)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    col = bin_centers - min(bin_centers)
    col /= max(col)
    
    for c, p in zip(col, patches):
        plt.setp(p, 'facecolor', plt.cm.RdYlGn(c))
    
    # Добавляем вертикальную линию для среднего значения
    mean_score = df['score'].mean()
    plt.axvline(mean_score, color='red', linestyle='dashed', linewidth=1, 
                label=f'Средний балл: {mean_score:.2f}')
    
    # Добавляем вертикальную линию для проходного балла (обычно 40)
    plt.axvline(40, color='black', linestyle='dashed', linewidth=1, 
                label=f'Проходной балл: 40')
    
    plt.title('Распределение баллов за экзаменационные задания', fontsize=16)
    plt.xlabel('Балл')
    plt.ylabel('Количество студентов')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "histogram_exam_scores.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    
    print(f"Гистограмма сохранена: {chart_path}")

def create_scatter_plot():
    """Создает диаграмму рассеяния: взаимосвязь между кликами и средним баллом"""
    
    df = execute_query(VISUALIZATION_QUERIES["scatter_plot_clicks_vs_score"])
    if df is None or df.empty:
        print("Нет данных для построения диаграммы рассеяния")
        return
    
    print(f"Получено {len(df)} строк данных")
    
    plt.figure(figsize=(12, 8))
    
    # Создаем диаграмму рассеяния с линией тренда
    plt.scatter(df['total_clicks'], df['avg_score'], alpha=0.5)
    
    # Добавляем линию регрессии
    z = np.polyfit(df['total_clicks'], df['avg_score'], 1)
    p = np.poly1d(z)
    plt.plot(df['total_clicks'], p(df['total_clicks']), "r--", 
             label=f"Тренд: y={z[0]:.6f}x+{z[1]:.2f}")
    
    # Рассчитываем и отображаем коэффициент корреляции Пирсона
    corr = df['total_clicks'].corr(df['avg_score'])
    plt.annotate(f"Корреляция: {corr:.2f}", xy=(0.05, 0.95), xycoords='axes fraction',
                 fontsize=12, bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    plt.title('Взаимосвязь между активностью студента на платформе и средним баллом', fontsize=16)
    plt.xlabel('Общее количество кликов')
    plt.ylabel('Средний балл')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Сохраняем график
    chart_path = charts_dir / "scatter_plot_clicks_vs_score.png"
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    

def main():
 
    # Создаем все типы визуализаций
    create_pie_chart()
    create_bar_chart()
    create_horizontal_bar_chart()
    create_line_chart()
    create_histogram()
    create_scatter_plot()
    


if __name__ == "__main__":
    main()