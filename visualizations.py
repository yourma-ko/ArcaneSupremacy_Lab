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
    
    # 4. Линейный график: средний балл студентов по неделям курса
    "line_chart_avg_score_by_week": """
        WITH weekly_scores AS (
            SELECT 
                a.code_module,
                a.code_presentation,
                FLOOR(a.date / 7) as week_number,
                AVG(sa.score) as avg_score,
                COUNT(DISTINCT sa.id_student) as student_count
            FROM assessments a
            JOIN studentassessment sa ON a.id_assessment = sa.id_assessment
            WHERE a.date IS NOT NULL
            GROUP BY a.code_module, a.code_presentation, FLOOR(a.date / 7)
        )
        SELECT 
            week_number,
            code_module,
            ROUND(AVG(avg_score)::numeric, 2) as avg_score
        FROM weekly_scores
        WHERE week_number BETWEEN 0 AND 30
        GROUP BY week_number, code_module
        ORDER BY week_number, code_module;
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
    print("\nСоздание круговой диаграммы: распределение активности по типам материалов...")
    
    df = execute_query(VISUALIZATION_QUERIES["pie_chart_activity_by_material_type"])
    if df is None or df.empty:
        print("Нет данных для построения круговой диаграммы")
        return
    
    print(f"Получено {len(df)} строк данных")
    
    # Вычисляем процентное соотношение
    df['percentage'] = (df['total_clicks'] / df['total_clicks'].sum()) * 100
    
    # Объединяем категории с процентом меньше 1% в "Others"
    threshold = 2.5
    df_main = df[df['percentage'] >= threshold].copy()
    df_others = df[df['percentage'] < threshold].copy()
    
    if not df_others.empty:
        others_row = pd.DataFrame({
            'activity_type': ['Others'],
            'total_clicks': [df_others['total_clicks'].sum()],
            'percentage': [df_others['percentage'].sum()]
        })
        df = pd.concat([df_main, others_row], ignore_index=True)
    else:
        df = df_main
    
    print(f"Категорий для отображения: {len(df)} (объединено в 'Others': {len(df_others)})")
    
    # Создаем простую круговую диаграмму
    plt.figure(figsize=(10, 8))
    
    colors = plt.cm.Pastel1(np.arange(len(df)) / len(df))
    
    plt.pie(
        df['total_clicks'], 
        labels=df['activity_type'], 
        autopct='%1.1f%%', 
        startangle=90,
        colors=colors
    )
    
    plt.title('Распределение активности студентов по типам материалов', fontsize=14, pad=20)
    plt.axis('equal')
    
    # Сохраняем график
    chart_path = charts_dir / "pie_chart_activity_by_material_type.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Круговая диаграмма сохранена: {chart_path}")

def create_bar_chart():
    """Создает столбчатую диаграмму: средний балл по типам заданий и модулям"""
    print("\nСоздание столбчатой диаграммы: средний балл по типам заданий и модулям...")
    
    df = execute_query(VISUALIZATION_QUERIES["bar_chart_avg_score_by_module_and_type"])
    if df is None or df.empty:
        print("Нет данных для построения столбчатой диаграммы")
        return
    
    print(f"Получено {len(df)} строк данных")
    
    # Создаем сгруппированную столбчатую диаграмму
    plt.figure(figsize=(14, 10))
    
    # Используем seaborn для более красивого отображения
    ax = sns.barplot(x='code_module', y='avg_score', hue='assessment_type', data=df, palette='viridis')
    
    # Добавляем подписи значений на столбцы
    for p in ax.patches:
        height = p.get_height()
        if not np.isnan(height):
            ax.annotate(f"{height:.1f}", 
                       (p.get_x() + p.get_width() / 2., height), 
                       ha='center', va='bottom',
                       fontsize=9)
    
    plt.title('Средний балл по типам заданий и модулям', fontsize=16)
    plt.xlabel('Модуль курса', fontsize=12)
    plt.ylabel('Средний балл', fontsize=12)
    plt.legend(title='Тип задания', fontsize=10)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "bar_chart_avg_score_by_module_and_type.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    
    print(f"✓ Столбчатая диаграмма сохранена: {chart_path}")

def create_horizontal_bar_chart():
    """Создает горизонтальную столбчатую диаграмму: процент отчислений по образовательному бэкграунду"""
    print("\nСоздание горизонтальной столбчатой диаграммы: процент отчислений по образовательному бэкграунду...")
    
    df = execute_query(VISUALIZATION_QUERIES["hbar_chart_dropout_by_education"])
    if df is None or df.empty:
        print("Нет данных для построения горизонтальной столбчатой диаграммы")
        return
    
    print(f"Получено {len(df)} строк данных")
    
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
                va='center', fontsize=10)
    
    plt.title('Процент отчислений по образовательному бэкграунду', fontsize=16)
    plt.xlabel('Процент отчислений, %', fontsize=12)
    plt.ylabel('Уровень образования', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "hbar_chart_dropout_by_education.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    
    print(f"✓ Горизонтальная столбчатая диаграмма сохранена: {chart_path}")

def create_line_chart():
    """Создает линейный график: средний балл студентов по неделям курса"""
    print("\nСоздание линейного графика: средний балл студентов по неделям курса...")
    
    df = execute_query(VISUALIZATION_QUERIES["line_chart_avg_score_by_week"])
    if df is None or df.empty:
        print("Нет данных для построения линейного графика")
        return
    
    print(f"Получено {len(df)} строк данных")
    
    plt.figure(figsize=(14, 8))
    
    # Создаем линейный график для каждого модуля
    for module, group in df.groupby('code_module'):
        plt.plot(group['week_number'], group['avg_score'], 
                marker='o', linestyle='-', linewidth=2, 
                label=module, markersize=5)
    
    plt.title('Средний балл студентов по неделям курса', fontsize=16)
    plt.xlabel('Номер недели', fontsize=12)
    plt.ylabel('Средний балл', fontsize=12)
    plt.legend(title='Модуль курса', fontsize=10)
    plt.grid(True, alpha=0.3)
    
    # Сохраняем график
    chart_path = charts_dir / "line_chart_avg_score_by_week.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    
    print(f"✓ Линейный график сохранен: {chart_path}")

def create_histogram():
    """Создает гистограмму: распределение баллов за экзаменационные задания"""
    print("\nСоздание гистограммы: распределение баллов за экзаменационные задания...")
    
    df = execute_query(VISUALIZATION_QUERIES["histogram_exam_scores"])
    if df is None or df.empty:
        print("Нет данных для построения гистограммы")
        return
    
    print(f"Получено {len(df)} строк данных")
    
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
    plt.axvline(mean_score, color='red', linestyle='dashed', linewidth=2, 
                label=f'Средний балл: {mean_score:.2f}')
    
    # Добавляем вертикальную линию для проходного балла (обычно 40)
    plt.axvline(40, color='black', linestyle='dashed', linewidth=2, 
                label=f'Проходной балл: 40')
    
    plt.title('Распределение баллов за экзаменационные задания', fontsize=16)
    plt.xlabel('Балл', fontsize=12)
    plt.ylabel('Количество студентов', fontsize=12)
    plt.legend(fontsize=11)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Сохраняем график
    chart_path = charts_dir / "histogram_exam_scores.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    
    print(f"✓ Гистограмма сохранена: {chart_path}")

def create_scatter_plot():
    """Создает диаграмму рассеяния: взаимосвязь между кликами и средним баллом"""
    print("\nСоздание диаграммы рассеяния: взаимосвязь между активностью на платформе и средним баллом...")
    
    df = execute_query(VISUALIZATION_QUERIES["scatter_plot_clicks_vs_score"])
    if df is None or df.empty:
        print("Нет данных для построения диаграммы рассеяния")
        return
    
    print(f"Получено {len(df)} строк данных")
    
    plt.figure(figsize=(12, 8))
    
    # Создаем диаграмму рассеяния с линией тренда
    plt.scatter(df['total_clicks'], df['avg_score'], alpha=0.5, s=30)
    
    # Добавляем линию регрессии
    z = np.polyfit(df['total_clicks'], df['avg_score'], 1)
    p = np.poly1d(z)
    plt.plot(df['total_clicks'], p(df['total_clicks']), "r--", linewidth=2,
             label=f"Тренд: y={z[0]:.6f}x+{z[1]:.2f}")
    
    # Рассчитываем и отображаем коэффициент корреляции Пирсона
    corr = df['total_clicks'].corr(df['avg_score'])
    plt.annotate(f"Корреляция Пирсона: {corr:.3f}", xy=(0.05, 0.95), xycoords='axes fraction',
                 fontsize=12, bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.8))
    
    plt.title('Взаимосвязь между активностью студента на платформе и средним баллом', fontsize=16)
    plt.xlabel('Общее количество кликов', fontsize=12)
    plt.ylabel('Средний балл', fontsize=12)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    
    # Сохраняем график
    chart_path = charts_dir / "scatter_plot_clicks_vs_score.png"
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
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