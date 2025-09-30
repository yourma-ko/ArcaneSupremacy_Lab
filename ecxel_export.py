import os
import pandas as pd
import numpy as np
from pathlib import Path
import psycopg2
from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import ColorScaleRule
from datetime import datetime

# Настройки подключения к БД
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "231367")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "ou_analytics")

# Создание директории для результатов
exports_dir = Path("exports")
exports_dir.mkdir(exist_ok=True, parents=True)

# SQL запросы для экспорта в Excel
EXCEL_EXPORT_QUERIES = {
    # Детальная статистика по курсам
    "excel_export_course_statistics": """
        WITH si AS (
        SELECT 
            code_module, code_presentation,
            COUNT(DISTINCT id_student)                          AS total_students,
            SUM((final_result = 'Pass')::int)                   AS passed_students,
            SUM((final_result = 'Distinction')::int)            AS distinction_students,
            SUM((final_result = 'Fail')::int)                   AS failed_students,
            SUM((final_result = 'Withdrawn')::int)              AS withdrawn_students
        FROM studentinfo
        GROUP BY code_module, code_presentation
        ),
        a AS (
        SELECT 
            code_module, code_presentation,
            COUNT(DISTINCT id_assessment) AS assessments_count
        FROM assessments
        GROUP BY code_module, code_presentation
        ),
        v AS (
        SELECT 
            code_module, code_presentation,
            COUNT(DISTINCT id_site) AS vle_materials_count
        FROM vle
        GROUP BY code_module, code_presentation
        ),
        sv AS (
        SELECT 
            code_module, code_presentation,
            SUM(sum_click)                  AS total_clicks,
            ROUND(AVG(sum_click)::numeric,2) AS avg_clicks_per_activity
        FROM studentvle
        GROUP BY code_module, code_presentation
        )
        SELECT 
        c.code_module, c.code_presentation, c.module_presentation_length,
        COALESCE(si.total_students, 0)        AS total_students,
        COALESCE(si.passed_students, 0)       AS passed_students,
        COALESCE(si.distinction_students, 0)  AS distinction_students,
        COALESCE(si.failed_students, 0)       AS failed_students,
        COALESCE(si.withdrawn_students, 0)    AS withdrawn_students,
        ROUND(
            100.0 * COALESCE(si.withdrawn_students, 0) 
            / NULLIF(COALESCE(si.total_students, 0), 0)
        , 2)                                   AS dropout_pct,
        COALESCE(a.assessments_count, 0)      AS assessments_count,
        COALESCE(v.vle_materials_count, 0)    AS vle_materials_count,
        COALESCE(sv.total_clicks, 0)          AS total_clicks,
        COALESCE(sv.avg_clicks_per_activity,0)AS avg_clicks_per_activity
        FROM courses c
        LEFT JOIN si USING (code_module, code_presentation)
        LEFT JOIN a  USING (code_module, code_presentation)
        LEFT JOIN v  USING (code_module, code_presentation)
        LEFT JOIN sv USING (code_module, code_presentation)
        ORDER BY c.code_module, c.code_presentation;

    """,
    
    # Демографическая статистика
    "excel_export_demographics": """
        SELECT 
            gender, age_band, region, highest_education, imd_band, disability,
            COUNT(*) AS students,
            ROUND(100.0 * SUM((final_result = 'Distinction')::int) / COUNT(*), 2) AS distinction_pct,
            ROUND(100.0 * SUM((final_result = 'Pass')::int) / COUNT(*), 2) AS pass_pct,
            ROUND(100.0 * SUM((final_result = 'Fail')::int) / COUNT(*), 2) AS fail_pct,
            ROUND(100.0 * SUM((final_result = 'Withdrawn')::int) / COUNT(*), 2) AS withdrawn_pct,
            ROUND(AVG(num_of_prev_attempts)::numeric, 2) AS avg_prev_attempts,
            ROUND(AVG(studied_credits)::numeric, 2) AS avg_studied_credits
        FROM 
            studentinfo
        GROUP BY 
            gender, age_band, region, highest_education, imd_band, disability
        HAVING 
            COUNT(*) >= 10
        ORDER BY 
            students DESC;
    """,
    
    # Статистика по заданиям
    "excel_export_assessment_statistics": """
        SELECT 
            a.code_module, 
            a.code_presentation, 
            a.assessment_type, 
            a.date, 
            a.weight,
            COUNT(DISTINCT sa.id_student) AS submitters,
            ROUND(MIN(sa.score)::numeric, 2) AS min_score,
            ROUND(AVG(sa.score)::numeric, 2) AS avg_score,
            ROUND(MAX(sa.score)::numeric, 2) AS max_score,
            ROUND(STDDEV(sa.score)::numeric, 2) AS stddev_score,
            COUNT(DISTINCT CASE WHEN sa.score >= 40 THEN sa.id_student END) AS passed_students,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN sa.score >= 40 THEN sa.id_student END) / 
                  COUNT(DISTINCT sa.id_student), 2) AS pass_rate
        FROM 
            assessments a
        LEFT JOIN 
            studentassessment sa ON a.id_assessment = sa.id_assessment
        GROUP BY 
            a.code_module, a.code_presentation, a.assessment_type, a.date, a.weight
        ORDER BY 
            a.code_module, a.code_presentation, a.date;
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

def export_to_excel(dataframes_dict, filename):
    """Экспортирует данные в Excel с форматированием"""
    print(f"\nЭкспорт данных в Excel-файл: {filename}...")
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        total_rows = 0
        
        for sheet_name, df in dataframes_dict.items():
            if df is None or df.empty:
                continue
            
            # Записываем данные на лист
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            total_rows += len(df)
            
            # Получаем рабочий лист для форматирования
            ws = writer.sheets[sheet_name]
            
            # Замораживаем первую строку (заголовки)
            ws.freeze_panes = "A2"
            
            # Добавляем фильтры ко всем столбцам
            ws.auto_filter.ref = ws.dimensions
            
            # Применяем условное форматирование к числовым столбцам
            for i, col in enumerate(df.columns):
                if df[col].dtype in [np.float64, np.int64, np.int32, np.float32]:
                    col_letter = chr(65 + i)  # A, B, C, ...
                    cell_range = f"{col_letter}2:{col_letter}{len(df) + 1}"
                    
                    # Применяем цветовую шкалу (красный -> желтый -> зеленый)
                    rule = ColorScaleRule(
                        start_type="min", start_color="FFFF0000",
                        mid_type="percentile", mid_value=50, mid_color="FFFFFF00",
                        end_type="max", end_color="FF00FF00"
                    )
                    ws.conditional_formatting.add(cell_range, rule)
            
            # Автоподбор ширины столбцов
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                # Находим максимальную длину в столбце
                for cell in column:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                
                # Устанавливаем ширину столбца с небольшим запасом
                adjusted_width = (max_length + 2)
                ws.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Создан файл {filename}, {len(dataframes_dict)} листов, {total_rows} строк")

def run_excel_export():
    """Запускает экспорт данных в Excel-файл"""

    # Получаем данные для всех листов Excel
    course_stats_df = execute_query(EXCEL_EXPORT_QUERIES["excel_export_course_statistics"])
    demographics_df = execute_query(EXCEL_EXPORT_QUERIES["excel_export_demographics"])
    assessment_stats_df = execute_query(EXCEL_EXPORT_QUERIES["excel_export_assessment_statistics"])
    

    # Формируем словарь с данными для каждого листа
    dataframes_dict = {
        'Статистика курсов': course_stats_df,
        'Демографическая статистика': demographics_df,
        'Статистика по заданиям': assessment_stats_df
    }
    
    # Путь для сохранения Excel-файла
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    excel_path = exports_dir / f"{timestamp}_OULAD_analytics.xlsx"
    
    # Экспортируем данные в Excel с форматированием
    export_to_excel(dataframes_dict, excel_path)
    return excel_path

def main():

    
    excel_path = run_excel_export()
    

if __name__ == "__main__":
    main()