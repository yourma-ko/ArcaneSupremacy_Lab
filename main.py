import os
import csv
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras

PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "231367")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "ou_analytics")

QUERIES = {
    # --- SELECT * LIMIT 10 ---
    "head_courses":              "select * from courses limit 10;",
    "head_assessments":          "select * from assessments limit 10;",
    "head_studentinfo":          "select * from studentinfo limit 10;",
    "head_studentregistration":  "select * from studentregistration limit 10;",
    "head_vle":                  "select * from vle limit 10;",
    "head_studentassessment":    "select * from studentassessment limit 10;",
    "head_studentvle":           "select * from studentvle limit 10;",

    # --- WHERE + ORDER BY ---
    "eee_2014j_attempts": """
        select id_student, num_of_prev_attempts, studied_credits
        from studentinfo
        where code_module = 'EEE' and code_presentation = '2014J'
        order by num_of_prev_attempts desc, studied_credits desc
        limit 20;
    """,

    # --- GROUP BY + COUNT/MIN/MAX/AVG ---
    "outcomes_by_course": """
        select code_module, code_presentation, final_result,
               count(*) as n,
               min(studied_credits) as min_cred,
               max(studied_credits) as max_cred,
               round(avg(studied_credits)::numeric, 2) as avg_cred
        from studentinfo
        group by code_module, code_presentation, final_result
        order by code_module, code_presentation, n desc;
    """,

    # --- Dropout rate (Withdrawn) ---
    "dropout_rate": """
        select code_module, code_presentation,
               round(100.0 * sum((final_result = 'Withdrawn')::int) / count(*), 2) as dropout_pct,
               count(*) as students
        from studentinfo
        group by code_module, code_presentation
        order by dropout_pct desc;
    """,

    # --- Сколько заданий сдал студент vs исход ---
    "submissions_vs_result": """
        with submits as (
          select id_student, count(*) as cnt
          from studentassessment
          group by id_student
        )
        select si.final_result,
               round(avg(coalesce(s.cnt, 0))::numeric, 2) as avg_submits,
               count(*) as students
        from studentinfo si
        left join submits s on s.id_student = si.id_student
        group by si.final_result
        order by avg_submits desc;
    """,

    # --- Топ студентов по суммарным кликам ---
    "top_students_by_clicks": """
        select sv.code_module, sv.code_presentation, sv.id_student,
               sum(sv.sum_click) as total_clicks
        from studentvle sv
        group by sv.code_module, sv.code_presentation, sv.id_student
        order by total_clicks desc
        limit 20;
    """,
    # -- балл учитывая вес по модулю и презентации
    "weighted_score_by_course": """
        with per_student as(
          select a.code_module, a.code_presentation, sa.id_student,
                 sum(sa.score * a.weight) / nullif(sum(a.weight), 0) as weighted_score
          from studentassessment sa
          join assessments a using (id_assessment)
          group by a.code_module, a.code_presentation, sa.id_student
        )
        select code_module, code_presentation,
               round(avg(weighted_score)::numeric, 2) as avg_weighted_score,
               count(*) as students
        from per_student
        group by code_module, code_presentation
        order by avg_weighted_score desc;
    """,

    # -- средний балл по типам оценивания
    "avg_score_by_assessment_type": """
        select a.assessment_type,
               round(avg(sa.score)::numeric, 2) as avg_score,
               count(*) as submissions
        from studentassessment sa
        join assessments a using (id_assessment)
        group by a.assessment_type
        order by avg_score desc;
    """,

    # -- среднее число кликов на студента по курсу
    "avg_clicks_per_student_by_course": """
        with per_student as (
          select code_module, code_presentation, id_student, sum(sum_click) as total_clicks
          from studentvle
          group by 1,2,3
        )
        select code_module, code_presentation,
               round(avg(total_clicks)::numeric, 1) as avg_clicks_per_student,
               count(*) as students
        from per_student
        group by 1,2
        order by avg_clicks_per_student desc;
    """,

    # -- активные студенты по неделям (retention после старта курса)
    "weekly_active_students": """
        select code_module, code_presentation,
               floor(date / 7.0) as week_num,
               count(distinct id_student) as active_students
        from studentvle
        where date >= 0
        group by 1,2,3
        order by code_module, code_presentation, week_num;
    """,

    # -- итог экзамена и число попыток (результаты в процентах)
    "dropout_by_prev_attempts": """
        select num_of_prev_attempts,
               count(*) as students,
               round(100.0 * sum((final_result = 'Withdrawn')::int) / count(*), 2) as dropout_pct,
               round(100.0 * sum((final_result = 'Pass')::int) / count(*), 2) as pass_pct
        from studentinfo
        group by num_of_prev_attempts
        order by num_of_prev_attempts;
    """,
}

def run_queries():
    dsn = {
        "host": PG_HOST, "port": PG_PORT, "dbname": PG_DB,
        "user": PG_USER, "password": PG_PASS,
    }
    out_dir = Path("outputs"); out_dir.mkdir(exist_ok=True, parents=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")

    with psycopg2.connect(**dsn) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for name, sql in QUERIES.items():
            print(f"\n{name}")
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc.name for desc in cur.description] if cur.description else []

            # печать первых 5 строк в консоль
            print("columns:", ", ".join(cols))
            for r in rows[:5]:
                print(tuple(r))

            # сохранение в CSV
            csv_path = out_dir / f"{stamp}_{name}.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if cols: w.writerow(cols)
                for r in rows:
                    w.writerow(list(r))
            print(f"saved: {csv_path}  (rows={len(rows)})")

if __name__ == "__main__":
    run_queries()
