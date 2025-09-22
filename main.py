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
    "head_courses":              "SELECT * FROM courses LIMIT 10;",
    "head_assessments":          "SELECT * FROM assessments LIMIT 10;",
    "head_studentinfo":          "SELECT * FROM studentinfo LIMIT 10;",
    "head_studentregistration":  "SELECT * FROM studentregistration LIMIT 10;",
    "head_vle":                  "SELECT * FROM vle LIMIT 10;",
    "head_studentassessment":    "SELECT * FROM studentassessment LIMIT 10;",
    "head_studentvle":           "SELECT * FROM studentvle LIMIT 10;",

    # --- WHERE + ORDER BY ---
    "eee_2014j_attempts": """
        SELECT id_student, num_of_prev_attempts, studied_credits
        FROM studentinfo
        WHERE code_module='EEE' AND code_presentation='2014J'
        ORDER BY num_of_prev_attempts DESC, studied_credits DESC
        LIMIT 20;
    """,

    # --- GROUP BY + COUNT/MIN/MAX/AVG ---
    "outcomes_by_course": """
        SELECT code_module, code_presentation, final_result,
               COUNT(*) AS n, MIN(studied_credits) AS min_cred,
               MAX(studied_credits) AS max_cred,
               ROUND(AVG(studied_credits)::numeric,2) AS avg_cred
        FROM studentinfo
        GROUP BY code_module, code_presentation, final_result
        ORDER BY code_module, code_presentation, n DESC;
    """,

    # --- Dropout rate (Withdrawn) ---
    "dropout_rate": """
        SELECT code_module, code_presentation,
               ROUND(100.0*SUM((final_result='Withdrawn')::int)/COUNT(*),2) AS dropout_pct,
               COUNT(*) AS students
        FROM studentinfo
        GROUP BY code_module, code_presentation
        ORDER BY dropout_pct DESC;
    """,

    # --- Сколько заданий сдал студент vs исход ---
    "submissions_vs_result": """
        WITH submits AS (
          SELECT id_student, COUNT(*) AS cnt
          FROM studentassessment
          GROUP BY id_student
        )
        SELECT si.final_result,
               ROUND(AVG(COALESCE(s.cnt,0))::numeric,2) AS avg_submits,
               COUNT(*) AS students
        FROM studentinfo si
        LEFT JOIN submits s ON s.id_student = si.id_student
        GROUP BY si.final_result
        ORDER BY avg_submits DESC;
    """,

    # --- Топ студентов по суммарным кликам ---
    "top_students_by_clicks": """
        SELECT sv.code_module, sv.code_presentation, sv.id_student,
               SUM(sv.sum_click) AS total_clicks
        FROM studentvle sv
        GROUP BY sv.code_module, sv.code_presentation, sv.id_student
        ORDER BY total_clicks DESC
        LIMIT 20;
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
        # курсор
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