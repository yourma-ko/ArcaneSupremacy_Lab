import os
import psycopg2
import random
import time
from datetime import datetime, timedelta

# Настройки подключения к БД
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "231367")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "ou_analytics")

# Интервал между вставками данных (в секундах)
INSERT_INTERVAL = 1  # 1 секунда между вставками

# Количество записей для добавления за одну итерацию
RECORDS_PER_BATCH_VLE = 5  # Добавляем по 5 записей активности за раз
RECORDS_PER_BATCH_ASSESSMENT = 200  # Добавляем по 200 записей оценок за раз

def get_db_connection():
    """Устанавливает соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def fetch_valid_foreign_keys():
    """Получает допустимые значения внешних ключей для генерации данных"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        
        # Получаем существующих студентов
        cursor.execute("""
            SELECT DISTINCT id_student 
            FROM studentinfo
            ORDER BY id_student
            LIMIT 1000
        """)
        valid_students = [row[0] for row in cursor.fetchall()]
        
        # Получаем существующие модули и презентации
        cursor.execute("""
            SELECT DISTINCT code_module, code_presentation
            FROM courses
        """)
        valid_modules = cursor.fetchall()
        
        # Получаем существующие ID сайтов
        cursor.execute("""
            SELECT DISTINCT id_site
            FROM vle
        """)
        valid_sites = [row[0] for row in cursor.fetchall()]
        
        # Получаем существующие ID заданий
        cursor.execute("""
            SELECT DISTINCT id_assessment
            FROM assessments
        """)
        valid_assessments = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        return {
            "students": valid_students,
            "modules": valid_modules,
            "sites": valid_sites,
            "assessments": valid_assessments
        }
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return None
    finally:
        conn.close()

def insert_studentvle_data(valid_keys, num_records=5):
    """Вставляет новые записи в таблицу studentvle"""
    if not valid_keys:
        print("Нет данных о допустимых ключах")
        return 0
    
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        records_inserted = 0
        
        for _ in range(num_records):
            student_id = random.choice(valid_keys["students"])
            module_info = random.choice(valid_keys["modules"])
            code_module, code_presentation = module_info
            site_id = random.choice(valid_keys["sites"])
            
            # Генерируем реалистичные данные активности
            date = random.randint(0, 100)  # Дни от начала курса
            sum_click = random.randint(1, 50)  # Количество кликов
            
            # Проверяем, существует ли уже запись для этой комбинации
            cursor.execute("""
                SELECT COUNT(*) 
                FROM studentvle 
                WHERE id_student = %s AND id_site = %s AND code_module = %s 
                AND code_presentation = %s AND date = %s
            """, (student_id, site_id, code_module, code_presentation, date))
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Если запись уже существует, обновляем количество кликов
                cursor.execute("""
                    UPDATE studentvle 
                    SET sum_click = sum_click + %s
                    WHERE id_student = %s AND id_site = %s AND code_module = %s 
                    AND code_presentation = %s AND date = %s
                """, (sum_click, student_id, site_id, code_module, code_presentation, date))
            else:
                # Если записи нет, вставляем новую
                cursor.execute("""
                    INSERT INTO studentvle 
                    (code_module, code_presentation, id_site, id_student, date, sum_click)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (code_module, code_presentation, site_id, student_id, date, sum_click))
            
            records_inserted += 1
        
        conn.commit()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Добавлено {records_inserted} записей в studentvle")
        return records_inserted
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при вставке данных в studentvle: {e}")
        return 0
    finally:
        conn.close()

def insert_studentassessment_data(valid_keys, num_records=30):
    """Вставляет новые записи в таблицу studentassessment"""
    if not valid_keys:
        print("Нет данных о допустимых ключах")
        return 0
    
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        records_inserted = 0
        records_updated = 0
        
        for _ in range(num_records):
            student_id = random.choice(valid_keys["students"])
            assessment_id = random.choice(valid_keys["assessments"])
            
            # Генерируем реалистичные данные оценки
            date_submitted = random.randint(10, 100)  # Дни от начала курса
            is_banked = random.choice([0, 0, 0, 1])  # Большая вероятность 0 (не банковская работа)
            score = round(random.uniform(30.0, 95.0), 1)  # Оценка от 30 до 95
            
            # Проверяем, существует ли уже запись для этой комбинации
            cursor.execute("""
                SELECT COUNT(*) 
                FROM studentassessment 
                WHERE id_student = %s AND id_assessment = %s
            """, (student_id, assessment_id))
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Если запись уже существует, обновляем оценку
                cursor.execute("""
                    UPDATE studentassessment 
                    SET score = %s, date_submitted = %s
                    WHERE id_student = %s AND id_assessment = %s
                """, (score, date_submitted, student_id, assessment_id))
                records_updated += 1
            else:
                # Если записи нет, вставляем новую
                cursor.execute("""
                    INSERT INTO studentassessment 
                    (id_student, id_assessment, date_submitted, is_banked, score)
                    VALUES (%s, %s, %s, %s, %s)
                """, (student_id, assessment_id, date_submitted, is_banked, score))
                records_inserted += 1
        
        conn.commit()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Добавлено {records_inserted} новых оценок, обновлено {records_updated} в studentassessment")
        return records_inserted + records_updated
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при вставке данных в studentassessment: {e}")
        return 0
    finally:
        conn.close()

def main():
    """Основная функция для периодического добавления данных"""
    print("=" * 80)
    print("Запуск автоматического добавления данных в базу")
    print(f"Интервал между обновлениями: {INSERT_INTERVAL} секунд")
    print(f"Записей VLE за итерацию: {RECORDS_PER_BATCH_VLE}")
    print(f"Записей оценок за итерацию: {RECORDS_PER_BATCH_ASSESSMENT}")
    print("=" * 80)
    
    # Получаем допустимые значения для внешних ключей
    print("Загрузка данных из базы...")
    valid_keys = fetch_valid_foreign_keys()
    
    if not valid_keys:
        print("Не удалось получить данные о допустимых ключах. Завершение работы.")
        return
    
    print(f"✓ Загружено: {len(valid_keys['students'])} студентов, "
          f"{len(valid_keys['modules'])} модулей, "
          f"{len(valid_keys['sites'])} сайтов, "
          f"{len(valid_keys['assessments'])} заданий")
    print("=" * 80)
    
    total_vle_records = 0
    total_assessment_records = 0
    iteration = 0
    
    try:
        while True:
            iteration += 1
            print(f"\n📊 Итерация #{iteration}")
            
            # Добавляем записи активности в VLE
            vle_records = insert_studentvle_data(valid_keys, RECORDS_PER_BATCH_VLE)
            total_vle_records += vle_records
            
            # Добавляем записи оценок
            assessment_records = insert_studentassessment_data(valid_keys, RECORDS_PER_BATCH_ASSESSMENT)
            total_assessment_records += assessment_records
            
            print(f"📈 Всего добавлено: {total_vle_records} записей активности, {total_assessment_records} оценок")
            print("-" * 80)
            
            # Ждем до следующей итерации
            time.sleep(INSERT_INTERVAL)
    except KeyboardInterrupt:
        print("\n" + "=" * 80)
        print("⏹️  Программа остановлена пользователем")
        print(f"📊 Итого добавлено: {total_vle_records} записей активности, {total_assessment_records} оценок")
        print(f"🔄 Выполнено итераций: {iteration}")
        print("=" * 80)
    except Exception as e:
        print(f"\n❌ Программа прервана из-за ошибки: {e}")

if __name__ == "__main__":
    main()