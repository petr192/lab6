from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv
import json
import redis
#from clickhouse_connect import get_client
from clickhouse_driver import Client

# Константы
POSTGRES_CONN = {
    "dbname": "sku_info",
    "user": "lab05",
    "password": "zua0ieMahk9Jei",
    "host": "data.ijklmn.xyz",
    "port": 5433
}
CLICKHOUSE_CONN = {
    "host": '212.233.73.6',
    "port": 8123,
    #"username": 'petr_yurlov',
    "user": 'petr_yurlov',
    "password": 'UxcVHmJIJSyOTjZq',
    "database": 'petr_yurlov'
}
REDIS_CONN = {"host": '89.208.211.89', "port": 6379, "db": 1}

#CSV_PATH = "/home/ubuntu/sku_cat.csv"
#JSONL_IN = "/home/ubuntu/_tech_events.jsonl"
#JSONL_OUT = "/home/ubuntu/cleaned.jsonl"

BASE_DIR = "/usr/local/airflow/files"
CSV_PATH = f"{BASE_DIR}/sku_cat.csv"
JSONL_IN = f"{BASE_DIR}/_tech_events.jsonl"
JSONL_OUT = f"{BASE_DIR}/cleaned.jsonl"

default_args = {"start_date": datetime(2025, 4, 20)}
dag = DAG("lab5_pipeline", schedule_interval=None, default_args=default_args, catchup=False)


def extract_postgres():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    query = """
    WITH RECURSIVE cat_tree AS (
        SELECT c.cat AS parent_cat, c.id AS parent_id, c.cat, c.id
        FROM public.category_tree c
        WHERE c.cat BETWEEN 10 AND 99
        UNION ALL
        SELECT ct.parent_cat, ct.parent_id, c.cat, c.id
        FROM public.category_tree c
        JOIN cat_tree ct ON c.parent_id = ct.id
    ),
    cte AS (
        SELECT parent_cat, parent_id, cat, id,
        ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id DESC) rn
        FROM cat_tree
    )
    SELECT cte.parent_cat, cte.cat, sku_id
    FROM cte
    JOIN public.sku_cat sku ON cte.cat = sku.cat
    WHERE rn = 1
    """
    cur.execute(query)
    with open(CSV_PATH, "w") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cur.description])
        writer.writerows(cur.fetchall())
    cur.close()
    conn.close()


def clean_jsonl():
    with open(JSONL_IN) as fin, open(JSONL_OUT, "w") as fout:
        for line in fin:
            obj = json.loads(line)
            cleaned = {
                "user_id": obj["userId"].replace("user:", ""),
                "item_id": obj["itemId"].replace("sku:", ""),
                "action": obj["action"],
                "timestamp": int(float(obj["timestamp"]))
            }
            fout.write(json.dumps(cleaned) + "\n")


def load_to_clickhouse():
    ch_client = Client(**CLICKHOUSE_CONN)

    # sku_cat
    with open(CSV_PATH, 'r') as f:
        header = f.readline().strip().split(',')
        rows = [line.strip().split(',') for line in f]
    #ch_client.insert(f'petr_yurlov.lab5_pg', rows, column_names=header)
    query = f"INSERT INTO petr_yurlov.lab5_pg ({', '.join(header)}) VALUES"
    ch_client.execute(query, rows)

    # cleaned.jsonl
    with open(JSONL_OUT, 'r') as f:
        rows = [json.loads(line.strip()) for line in f]
    #ch_client.insert('petr_yurlov.lab5', rows)
    columns = rows[0].keys()
    data = [tuple(row[col] for col in columns) for row in rows]

    query = f"INSERT INTO petr_yurlov.lab5 ({', '.join(columns)}) VALUES"
    ch_client.execute(query, data)


def aggregate_clickhouse():
    ch_client = get_client(**CLICKHOUSE_CONN)
    ch_client.execute("""
        INSERT INTO petr_yurlov.lab5_ans
        WITH cl AS (
            SELECT *, toDate(timestamp) as dt, formatDateTime(timestamp, '%H') || 'h' as hr
            FROM petr_yurlov.lab5
            WHERE action = 'favAdd'
        ),
        clpg AS (
            SELECT dt, hr, parent_cat, count(item_id) cnt, min(timestamp) mintmp
            FROM cl
            LEFT JOIN petr_yurlov.lab5_pg pg ON pg.sku_id = cl.item_id
            GROUP BY dt, hr, parent_cat
        ),
        fnl AS (
            SELECT dt, hr, parent_cat, cnt, mintmp,
            rank() OVER (PARTITION BY dt, hr ORDER BY cnt DESC, mintmp ASC) rnk
            FROM clpg
        )
        SELECT 'fav' as event, 'level2' as level, dt, hr,
        arrayStringConcat(groupArray(parent_cat), ',') as cats
        FROM fnl
        WHERE rnk <= 5
        GROUP BY event, level, dt, hr
    """)


def push_to_redis():
    ch_client = get_client(**CLICKHOUSE_CONN)
    r = redis.Redis(**REDIS_CONN)
    rows = ch_client.query("SELECT event, level, dt, hr, cats FROM petr_yurlov.lab5_ans").result_rows

    for event, level, dt, hr, cats in rows:
        key = f"{event}:{level}:{dt}:{hr}:top5"
        cat_set = {cat.strip() for cat in cats.split(',')}
        r.delete(key)
        r.sadd(key, *cat_set)
        print(f"Добавлено в Redis (SADD): {key} → {cat_set}")


# Описание DAG
extract_task = PythonOperator(task_id="extract_postgres", python_callable=extract_postgres, dag=dag)
clean_task = PythonOperator(task_id="clean_jsonl", python_callable=clean_jsonl, dag=dag)
clickhouse_task = PythonOperator(task_id="load_to_clickhouse", python_callable=load_to_clickhouse, dag=dag)
agg_task = PythonOperator(task_id="aggregate_clickhouse", python_callable=aggregate_clickhouse, dag=dag)
redis_task = PythonOperator(task_id="push_to_redis", python_callable=push_to_redis, dag=dag)

extract_task >> clean_task >> clickhouse_task >> agg_task >> redis_task
