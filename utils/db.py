import os

import psycopg2


def get_pg_conn(database=None, connection_factory=None, initial=None):
    """
    Must be manually closed
    """
    if not database:
        database = os.environ.get('DB_NAME')
    conn = psycopg2.connect(
        connection_factory=connection_factory,
        dbname=database,
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT', 5439)
    )
    if initial:
        conn.initialize(initial)
    cur = conn.cursor()
    return conn, cur
