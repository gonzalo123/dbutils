import pytest
from dbutils.dbutils import get_conn, Db
import os

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')

DSN = f"dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASS}'"


@pytest.fixture(scope="function", autouse=True)
def init():
    conn = get_conn(dsn=DSN, named_tuple=True, autocommit=False)
    with conn as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM users")


@pytest.fixture()
def db(cursor):
    return Db(cursor=cursor)


@pytest.fixture()
def conn():
    yield get_conn(dsn=DSN, named_tuple=True, autocommit=False)


@pytest.fixture()
def cursor(conn):
    with conn as conn:
        with conn.cursor() as cursor:
            yield cursor
