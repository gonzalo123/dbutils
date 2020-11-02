import pytest
from dbutils.dbutils import get_conn, Db
import os

DSN = f"dbname='{os.getenv('POSTGRES_DB')}' user='{os.getenv('POSTGRES_USER')}' host='{os.getenv('POSTGRES_HOST')}' password='{os.getenv('POSTGRES_PASSWORD')}'"


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
