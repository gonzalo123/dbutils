import psycopg2
from psycopg2._psycopg import cursor as psycopg_cursor

from dbutils.dbutils import transactional, get_cursor, Db, transactional_cursor
from tests.sql import SQL_COUNT


def test_simple_transaction(conn):
    cursor = get_cursor(conn=conn)
    db2 = Db(cursor=cursor)

    with transactional(conn) as db:
        assert isinstance(db.get_cursor(), psycopg_cursor)
        assert 1 == db.insert(
            table='users',
            values={'email': 'user1@email.com', 'name': 'user1'})

    assert 1 == db2.fetch_one(sql=SQL_COUNT)


def test_transaction_exception_rollback(conn):
    cursor = get_cursor(conn=conn)
    db2 = Db(cursor=cursor)
    assert 0 == db2.fetch_one(sql=SQL_COUNT)

    with transactional(conn) as db:
        assert isinstance(db.get_cursor(), psycopg_cursor)
        assert 1 == db.insert(
            table='users',
            values={'email': 'user1@email.com', 'name': 'user1'})
    assert 1 == db2.fetch_one(sql=SQL_COUNT)

    try:
        with transactional_cursor(conn) as cursor:
            assert isinstance(cursor, psycopg_cursor)
            db = Db(cursor)
            db.insert(
                table='users',
                values={'email': 'user1@email.com', 'name': 'user1'})
    except psycopg2.Error as e:
        assert isinstance(e, psycopg2.errors.UniqueViolation)

    with transactional(conn) as db:
        assert isinstance(db.get_cursor(), psycopg_cursor)
        assert 1 == db.insert(
            table='users',
            values={'email': 'user2@email.com', 'name': 'user2'})
    assert 2 == db2.fetch_one(sql=SQL_COUNT)
