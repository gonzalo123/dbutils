from contextlib import contextmanager
from functools import wraps

import psycopg2
from psycopg2 import sql as psycopg2_sql
from psycopg2.extras import RealDictConnection, RealDictCursor, execute_batch


@contextmanager
def transactional(conn, named=False):
    return _get_transactional(conn, named, lambda cursor: Db(cursor))


@contextmanager
def transactional_django(conn):
    return _get_transactional_django(conn, lambda cursor: DbDjango(cursor))


@contextmanager
def transactional_cursor_django(conn):
    return _get_transactional_django(conn, lambda cursor: cursor)


@contextmanager
def transactional_cursor(conn, named=False):
    return _get_transactional(conn, named, lambda cursor: cursor)


def get_conn(dsn, named=False, autocommit=False):
    conn = psycopg2.connect(
        dsn=dsn,
        connection_factory=RealDictConnection if named else None,
    )
    conn.autocommit = autocommit

    return conn


def get_cursor(conn, named=True):
    if named:
        return conn.cursor(cursor_factory=RealDictCursor)
    else:
        return conn.cursor()


def fetch_all(cursor, sql, params=None):
    cursor.execute(sql, {} if params is None else params)

    return cursor.fetchall()


def fetch_one(cursor, sql, where=None):
    where = {} if where is None else where
    cursor.execute(sql, where)
    data = cursor.fetchone()
    if data:
        return data[0] if type(data) is tuple else list(data.values())[0]
    else:
        return None


def _call_proc(cursor, function, params=None):
    params = {} if params is None else params
    cursor.callproc(function, params)


def sp_fetch_all(cursor, function, params=None):
    _call_proc(cursor, function, params)

    return cursor.fetchall()


def sp_fetch_one(cursor, function, params=None):
    _call_proc(cursor, function, params)
    data = cursor.fetchone()

    return data[0] if type(data) is tuple else list(data.values())[0]


def insert(cursor, table, values):
    sql = _get_insert_sql(table=table, values=values)
    if type(values) is list:
        cursor.executemany(sql, values)
    else:
        cursor.execute(sql, values)

    return cursor.rowcount


def insert_batch(cursor, table, values):
    execute_batch(
        cur=cursor,
        sql=_get_insert_sql(table=table, values=values),
        argslist=values)

    return cursor.rowcount


def select(cursor, table, where=None):
    schema, table = _get_table(table)
    if where:
        raw_sql = "SELECT * from {sch}.{tbl} where {t_where}"
    else:
        raw_sql = "SELECT * from {sch}.{tbl}"
        where = {}

    sql = psycopg2_sql.SQL(raw_sql).format(
        sch=psycopg2_sql.Identifier(schema),
        tbl=psycopg2_sql.Identifier(table),
        t_where=psycopg2_sql.SQL(' and ').join(_get_conditions(where)))

    cursor.execute(sql, where)
    return cursor.fetchall()


def delete(cursor, table, where=None):
    schema, table = _get_table(table)
    if where:
        raw_sql = "delete from {sch}.{tbl} where {t_where}"
    else:
        raw_sql = "delete from {sch}.{tbl}"
        where = {}

    sql = psycopg2_sql.SQL(raw_sql).format(
        sch=psycopg2_sql.Identifier(schema),
        tbl=psycopg2_sql.Identifier(table),
        t_where=psycopg2_sql.SQL(' and ').join(_get_conditions(where)))

    cursor.execute(sql, where)

    return cursor.rowcount


def update(cursor, table: str, data, identifier: object):
    sql = _get_update_sql(data, identifier, table)
    cursor.execute(sql, {**data, **identifier})

    return cursor.rowcount


def upsert(cursor, table, data, identifier):
    merger_data, sql = _get_upsert_sql(data, identifier, table)

    cursor.execute(sql, merger_data)
    return cursor.rowcount


def _get_table(table):
    data = table.split(".")
    if len(data) == 1:
        return "public", table
    else:
        return data[0], data[1]


def _get_transactional_django(conn, callback):
    try:
        with get_cursor(conn=conn, named=False) as cursor:
            yield callback(cursor)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def _get_transactional(conn, named, callback):
    try:
        with conn as connection:
            with get_cursor(conn=connection, named=named) as cursor:
                yield callback(cursor)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def _get_insert_sql(table, values):
    keys = values[0].keys() if type(values) is list else values.keys()
    schema, table = _get_table(table)

    raw_sql = "insert into {sch}.{tbl} ({t_fields}) values ({t_values})"
    return psycopg2_sql.SQL(raw_sql).format(
        sch=psycopg2_sql.Identifier(schema),
        tbl=psycopg2_sql.Identifier(table),
        t_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Identifier, keys)),
        t_values=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Placeholder, keys)))


def _get_update_sql(data, identifier, table):
    schema, table = _get_table(table)
    raw_sql = "update {sch}.{tbl} set {t_set} where {t_where}"
    sql = psycopg2_sql.SQL(raw_sql).format(
        sch=psycopg2_sql.Identifier(schema),
        tbl=psycopg2_sql.Identifier(table),
        t_set=psycopg2_sql.SQL(', ').join(_get_conditions(data)),
        t_where=psycopg2_sql.SQL(' and ').join(_get_conditions(identifier)))
    return sql


def _get_upsert_sql(data, identifier, table):
    raw_sql = """
        WITH
            upsert AS (
                UPDATE {sch}.{tbl}
                SET {t_set}
                WHERE {t_where}
                RETURNING {sch}.{tbl}.*),
            inserted AS (
                INSERT INTO {sch}.{tbl} ({t_fields})
                SELECT {t_select_fields}
                WHERE NOT EXISTS (SELECT 1 FROM upsert)
                RETURNING *)
        SELECT * FROM upsert
        UNION ALL
        SELECT * FROM inserted
    """
    merger_data = {**data, **identifier}
    schema, table = _get_table(table)
    sql = psycopg2_sql.SQL(raw_sql).format(
        sch=psycopg2_sql.Identifier(schema),
        tbl=psycopg2_sql.Identifier(table),
        t_set=psycopg2_sql.SQL(', ').join(_get_conditions(data)),
        t_where=psycopg2_sql.SQL(' and ').join(_get_conditions(identifier)),
        t_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Identifier, merger_data.keys())),
        t_select_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Placeholder, merger_data.keys())))

    return merger_data, sql


def _get_conditions(where):
    return [psycopg2_sql.SQL("{identifier} = {placeholder}").format(
        identifier=psycopg2_sql.Identifier(key),
        placeholder=psycopg2_sql.Placeholder(key)) for key in where]


class Db:
    def __init__(self, cursor):
        self.cursor = cursor

    def fetch_all(self, sql, params=None):
        return fetch_all(self.cursor, sql, params)

    def select(self, table, where=None):
        return select(self.cursor, table, where)

    def upsert(self, table, data, identifier):
        return upsert(self.cursor, table, data, identifier)

    def update(self, table, data, identifier):
        return update(self.cursor, table, data, identifier)

    def delete(self, table, where=None):
        return delete(self.cursor, table, where)

    def insert(self, table, values):
        return insert(self.cursor, table, values)

    def insert_batch(self, table, values):
        return insert_batch(self.cursor, table, values)

    def sp_fetch_one(self, function, params=None):
        return sp_fetch_one(self.cursor, function, params)

    def sp_fetch_all(self, function, params=None):
        return sp_fetch_all(self.cursor, function, params)

    def fetch_one(self, sql, where=None):
        return fetch_one(self.cursor, sql, where)

    def get_cursor(self):
        return self.cursor


def append_column_names(func):
    @wraps(func)
    def wrapper_decorator(*args, **kwargs):
        db = args[0]
        data = func(*args, **kwargs)
        column_names = [desc[0] for desc in db.get_cursor().description]
        return [dict(zip(column_names, row)) for row in data]

    return wrapper_decorator


class DbDjango(Db):
    @append_column_names
    def fetch_all(self, sql, params=None):
        return super().fetch_all(sql, params)

    @append_column_names
    def select(self, table, where=None):
        return super().select(table, where)
