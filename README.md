# Database utils for psycopg2

[![PyPI version fury.io](https://badge.fury.io/py/ansicolortags.svg)](https://pypi.org/project/dbutils-gonzalo123/)
[![GitHub license](https://img.shields.io/github/license/Naereen/StrapDown.js.svg)](https://github.com/gonzalo123/dbutils/blob/master/LICENSE)

I normally need to perform raw queries when I'm working with Python. Even when I'm using Django and it's ORM I need to execute sql against the database. As I use (almost always) PostgreSQL, I use (as we all do) psycopg2. Psycopg2 is very complete and easy to use but sometimes I need a few helpers to make my usual tasks easier. I don't want to create a complex library on top of psycopg2, only a helper.

Something that I miss from PHP and DBAL library is the way that DBAL helps me to create simple CRUD operations. The idea of this helper functions is to do something similar. Let's start

I've created procedural functions to do all and also a Db class.

Normally all functions needs the cursor parameter.
```python
def fetch_all(cursor, sql, params=None):
    cursor.execute(
        query=sql,
        vars={} if params is None else params)

    return cursor.fetchall()
```

The Db class accepts cursor in the constructor

```python
db = Db(cursor=cursor)
data = db.fetch_all("SELECT * FROM table")
```

## Connection
Nothing especial in the connection. I like to use "connection_factory=[NamedTupleConnection](https://www.psycopg.org/docs/extras.html?highlight=namedtupleconnection#psycopg2.extras.NamedTupleConnection)" to allow me to access to the recordset as an Object. I've created simple factory helper to create connections:

```python
def get_conn(dsn, named_tuple=False, autocommit=False):
    conn = psycopg2.connect(
        dsn=dsn,
        connection_factory=NamedTupleConnection if named_tuple else None,
    )
    conn.autocommit = autocommit

    return conn
```

Sometimes I use [NamedTuple](https://www.psycopg.org/docs/extras.html?highlight=namedtuple#namedtuple-cursor) in the cursor instead connection, so I've created a simple helper

```python
def get_cursor(conn, named_tuple=True):
    return conn.cursor(cursor_factory=NamedTupleCursor if named_tuple else None)
```

## Fetch all
```python
db = Db(cursor)
for reg in db.fetch_all(sql="SELECT email, name from users"):
    assert 'user1' == reg.name
    assert 'user1@email.com' == reg.email
```

## Fetch one
```python
data == db.fetch_one(sql=SQL)
```

## Select
```python
data = db.select(
        table='users',
        where={'email': 'user1@email.com'}
    )
```
This helper only allows me to perform simple where statements (joined with AND). If I need one complex Where, then I use fetch

## Insert
Insert one row
```python
db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})
```

Or insert multiple rows (using spycopg2's [executemany](https://www.psycopg.org/docs/cursor.html#cursor.executemany))
```python
db.insert(
        table='users',
        values=[
            {'email': 'user2@email.com', 'name': 'user2'},
            {'email': 'user3@email.com', 'name': 'user3'}
        ])
```
We also can use insert_batch to insert all rows within one command
```python
db.insert_batch(
        table='users',
        values=[
            {'email': 'user2@email.com', 'name': 'user2'},
            {'email': 'user3@email.com', 'name': 'user3'}
        ])
```
## Update
```python
db.update(
        table='users',
        data={'name': 'xxxx'},
        identifier={'email': 'user1@email.com'},
    )
```

## Delete
```python
db.delete(table='users', where={'email': 'user1@email.com'})
```

## Upsert
Sometimes we need to insert one row or update the row if the primary key already exists. We can do that with two statements: One select and if there isn't any result one insert. Else on update. We can do that with one sql statement. I've created a helper to to that:
```python
def _get_upsert_sql(data, identifier, table):
    raw_sql = """
        WITH
            upsert AS (
                UPDATE {tbl}
                SET {t_set}
                WHERE {t_where}
                RETURNING {tbl}.*),
            inserted AS (
                INSERT INTO {tbl} ({t_fields})
                SELECT {t_select_fields}
                WHERE NOT EXISTS (SELECT 1 FROM upsert)
                RETURNING *)
        SELECT * FROM upsert
        UNION ALL
        SELECT * FROM inserted
    """
    merger_data = {**data, **identifier}
    sql = psycopg2_sql.SQL(raw_sql).format(
        tbl=psycopg2_sql.Identifier(table),
        t_set=psycopg2_sql.SQL(', ').join(_get_conditions(data)),
        t_where=psycopg2_sql.SQL(' and ').join(_get_conditions(identifier)),
        t_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Identifier, merger_data.keys())),
        t_select_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Placeholder, merger_data.keys())))

    return merger_data, sql


def upsert(cursor, table, data, identifier):
    merger_data, sql = _get_upsert_sql(data, identifier, table)

    cursor.execute(sql, merger_data)
    return cursor.rowcount
```

Now we can execute a upsert in a simple way:

```python
db.upsert(
        table='users',
        data={'name': 'yyyy'},
        identifier={'email': 'user1@email.com'})
```

## Stored procedures
```python
data = db.sp_fetch_one(
    function='hello',
    params={'name': 'Gonzalo'})
```

```python
data = db.sp_fetch_all(
    function='hello',
    params={'name': 'Gonzalo'})
```


## Transactions
In PHP and [DBAL](https://www.doctrine-project.org/projects/doctrine-dbal/en/2.12/reference/transactions.html) to create one transaction we can use this syntax
```php
<?php
$conn->transactional(function($conn) {
    // do stuff
});
```

In Python we can do the same with context management, but I prefer to use this syntax:
```python
with transactional(conn) as db:
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})
```

The transactional function is like that (I've created two functions. One with raw cursor and another one with my Db class):
```python
def _get_transactional(conn, named_tuple, callback):
    try:
        with conn as connection:
            with get_cursor(conn=connection, named_tuple=named_tuple) as cursor:
                yield callback(cursor)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

@contextmanager
def transactional(conn, named_tuple=False):
    return _get_transactional(conn, named_tuple, lambda cursor: Db(cursor))


@contextmanager
def transactional_cursor(conn, named_tuple=False):
    return _get_transactional(conn, named_tuple, lambda cursor: cursor)
```

## Return values
Select and fetch helpers returns the recordset, but insert, update, delete and upsert returns the number of affected rows. For example that's the insert function:

```python
def _get_insert_sql(table, values):
    keys = values[0].keys() if type(values) is list else values.keys()

    raw_sql = "insert into {tbl} ({t_fields}) values ({t_values})"
    return psycopg2_sql.SQL(raw_sql).format(
        tbl=psycopg2_sql.Identifier(table),
        t_fields=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Identifier, keys)),
        t_values=psycopg2_sql.SQL(', ').join(map(psycopg2_sql.Placeholder, keys)))


def insert(cursor, table, values):
    sql = _get_insert_sql(table=table, values=values)
    cursor.executemany(query=sql, vars=values) if type(values) is list else cursor.execute(sql, values)

    return cursor.rowcount
```

## Run tests
You can try the library running the unit tests. I've also provide one docker-compose.yml file to set up a PostgreSQL database to run the tests

```yaml
version: '3.6'

services:
  pg:
    build:
      context: .docker/pg
      dockerfile: Dockerfile
    ports:
      - 5432:5432
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_DB: ${POSTGRES_DB}
      PGDATA: /var/lib/postgresql/data/pgdata
```

## Install

You can install the library using pip

```
pip install dbutils-gonzalo123
```
