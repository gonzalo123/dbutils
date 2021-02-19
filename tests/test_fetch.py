from tests.sql import SQL_ALL_USERS, SQL_EMPTY_RECORDSET, SQL_COUNT


def test_fetch_all(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    for reg in db.fetch_all(sql=SQL_ALL_USERS):
        assert 'user1' == reg['name']
        assert 'user1@email.com' == reg['email']


def test_fetch_one(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    assert 1 == db.fetch_one(sql=SQL_COUNT)


def test_fetch_none(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    assert None is db.fetch_one(sql=SQL_EMPTY_RECORDSET)


def test_empty_recordset(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    data = db.fetch_all(sql=SQL_EMPTY_RECORDSET)
    assert 0 == len(data)
