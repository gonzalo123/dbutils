from tests.sql import SQL_COUNT, SQL_SINGLE


def test_upsert_existing_row(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    assert 1 == db.fetch_one(sql=SQL_COUNT)

    assert 1 == db.upsert(
        table='users',
        data={'name': 'yyyy'},
        identifier={'email': 'user1@email.com'})

    assert 'yyyy' == db.fetch_one(
        sql=SQL_SINGLE,
        where={'email': 'user1@email.com'})

    assert 1 == db.fetch_one(sql=SQL_COUNT)


def test_upsert_non_existing_row(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.upsert(
        table='users',
        data={'name': 'zzzz'},
        identifier={'email': 'user1@email.com'},
    )
    assert 1 == db.fetch_one(sql=SQL_COUNT)

    assert 'zzzz' == db.fetch_one(
        sql=SQL_SINGLE,
        where={'email': 'user1@email.com'})
