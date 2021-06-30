from tests.sql import SQL_SINGLE


def test_fetch_one(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    assert 'user1' == db.fetch_one(
        sql=SQL_SINGLE,
        where={'email': 'user1@email.com'})


def test_fetch_one_non_existent(db):
    assert None is db.fetch_one(
        sql=SQL_SINGLE,
        where={'email': 'non_existent@email.com'})
