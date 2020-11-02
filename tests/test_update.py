from tests.sql import SQL_SINGLE


def test_update_one_row(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    assert 1 == db.update(
        table='users',
        data={'name': 'xxxx'},
        identifier={'email': 'user1@email.com'},
    )

    assert 'xxxx' == db.fetch_one(
        sql=SQL_SINGLE,
        where={'email': 'user1@email.com'})
