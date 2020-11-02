from tests.sql import SQL_COUNT, SQL_ALL_USERS


def test_delete_one_row(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})
    assert 1 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.delete(table='users', where={'email': 'user1@email.com'})
    assert 0 == db.fetch_one(sql=SQL_COUNT)


def test_delete_two_rows(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 2 == db.insert(
        table='users',
        values=[
            {'email': 'user2@email.com', 'name': 'user2'},
            {'email': 'user3@email.com', 'name': 'user3'}
        ])
    assert 2 == db.fetch_one(sql=SQL_COUNT)
    assert 2 == db.delete(table='users')
    assert 0 == db.fetch_one(sql=SQL_COUNT)


def test_delete_one_row_where(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 2 == db.insert(
        table='users',
        values=[
            {'email': 'user2@email.com', 'name': 'user2'},
            {'email': 'user3@email.com', 'name': 'user3'}
        ])
    assert 2 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.delete(table='users', where={'email': 'user3@email.com'})
    assert 1 == db.fetch_one(sql=SQL_COUNT)
    data = db.fetch_all(sql=SQL_ALL_USERS)

    assert 'user2' == data[0].name
    assert 'user2@email.com' == data[0].email


def test_delete_zero_rows(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 2 == db.insert(
        table='users',
        values=[
            {'email': 'user2@email.com', 'name': 'user2'},
            {'email': 'user3@email.com', 'name': 'user3'}
        ])
    assert 2 == db.fetch_one(sql=SQL_COUNT)
    assert 0 == db.delete(table='users', where={'email': 'xxxx@email.com'})
    assert 2 == db.fetch_one(sql=SQL_COUNT)
