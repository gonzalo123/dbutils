def test_select(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    data = db.select(
        table='users',
        where={'email': 'user1@email.com'}
    )

    assert 1 == len(data)
    assert data[0]['email'] == 'user1@email.com'
    assert data[0]['name'] == 'user1'


def test_select_empty_recordset(db):
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})

    data = db.select(
        table='users',
        where={'email': 'xxx@email.com'}
    )

    assert 0 == len(data)
