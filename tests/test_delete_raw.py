from psycopg2.extras import RealDictRow

from tests.sql import SQL_COUNT, SQL_DELETE


def test_delete_one_row(db):
    assert 0 == db.fetch_one(sql=SQL_COUNT)
    assert 1 == db.insert(
        table='users',
        values={'email': 'user1@email.com', 'name': 'user1'})
    assert 1 == db.execute(sql=SQL_DELETE, params=dict(email='user1@email.com'))



