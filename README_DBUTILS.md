# Database utils for psycopg2

[dbutils](https://github.com/gonzalo123/dbutils)

## Install

```commandline
pip install dbutils-gonzalo123
```

From github
```commandline
pip install -e git+https://github.com/gonzalo123/dbutils#egg=dbutils-gonzalo123
```

### Run test
```commandline
pytest
```

Note: Set up your connection parameters within pytest.ini. I also provide one docker-compose.yml file with a PostgreSQL database ready for the testing.

```ini
[pytest]
python_files = tests.py test_*.py *_tests.py
env =
    POSTGRES_HOST=localhost
    POSTGRES_PASSWORD=password
    POSTGRES_USER=username
    POSTGRES_DB=gonzalo123
```

In this database ther's one table and one stored procedure:

```sql
CREATE TABLE users
(
    email  VARCHAR(256) UNIQUE NOT NULL,
    "name" VARCHAR(256)        NOT NULL
);

CREATE OR REPLACE FUNCTION hello(name varchar(100))
    RETURNS VARCHAR AS
$$
BEGIN
    RETURN 'Hello ' || name;

END;
$$
    LANGUAGE plpgsql;
```
