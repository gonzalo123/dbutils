SQL_COUNT = "SELECT count(1) FROM users"
SQL_ALL_USERS = "SELECT email, name from users"
SQL_EMPTY_RECORDSET = "SELECT email, name FROM users WHERE email='non-exitent@email.com'"
SQL_SINGLE = "SELECT name FROM users where email=%(email)s"

SQL_DELETE = "DELETE FROM users where email=%(email)s"
