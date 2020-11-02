import os

DB_NAME = os.environ.get('DB_NAME', 'gonzalo123')
DB_USER = os.environ.get('DB_USER', 'username')
DB_PASS = os.environ.get('DB_PASS', 'password')
DB_HOST = os.environ.get('DB_HOST', 'localhost')

DB_DSN = os.environ.get("DSN", f"dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASS}'")
