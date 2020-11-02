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
