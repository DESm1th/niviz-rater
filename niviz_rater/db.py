import os

from peewee import Database, SqliteDatabase, PostgresqlDatabase
from typing import Any, List, Dict, Optional, Callable
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_or_create_db(
        app_config: Dict[str, str],
        additional_pragmas: Optional[List[Any]] = None) -> Database:

    if 'datman_config' in app_config:
        return create_postgres_db(app_config)

    db_str = app_config['niviz_rater.db.file']
    pragmas = [('foreign_keys', 'on')]
    if additional_pragmas:
        pragmas.append(additional_pragmas)

    sqlite_db = SqliteDatabase(db_str, pragmas=pragmas, uri=True)
    return sqlite_db


def fetch_db_from_config(app_config,
                         additional_pragmas: Optional[List[Any]] = None):
    """
    Fetch database by first trying to pull from
    stored application configuration and if fail, then
    resort to requesting one using db_file
    """

    return app_config.get(
        'niviz_rater.db.instance',
        get_or_create_db(app_config,
                         additional_pragmas))


def create_postgres_db(app_config: Dict[str, str]) -> PostgresqlDatabase:
    dm_config = app_config['datman_config']
    name = dm_config['db_name']
    user = os.getenv(dm_config['user'])
    password = os.getenv(dm_config['password'])
    host = os.getenv(dm_config['server'])

    connection = psycopg2.connect(
        dbname='postgres',
        user=user,
        host=host,
        password=password)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    try:
        cursor.execute(sql.SQL(f'CREATE DATABASE {name}'))
    except psycopg2.errors.DuplicateDatabase:
        pass

    return PostgresqlDatabase(name, user=user, password=password, host=host)
