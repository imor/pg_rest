import os

# Connection settings come from the standard libpq environment variables. Default them to the
# local test cluster only when they aren't set, so the suite can run against any server.
for _name, _default in {
    "PGHOST": "127.0.0.1",
    "PGPORT": "28818",
    "PGUSER": "postgres",
    "PGDATABASE": "postgres",
}.items():
    os.environ.setdefault(_name, _default)

import psycopg  # noqa: E402
import pytest  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from common import PSYCOPG_CONNSTR  # noqa: E402

# Equivalent of pg_net's test/init.sql and test/utils/helpers.sql.
SETUP_SQL = """
do $$
begin
  if not exists (select from pg_roles where rolname = 'pre_existing') then
    create role pre_existing nosuperuser login;
  end if;
end
$$;

create or replace function public.is_worker_up() returns bool as $$
  select exists(select pid from pg_stat_activity where backend_type ilike '%pg_rest%');
$$ language sql;

create or replace function public.kill_worker() returns bool as $$
  select pg_terminate_backend(pid) from pg_stat_activity where backend_type ilike '%pg_rest%';
$$ language sql;
"""

TEARDOWN_SQL = """
drop function if exists public.is_worker_up();
drop function if exists public.kill_worker();
drop role if exists pre_existing;
"""


@pytest.fixture(scope="session", autouse=True)
def test_setup():
    """Creates the helpers pg_net's init.sql creates, and removes them afterwards."""

    with psycopg.connect("", autocommit=True) as c:
        was_installed = (
            c.execute("select count(*) = 1 from pg_extension where extname = 'pg_rest'").fetchone()[0]
        )
        c.execute(SETUP_SQL)
        # Every test starts without the extension (the fixtures create it), so that request ids
        # start at 1.
        c.execute("drop extension if exists pg_rest cascade")

    yield

    with psycopg.connect("", autocommit=True) as c:
        c.execute(TEARDOWN_SQL)
        if was_installed:
            c.execute("create extension if not exists pg_rest")


@pytest.fixture(scope="function")
def engine():
    engine = create_engine(PSYCOPG_CONNSTR)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def conn():
    """Direct connection via psycopg"""

    conn = psycopg.connect("")

    conn.execute("create extension if not exists pg_rest;")
    conn.commit()

    yield conn

    conn.rollback()

    conn.execute("drop extension if exists pg_rest cascade;")
    conn.commit()
    conn.close()


@pytest.fixture(scope="function")
def sess(engine):
    session = Session(engine)

    session.execute(
        text(
            """
    create extension if not exists pg_rest;
    """
        )
    )
    session.commit()

    yield session

    session.rollback()

    session.execute(
        text(
            """
    drop extension if exists pg_rest cascade;
    """
        )
    )
    session.commit()
    session.close()


@pytest.fixture(scope="function")
def autocommit_sess(engine):
    ac_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    session = Session(ac_engine)

    yield session

    session.close()
