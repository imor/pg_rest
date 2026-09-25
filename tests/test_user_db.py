from sqlalchemy import text

# pg_net's test_net_with_different_username_dbname is not ported: pg_rest has no
# pg_net.username/pg_net.database_name GUCs, the worker always connects to the "postgres"
# database as the bootstrap superuser (src/consts.rs).


def test_rest_appname(sess):
    """Check that pg_stat_activity has appname set"""

    (count,) = sess.execute(
        text(
            """
        select count(1) from pg_stat_activity where application_name ilike '%pg_rest%';
    """
        )
    ).fetchone()
    assert count == 1


def test_worker_connects_to_postgres_database(sess):
    """The worker connects to the database named in src/consts.rs"""

    (datname,) = sess.execute(
        text(
            """
        select datname from pg_stat_activity where backend_type ilike '%pg_rest%';
    """
        )
    ).fetchone()
    assert datname == "postgres"
