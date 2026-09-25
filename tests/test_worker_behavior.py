from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import time
import subprocess
import os
import shutil
import pytest
from common import fetch_worker_pid, get_queue_length, wait_for_claimed_count
from common import http_requests, restart_worker, wait_for_any_response
from common import wait_for_extension_drop, wait_for_postgres_ready
from common import wait_for_queue_drain, wait_for_response_count
from common import wait_for_worker_down, wait_for_worker_state
from common import wait_for_worker_up, wait_until, wakeup_worker
from common import PSYCOPG_CONNSTR


def test_worker_will_not_block_drop_database(autocommit_sess):
    """Check that the worker will not block a session doing drop database"""

    autocommit_sess.execute(text("create database foo;"))
    autocommit_sess.execute(text("drop database foo;"))

    (result,) = autocommit_sess.execute(
        text("""
        select 1
    """)
    ).fetchone()

    assert result is not None
    assert result == 1

    (pg_version,) = autocommit_sess.execute(
        text(
            """
        select current_setting('server_version_num');
    """
        )
    ).fetchone()

    # drop database with force only available from pg 13
    if int(pg_version) >= 130000:
        autocommit_sess.execute(text("create database foo;"))
        autocommit_sess.execute(text("drop database foo with (force);"))

        (result,) = autocommit_sess.execute(
            text("""
            select 1
        """)
        ).fetchone()

        assert result is not None
        assert result == 1


def test_success_when_worker_is_up(sess):
    """rest.check_worker_is_up should not return anything when the worker is running"""

    (result,) = sess.execute(
        text("""
        select rest.wait_until_running();
        select rest.check_worker_is_up();
    """)
    ).fetchone()
    assert result is not None
    assert result == ""


def test_worker_will_process_queue_when_up(sess, autocommit_sess):
    """
    Check that when pg background worker is down and requests arrive,
    it will process them once it wakes up
    """

    # Assert that background worker is worker up
    (worker_is_up,) = sess.execute(
        text("""
        select is_worker_up();
    """)
    ).fetchone()
    assert worker_is_up

    # kill background worker
    (killed,) = sess.execute(
        text("""
        select public.kill_worker();
    """)
    ).fetchone()
    assert killed is not None
    assert killed

    # Wait for background worker to go down
    wait_for_worker_down(autocommit_sess)

    # Make a request while the worker is down
    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10);
    """
        ),
    )

    # Check that requests were enqueued
    (count,) = sess.execute(
        text(
            """
        select count(*) from rest.http_request_queue;
    """
        )
    ).fetchone()
    assert count == 10

    # Check that worker is still down. Note that this is a bit racy
    # as there is no guarantee that worker hasn't come back up at this
    # time, but in practice this rarely fails because worker takes 2
    # seconds before coming back up, which is more than enough time
    # to reach here.
    (worker_is_up,) = sess.execute(
        text("""
        select is_worker_up();
    """)
    ).fetchone()
    assert not worker_is_up

    sess.commit()

    # Wait for background worker to come back up
    # It's critical to use autocommit_sess to see a new snapshot
    # on each retry in wait_until, otherwise it might keep reading
    # stale data and fail with a timeout.
    wait_for_worker_up(autocommit_sess)

    # Wait for request queue to drain
    wait_for_queue_drain(autocommit_sess)

    # Wait until all responses have arrived
    wait_for_response_count(autocommit_sess, 10)


def test_can_delete_rows_while_processing_queue(sess, autocommit_sess):
    """
    Check that a user can delete the queue rows while the worker is
    processing them

    pg_net lowers pg_net.batch_size to 1 so that rows are still queued while
    the worker processes the first ones. pg_rest keeps a request's row in the
    queue while it is in flight, so slow requests are used instead: the rows
    are deleted while they are claimed and in flight.
    """

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200&delay=1') from generate_series(1,10);
    """
        ),
    )

    # Wait until all requests are in flight
    wait_for_claimed_count(autocommit_sess, 10)

    (count,) = sess.execute(
        text(
            """
        with deleted as (delete from rest.http_request_queue returning *) select count(*) from deleted;
    """
        )
    ).fetchone()
    assert count == 10

    sess.commit()

    # The responses of the deleted requests still arrive, and the worker keeps working
    wait_for_response_count(autocommit_sess, 10)
    assert get_queue_length(autocommit_sess)() == 0


def test_truncate_wait_while_processing_queue(sess, autocommit_sess):
    """
    Check that a truncate will not wait until the worker
    is done processing all requests

    pg_net lowers pg_net.batch_size to 1 so that the worker processes the
    queue slowly. pg_rest holds no lock on the queue while requests are in
    flight, so the truncate is done while slow requests are in flight.
    """

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200&delay=2') from generate_series(1,10);
    """
        ),
    )

    wait_for_claimed_count(autocommit_sess, 10)

    # truncate succeeds fast, despite the worker still processing the queue
    started = time.time()
    sess.execute(
        text(
            """
        set local lock_timeout = '1s';
        truncate rest.http_request_queue;
    """
        )
    )
    assert time.time() - started < 1

    # now the queue will be empty
    (count,) = sess.execute(
        text(
            """
        select count(*) from rest.http_request_queue;
    """
        )
    ).fetchone()
    assert count == 0

    sess.commit()

    # the worker keeps working
    wait_for_response_count(autocommit_sess, 10)
    wait_for_worker_state(autocommit_sess, "idle")


def test_no_failure_on_drop_extension(sess, autocommit_sess):
    """
    Check that while waiting for a slow request, a drop extension should
    wait and not crash the worker
    """

    old_pid = fetch_worker_pid(autocommit_sess)

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200&delay=2') from generate_series(1,10);
    """
        ),
    )

    # Wait until the requests are in flight (pg_rest sends all 10 at once, so waiting for
    # responses like pg_net does would drop the extension after they are done)
    wait_for_claimed_count(autocommit_sess, 10)

    sess.execute(
        text("""
        drop extension pg_rest cascade;
    """)
    )

    sess.commit()

    # wait until the extension is fully gone
    wait_for_extension_drop(autocommit_sess)

    # wait until the in-flight responses have arrived and been discarded
    time.sleep(2.5)

    # The background worker should not have crash even after dropping the extension
    (up,) = sess.execute(
        text("""
        select is_worker_up();
    """)
    ).fetchone()
    assert up is not None
    assert up
    assert fetch_worker_pid(autocommit_sess) == old_pid

    # And it works once the extension is created again
    sess.execute(text("create extension pg_rest"))
    sess.commit()
    http_requests(sess, text("select rest.http_get('http://localhost:8080/pathological?status=200')"))
    wait_for_response_count(autocommit_sess, 1)


def test_worker_will_keep_processing_queue_when_restarted(sess, autocommit_sess):
    """
    Check that when the background worker is restarted while working,
    it will pick up the remaining requests

    pg_net lowers pg_net.batch_size to 1 so that some requests are still
    queued when it restarts. pg_rest claims all 5 at once, so here they are
    in flight when it restarts; the next worker sends them again.
    """

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200&delay=1') from generate_series(1,5);
    """
        ),
    )

    wait_for_claimed_count(autocommit_sess, 5)

    restart_worker(autocommit_sess)

    wait_for_claimed_count(autocommit_sess, 5)

    restart_worker(autocommit_sess)

    # And now wait until all responses have arrived
    wait_for_response_count(autocommit_sess, 5)
    wait_for_queue_drain(autocommit_sess)

    (status_code, count) = autocommit_sess.execute(
        text("select status_code, count(*) from rest._http_response group by status_code")
    ).fetchone()
    assert (status_code, count) == (200, 5)


def test_new_requests_get_attended_without_explicit_wakeup(sess, autocommit_sess):
    """Check that new requests get attended without an explicit wakeup"""

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10);
    """
        ),
    )

    # wait until all responses have arrived
    wait_for_response_count(autocommit_sess, 10)


def test_direct_inserts_no_requests(sess, autocommit_sess):
    """
    Check that direct insertions to the rest.http_request_queue doesn't
    trigger new requests
    """

    # Make sure the worker has already settled into its idle wait before we
    # insert.
    wait_for_worker_state(autocommit_sess, "idle")

    sess.execute(
        text(
            """
        insert into rest.http_request_queue(method, url, headers, timeout_milliseconds)
        values (
            'GET',
            'http://localhost:8080/pathological?status=200',
            '{}',
            5000
        );
    """
        )
    )

    sess.commit()

    # Even waiting for 3 seconds doesn't process the request
    time.sleep(3)

    # No response still
    (count,) = sess.execute(
        text(
            """
        select count(*) from rest._http_response;
    """
        )
    ).fetchone()

    assert count == 0

    # Reqest is still in queue
    (count,) = sess.execute(
        text(
            """
        select count(*) from rest.http_request_queue;
    """
        )
    ).fetchone()

    assert count == 1

    # An explicit wake will make it serve requests though
    wakeup_worker(sess)

    # wait until the response has arrived
    wait_for_response_count(autocommit_sess, 1)


@pytest.mark.skipif(
    not os.getenv("PGDATA") or not shutil.which("pg_ctl"),
    reason="needs PGDATA and pg_ctl in PATH to restart the server",
)
def test_processing_survives_postmaster_crash(autocommit_sess):
    """
    Check that the queue will continue processing even when a postmaster
    crash or restart happens

    pg_net lowers pg_net.batch_size to 5 so that some requests are still
    queued when postgres restarts. With pg_rest, the requests are slow so
    that they are claimed and in flight when postgres restarts; the new
    worker resets the claims and sends them again.

    The server is restarted with `pg_ctl restart -D $PGDATA -l $PG_LOGFILE`
    (PG_LOGFILE defaults to "$PGDATA.log").
    """

    engine = create_engine(PSYCOPG_CONNSTR)
    ac_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    tmp_sess = Session(ac_engine)

    try:
        tmp_sess.execute(text("create extension if not exists pg_rest;"))

        tmp_sess.execute(
            text(
                """
            select rest.http_get('http://localhost:8080/pathological?status=200&delay=1') from generate_series(1,10);
        """
            )
        ).fetchone()

        (count,) = tmp_sess.execute(
            text(
                """
            select count(*) from rest.http_request_queue;
        """
            )
        ).fetchone()
        assert count == 10

        wait_for_claimed_count(tmp_sess, 10)

        tmp_sess.close()
        engine.dispose()

        pgdata_env = os.getenv("PGDATA")
        logfile = os.getenv("PG_LOGFILE", pgdata_env.rstrip("/") + ".log")
        subprocess.run(
            ["pg_ctl", "restart", "-w", "-D", pgdata_env, "-l", logfile],
            check=True,
            stdout=subprocess.DEVNULL,
        )

        # wait for postmaster to finish restarting and accept connections
        wait_for_postgres_ready(engine, tmp_sess)

        # Recreate engine and session after restart
        engine = create_engine(PSYCOPG_CONNSTR)
        ac_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
        tmp_sess = Session(ac_engine)

        # wait until the queue has finished processing
        wait_for_queue_drain(tmp_sess)

        (status_code, count) = tmp_sess.execute(
            text(
                """
            select status_code, count(*) from rest._http_response group by status_code;
        """
            )
        ).fetchone()

        assert status_code == 200
        assert count == 10

    finally:
        tmp_sess.close()
        engine.dispose()


def test_worker_writes_increment_pgstat_counters(sess, autocommit_sess):
    """
    Check that the worker's INSERTs into rest._http_response must be reflected
    in pg_stat_user_tables. Without this, autovacuum/autoanalyze can never be
    scheduled and the table silently bloats.
    """

    # Make sure the worker is fully up before we start, otherwise we race
    # with whatever the previous test left behind.
    autocommit_sess.execute(text("select rest.wait_until_running();"))

    # Clean baseline so deltas are unambiguous.
    autocommit_sess.execute(
        text(
            "select pg_stat_reset_single_table_counters('rest._http_response'::regclass);"
        )
    )

    # Drive a batch of requests through the worker.
    http_requests(
        sess,
        text("""
        select rest.http_get('http://localhost:8080/pathological?status=200')
        from generate_series(1,30);
    """),
    )

    # Wait until the worker has actually drained the queue and written all
    # responses to rest._http_response. Don't assume "30 rows" - the worker
    # may pick up the queue in chunks depending on wake() coalescing.
    wait_until(
        fetch=lambda: sess.execute(
            text("select count(*) from rest.http_request_queue;")
        ).fetchone(),
        predicate=lambda result: result[0] == 0,
        timeout=10,
        sleep_interval=0.5,
        description="worker to drain the request queue",
    )

    # Confirm the worker actually wrote rows, otherwise the pgstat assertion
    # below would be meaningless.
    (resp_count,) = autocommit_sess.execute(
        text("select count(*) from rest._http_response;")
    ).fetchone()
    assert resp_count > 0, "worker did not write any responses"

    # Poll for the pgstat counter to reflect the worker's INSERTs. With
    # `pgstat_report_stat(false)` and PGSTAT_MIN_INTERVAL = 1000ms, the
    # worker's first flush attempt is normally a hit (last_flush is far in
    # the past after a long idle), but we allow generous slack here so an
    # off-by-a-tick scheduling doesn't flake the suite.
    (resp_ins, resp_mod) = wait_until(
        fetch=lambda: autocommit_sess.execute(
            text("""
            select n_tup_ins, n_mod_since_analyze
            from pg_stat_user_tables where relname='_http_response';
        """)
        ).fetchone(),
        predicate=lambda result: result[0] > 0,
        timeout=30,
        sleep_interval=0.5,
        description="rest._http_response pgstat counters to reflect worker INSERTs",
    )

    assert resp_ins > 0, (
        f"rest._http_response.n_tup_ins is still 0 after 30s. "
        f"Worker wrote {resp_count} responses but none reached pgstat. "
        f"This is the pre-fix behaviour - autovacuum/autoanalyze will never "
        f"be scheduled and the table will silently bloat."
    )
    assert resp_mod > 0, (
        f"rest._http_response.n_mod_since_analyze is still 0 after 30s "
        f"(n_tup_ins reported {resp_ins})."
    )


def test_worker_writes_trigger_autoanalyze_on_http_response(sess, autocommit_sess):
    """
    Check that autoanalyze on rest._http_response must fire after the worker
    writes enough rows. Without working pgstat counters, autovacuum/autoanalyze
    never get scheduled and the table bloats - this is the primary symptom
    seen on production (slow expiry DELETEs from a bloated index).
    """

    try:
        # Make sure the worker is fully up before we start.
        autocommit_sess.execute(text("select rest.wait_until_running();"))

        # Make autovacuum eager *before* generating traffic so the launcher is
        # already running on a 1s naptime by the time stats threshold is crossed.
        # autovacuum_naptime is PGC_SIGHUP (reloadable). Give the reload a moment
        # to propagate to the launcher.
        autocommit_sess.execute(text("alter system set autovacuum_naptime = '1s';"))
        autocommit_sess.execute(text("select pg_reload_conf();"))

        wait_until(
            fetch=lambda: autocommit_sess.execute(
                text("select current_setting('autovacuum_naptime');")
            ).fetchone(),
            predicate=lambda result: result[0] == "1s",
            timeout=5,
            sleep_interval=0.1,
            description="autovacuum_naptime reload to propagate",
        )

        # Per-table: trip the autoanalyze threshold after a handful of rows.
        # Reloptions take effect immediately; no reload required.
        autocommit_sess.execute(
            text("""
            alter table rest._http_response set (
                autovacuum_analyze_threshold = 10,
                autovacuum_analyze_scale_factor = 0,
                autovacuum_vacuum_threshold = 10,
                autovacuum_vacuum_scale_factor = 0
            );
        """)
        )

        autocommit_sess.execute(
            text(
                "select pg_stat_reset_single_table_counters('rest._http_response'::regclass);"
            )
        )

        # Drive 30 inserts through the worker. 30 is well above the threshold (10).
        http_requests(
            sess,
            text("""
            select rest.http_get('http://localhost:8080/pathological?status=200')
            from generate_series(1,30);
        """),
        )

        # 30s budget covers worst-case worker pgstat flush (PGSTAT_MIN_INTERVAL
        # = 1s slack) + worst-case launcher cycle (autovacuum_max_workers=3,
        # 3 databases at 1s naptime each ~= 3s/cycle, with 2-3 cycle slack) +
        # autoanalyze worker spawn + ANALYZE on a tiny table (sub-second).
        # Real wall time on a clean rig is typically ~2-5s; the slack is to
        # absorb test-rig load and not flake.
        (autoanalyze_count,) = wait_until(
            fetch=lambda: autocommit_sess.execute(
                text("""
                select autoanalyze_count
                from pg_stat_user_tables where relname='_http_response';
            """)
            ).fetchone(),
            predicate=lambda result: result[0] > 0,
            timeout=30,
            sleep_interval=0.5,
            description="autoanalyze to fire on rest._http_response",
        )

        assert autoanalyze_count > 0, (
            "autoanalyze never fired on rest._http_response within 30s. "
            "Worker writes are not making pgstat threshold visible to the "
            "autovacuum launcher - the customer-facing symptom (silent bloat) "
            "would manifest in production."
        )

    finally:
        # Cleanup: restore defaults so we don't bleed into other tests.
        autocommit_sess.execute(
            text("""
            alter table rest._http_response reset (
                autovacuum_analyze_threshold,
                autovacuum_analyze_scale_factor,
                autovacuum_vacuum_threshold,
                autovacuum_vacuum_scale_factor
            );
        """)
        )
        autocommit_sess.execute(text("alter system reset autovacuum_naptime;"))
        autocommit_sess.execute(text("select pg_reload_conf();"))


def test_worker_reports_activity_in_pg_stat_activity(sess, autocommit_sess):
    """
    Check that the pg_rest worker must call pgstat_report_activity() so
    its row in pg_stat_activity has a valid state column.
    """

    autocommit_sess.execute(text("select rest.wait_until_running();"))

    # Wait for the worker to drain any leftover work from previous tests
    # and settle into idle. Polling makes this robust regardless of what
    # ran before.
    wait_for_worker_state(autocommit_sess, "idle")

    # Fire a slow request so the worker stays active long enough to observe.
    http_requests(
        sess,
        text("""
        select rest.http_get('http://localhost:8080/pathological?status=200&delay=2');
    """),
    )

    # Poll for 'active' for up to 5s. The slow request keeps the worker
    # busy for ~2s, so we have a wide observation window.
    saw_active = False
    try:
        wait_for_worker_state(autocommit_sess, "active")
        saw_active = True
    except AssertionError:
        pass

    assert saw_active, (
        "pg_rest worker state was never observed as 'active' during a slow "
        "request. Without pgstat_report_activity(STATE_RUNNING, ...) the "
        "state column stays NULL."
    )


def test_worker_idles_when_rest_schema_exists_without_extension(sess, autocommit_sess):
    """
    Check that when a schema named "rest" exists but the pg_rest tables don't
    (e.g. another extension installed into a schema named "rest"), the worker
    should treat the extension as not installed instead of crash looping
    """

    sess.execute(text("drop extension pg_rest cascade;"))
    # Differs from pg_net: the schema comes from `schema = rest` in pg_rest.control, so
    # CREATE EXTENSION creates it but DROP EXTENSION leaves it behind. It usually exists already.
    sess.execute(text("create schema if not exists rest;"))
    sess.commit()

    # restart the worker so it comes back up with a pending wake signal
    autocommit_sess.execute(text("select kill_worker();"))

    # wait for the worker to come back up (bgw_restart_time is 1 second)
    (pid,) = wait_until(
        fetch=lambda: autocommit_sess.execute(
            text(
                "select pid from pg_stat_activity where backend_type ilike '%pg_rest%';"
            )
        ).fetchone(),
        predicate=lambda result: result,
        timeout=5,
        sleep_interval=0.1,
        description="pg_rest worker to to be back up",
    )

    assert pid is not None, "pg_rest worker did not come back up after restart"

    # Watch for several seconds; a crash loop would respawn the worker with a
    # new pid. Poll for the bad condition (crash or restart) so we fail as
    # soon as it happens instead of only checking once at the end of a blind
    # sleep; a timeout here means the worker stayed up with the same pid.
    changed = False
    last_row = None
    try:
        last_row = wait_until(
            fetch=lambda: autocommit_sess.execute(
                text(
                    "select pid from pg_stat_activity where backend_type ilike '%pg_rest%';"
                )
            ).fetchone(),
            predicate=lambda result: result is None or result[0] != pid,
            timeout=3,
            sleep_interval=0.1,
            description="pg_rest worker to remain stable with the same pid",
        )
        changed = True
    except AssertionError:
        pass

    if changed:
        assert last_row is not None, (
            "pg_rest worker is down, it crashed after seeing the rest schema"
        )
        assert last_row[0] == pid, (
            "pg_rest worker restarted, it's crash looping on the rest schema"
        )

    sess.execute(text("drop schema rest;"))
    sess.commit()
