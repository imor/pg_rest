from sqlalchemy import text

from common import fetch_worker_pid, wait_until


def test_bad_response_rows_do_not_lose_the_bucket(sess, autocommit_sess):
    """
    If storing some responses fails (here: a user trigger on _http_response raising for every
    10th request), the rest of their bucket is still stored, the failed ones are dropped and
    their requests deleted, and the worker keeps running.
    """
    autocommit_sess.execute(
        text(
            """
        create or replace function public.fail_every_tenth() returns trigger language plpgsql as $$
        begin
          if new.id % 10 = 0 then
            raise exception 'simulated failure for request %', new.id;
          end if;
          return new;
        end
        $$;
        create trigger fail_every_tenth before insert on rest._http_response
          for each row execute function public.fail_every_tenth();
    """
        )
    )
    try:
        pid_before = fetch_worker_pid(autocommit_sess)

        (first_id,) = sess.execute(
            text(
                "select min(id) from (select rest.http_get('http://localhost:8080/') as id "
                "from generate_series(1, 300)) r"
            )
        ).one()
        sess.commit()

        def queue_length():
            return autocommit_sess.execute(
                text("select count(*) from rest.http_request_queue")
            ).scalar()

        wait_until(queue_length, lambda n: n == 0, description="queue to drain")

        ids = [
            r[0]
            for r in autocommit_sess.execute(
                text("select id from rest._http_response where id >= :first"),
                {"first": first_id},
            )
        ]
        expected = [i for i in range(first_id, first_id + 300) if i % 10 != 0]
        assert sorted(ids) == expected
        assert fetch_worker_pid(autocommit_sess) == pid_before
    finally:
        autocommit_sess.execute(
            text(
                "drop trigger if exists fail_every_tenth on rest._http_response; "
                "drop function if exists public.fail_every_tenth()"
            )
        )
