-- pg_rest version of pg_net's test/utils/loadtest.sql: the same `run` table and
-- `wait_for_many_gets` procedure, calling rest.* instead of net.*.
create table run (
  requests int,
  batch_size int,
  time_taken interval,
  request_successes bigint,
  request_failures bigint,
  last_failure_error text
);

create or replace procedure wait_for_many_gets(number_of_requests int default 10000, url text default 'http://localhost:8080') as $$
declare
  last_id bigint;
  first_time timestamptz;
  second_time timestamptz;

  request_successes bigint;
  request_failures bigint;
  last_failure_error text;
begin
  delete from rest._http_response;

  with do_requests as (
    select
      rest.http_get(url) as id
    from generate_series (1, number_of_requests) x
  )
  select id, clock_timestamp() into last_id, first_time from do_requests offset number_of_requests - 1;

  commit;

  -- pg_net commits responses batch by batch in id order, so its version waits for the last id.
  -- pg_rest can complete requests out of order, so wait for all of them (same 50 ms polling as
  -- net._await_response).
  while (select count(*) from rest._http_response) < number_of_requests loop
    perform pg_sleep(0.05);
  end loop;

  select clock_timestamp() into second_time;

  select
    count(*) filter (where error_msg is null),
    count(*) filter (where error_msg is not null),
    (select error_msg from rest._http_response where error_msg is not null order by id desc limit 1)
  into request_successes, request_failures, last_failure_error
  from rest._http_response;

  -- pg_rest has no batch size; batch_size is left null.
  insert into run values (
    number_of_requests, null, age(second_time, first_time),
    request_successes, request_failures, last_failure_error);
end;
$$ language plpgsql;
