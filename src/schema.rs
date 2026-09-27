//! Views, types and SQL functions of the `rest` schema.
//!
//! On this branch there are no tables: requests and responses live in shared memory (see
//! `mem`), and `rest.http_request_queue` / `rest._http_response` are views over functions that
//! read it. `DELETE` on the views is supported through INSTEAD OF triggers; `TRUNCATE` is not
//! (views can't be truncated).

use pgrx::prelude::*;

extension_sql!(
    r#"
-- Created here (rather than via `schema = rest` in the control file) so it belongs to the
-- extension and is dropped with it.
create schema if not exists rest;

create domain rest.http_method as text
check (
  value ilike 'get'
  or value ilike 'post'
  or value ilike 'delete'
);

-- Lifecycle states of a request (all protocols)
-- API: Public
create type rest.request_status as enum ('PENDING', 'SUCCESS', 'ERROR');

-- A response from an HTTP server
-- API: Public
create type rest.http_response as (
    status_code integer,
    headers jsonb,
    body text
);

-- State wrapper around responses
-- API: Public
create type rest.http_response_result as (
    status rest.request_status,
    message text,
    response rest.http_response
);

create function rest.check_worker_is_up() returns void as $$
begin
  if not exists (select pid from pg_stat_activity where backend_type ilike '%pg_rest%') then
    raise exception using
      message = 'the pg_rest background worker is not up'
    , detail  = 'the pg_rest background worker is down due to an internal error and cannot process requests'
    , hint    = 'make sure that you didn''t modify any of pg_rest internal tables';
  end if;
end
$$ language plpgsql;
comment on function rest.check_worker_is_up() is 'raises an exception if the pg_rest background worker is not up, otherwise it doesn''t return anything';
"#,
    name = "bootstrap",
    bootstrap
);

extension_sql!(
    r#"
-- Pending and in-flight requests, read from shared memory.
-- API: Private
create view rest.http_request_queue as select * from rest._requests();

-- Stored responses, read from shared memory.
-- API: Private
create view rest._http_response as select * from rest._responses();

create function rest._delete_request_row() returns trigger language plpgsql as $$
begin
  perform rest._delete_request(old.id);
  return old;
end
$$;
create trigger delete_request instead of delete on rest.http_request_queue
  for each row execute function rest._delete_request_row();

create function rest._delete_response_row() returns trigger language plpgsql as $$
begin
  perform rest._delete_response(old.id);
  return old;
end
$$;
create trigger delete_response instead of delete on rest._http_response
  for each row execute function rest._delete_response_row();

-- Blocks until an http_request is complete
-- API: Private
create function rest._await_response(
    request_id bigint
)
    returns bool
    language plpgsql
as $$
declare
    rec rest._http_response;
begin
    while rec is null loop
        select *
        into rec
        from rest._response(request_id);

        if rec is null then
            -- Wait 50 ms before checking again
            perform pg_sleep(0.05);
        end if;
    end loop;

    return true;
end;
$$;

-- Collect respones of an http request
-- API: Private
create function rest._http_collect_response(
    -- request_id reference
    request_id bigint,
    -- when `true`, return immediately. when `false` wait for the request to complete before returning
    async bool default true
)
    -- http response composite wrapped in a result type
    returns rest.http_response_result
    language plpgsql
as $$
declare
    rec rest._http_response;
begin

    if not async then
        perform rest._await_response(request_id);
    end if;

    select *
    into rec
    from rest._response(request_id);

    if rec is null or rec.error_msg is not null then
        -- The request is either still processing or the request_id provided does not exist

        -- TODO: request in progress is indistinguishable from request that doesn't exist

        -- No request matching request_id found
        return (
            'ERROR',
            coalesce(rec.error_msg, 'request matching request_id not found'),
            null
        )::rest.http_response_result;

    end if;

    -- Return a valid, populated http_response_result
    return (
        'SUCCESS',
        'ok',
        (
            rec.status_code,
            rec.headers,
            rec.content
        )::rest.http_response
    )::rest.http_response_result;
end;
$$;

comment on function rest.wait_until_running() is 'waits until the worker is running';

grant usage on schema rest to PUBLIC;
grant select, delete on rest.http_request_queue, rest._http_response to PUBLIC;

-- Shared memory outlives the extension: start from a clean slate when it is (re)created.
select rest._clear();
"#,
    name = "finalize",
    finalize
);
