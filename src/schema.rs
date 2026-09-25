//! Tables, types and SQL functions of the `rest` schema. Modeled on pg_net's `net` schema.

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

-- Pending and in-flight requests. The background worker claims rows by setting `claimed_at`, and
-- deletes them in the same transaction that inserts their response.
-- API: Private
create table rest.http_request_queue(
    id bigserial primary key,
    method rest.http_method not null,
    url text not null,
    headers jsonb,
    body bytea,
    timeout_milliseconds int not null,
    claimed_at timestamptz
);

-- Keeps claiming fast when many rows are claimed but not yet retired.
create index http_request_queue_unclaimed_idx on rest.http_request_queue (id) where claimed_at is null;

-- Associates a response with a request
-- API: Private
create table rest._http_response(
    id bigint,
    status_code integer,
    content_type text,
    headers jsonb,
    content text,
    timed_out bool,
    error_msg text,
    created timestamptz not null default now()
);

create index on rest._http_response (created);

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
        from rest._http_response
        where id = request_id;

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
    from rest._http_response
    where id = request_id;

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
comment on function rest.wait_until_running() is 'waits until the worker is running';

grant usage on schema rest to PUBLIC;
grant all on all sequences in schema rest to PUBLIC;
grant select, insert, update, delete, truncate, references on all tables in schema rest to PUBLIC;
"#,
    name = "finalize",
    finalize
);
