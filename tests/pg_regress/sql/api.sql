-- SQL surface of pg_rest. Enqueueing happens inside rolled-back transactions so that the
-- background worker never sees these requests and the output stays deterministic.

select p.proname, pg_get_function_identity_arguments(p.oid) as args, pg_get_function_result(p.oid) as result
from pg_proc p join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'rest'
order by p.proname;

select c.relname, c.relpersistence
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'rest' and c.relkind = 'r'
order by c.relname;

begin;
select rest.http_get('http://localhost:8080/anything', params := '{"hello": "world", "a b": "c&d"}') > 0 as enqueued;
select rest.http_post('http://localhost:8080/post', '{"hello": "world"}') > 0 as enqueued;
select rest.http_post('http://localhost:8080/post', headers := '{"X-Test": "1"}') > 0 as enqueued;
select rest.http_delete('http://localhost:8080/delete', timeout_milliseconds := 100) > 0 as enqueued;
select method, url, headers, convert_from(body, 'UTF8') as body, timeout_milliseconds, claimed_at
from rest.http_request_queue
order by id;
rollback;

-- invalid requests are rejected when they are enqueued
\set VERBOSITY terse
select rest.http_get('/malformed_url');
select rest.http_get('localhost:8080');
select rest.http_get(null);
select rest.http_post('http://localhost:8080/post', headers := '{"Content-Type": "text/plain"}');

-- collecting a response that doesn't exist
select * from rest._http_collect_response(-1);
