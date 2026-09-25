I want you to write a replacement for pg_net. pg_net's source code is available at /Users/raminder.singh/supabase/c/pg_net. I want the design as follows:

* It should have two tables: one for requests and one for responses similar to pg_net.
* It should use a background worker to schedule sending the requests similar to pg_net.
* It should call CHECK_FOR_INTERRUPTS at regular intervals to let users interrupt of terminate the background worker.
* It should use tokio for async but be careful that only the main thread of the background worker calls any Postgres APIs, the non-main threads should purely be for sending http requests and receiving responses. Both kinds of threads might communicate via channels. Be careful and avoid introducing deadlocks or race conditions due to this design constraint.
* The background worker should report worker activity to pg_stat_activity as done for pg_net in this PR: https://github.com/supabase/pg_net/pull/255.
* The background worker should flush pgstat counters as done for pg_net in this PR: https://github.com/supabase/pg_net/pull/254.
* Do not introduce any GUC's yet, but try to keep anything which is a potential GUC in a constant in the code.
* Keep the interface similar to pg_net: users send requests and receive responses.
* The batching design is where pg_rest should differe substantially from pg_net for performance. pg_net opens a transaction, reads a batch of requests from the requests table, sends all the requests and waits for all of their responses before commiting their results. This causes the whole batch to be delayed if even a single response is slow. In pg_rest we want to claim a batch of requests by updating a column in the requests table and having no transaction open while the requests are in flight. When the responses arrive we want them to collect in a (configurable) bucket before commiting them in the responses table. The bucket size and the time to wait for it to fill should be configurable. The response bucket is committed when either the bucket fills or the time to fill the bucket is over. The request batch size can be (and usually will be) larger than the response bucket size, and more requests should be sent as soon as a response bucket is committed. This should act like the pipelining design of a CPU where more instructions enter the pipeline as soon as some of the older ones retire. This should lead to substtial perf gains over pg_net.
* It should use reqwest for sending http requests and receiving responses.
* It should use only the latest version of all the crates.
* It should mention the full three part version of the crates in Cargo.toml.

Make a plan for the implementation. If there are any clarifying questsion you want to ask me feel free. Also feel free to explore pg_net's code base.