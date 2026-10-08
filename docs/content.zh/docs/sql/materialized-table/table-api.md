---
title: Table API
weight: 5
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Materialized Tables in the Table API

A Table API program can declare the query of a `Table` as the definition of a materialized table.
`Table#materializeInto` builds the same declaration as
[`CREATE OR ALTER MATERIALIZED TABLE ... AS <query>`]({{< ref "docs/sql/materialized-table/statements" >}})
without a column list or any clause other than `FRESHNESS`, and `execute()` submits it.

## Requirements

A declaration has no `WITH` options, so the catalog must provide the storage of the materialized
table, as catalogs built for materialized tables do (see
[Deployment]({{< ref "docs/sql/materialized-table/deployment" >}})).

`execute()` submits the refresh job by running an `INSERT INTO` statement, which needs a
session-style cluster deployment target (`execution.target`), for example `remote`,
`yarn-session` or `kubernetes-session`. A `yarn`-/`kubernetes`-prefixed target also needs its
cluster id (`yarn.application.id` or `kubernetes.cluster-id`), set in the same configuration as
`execution.target`. Whenever `execute()` would start a refresh job, an unset target, `local`,
`embedded`, an application-mode target (one ending in `application`) or a missing cluster id
fails it with a `ValidationException` before anything is written; when redeploying, before the
running refresh job is stopped. A redeploy restarts the refresh job only with the target and
cluster id recorded in the catalog; a different target or cluster id fails the same way and keeps
the job running.
Re-executing a declaration whose materialized table has a running continuous refresh job
additionally needs `execution.checkpointing.savepoint-dir` set, to stop that job with a savepoint
(see [Redeploying](#redeploying)).

## Minimal declaration

```java
MaterializedTable revenue = tEnv.from("orders")
        .groupBy($("ds"), $("region"))
        .select($("ds"), $("region"), $("amount").sum().as("revenue"))
        .materializeInto("revenue")
        .execute();
```

The schema is derived from the query. The engine applies the default freshness
(`materialized-table.default-freshness.continuous`) and records the default start mode
(`materialized-table.default-start-mode`) in the catalog. The refresh job does not apply the start
mode. Every new refresh job that `execute()` starts, whether the first one or a restart from
re-executing a declaration, starts from the sources' own startup options, for example
`scan.startup.mode`, or the
[dynamic table options]({{< ref "docs/sql/reference/queries/hints" >}}#dynamic-table-options)
specified in the query; only the rollback restart described under [Redeploying](#redeploying)
restores a savepoint.

## Declaring freshness

```java
tEnv.from("orders").materializeInto("orders_copy", Duration.ofMinutes(1)).execute();
```

The freshness must be a positive whole number of seconds, at most `Integer.MAX_VALUE` seconds.
Any other freshness fails at `materializeInto(...)` with a `ValidationException`.

The refresh strategy is automatic: a freshness below
`materialized-table.refresh-mode.freshness-threshold` runs a continuous
refresh job. A freshness at or above the threshold resolves to full refresh, which needs a
workflow scheduler, for example the SQL Gateway, and so fails on a plain `TableEnvironment` with a
`TableException` stating that a workflow scheduler is required. That specific error only applies
to a freshness the engine could otherwise schedule. A freshness that cannot be expressed as a cron
schedule (for example 45 minutes) fails earlier instead, with a `ValidationException`, before the
engine gets to check for a workflow scheduler. Use a freshness below the threshold, or declare the
table in SQL with `REFRESH_MODE = CONTINUOUS`, to keep it continuous.

## Redeploying

`execute()` is the equivalent of `CREATE OR ALTER MATERIALIZED TABLE`:

- It creates the materialized table if nothing exists at the path.
- It alters an existing materialized table (see
  [OR ALTER]({{< ref "docs/sql/materialized-table/statements" >}}#or-alter)). If its continuous
  refresh job is running, the job is stopped with a savepoint
  (`execution.checkpointing.savepoint-dir` must be set) and a new refresh job is started that does
  not restore that savepoint, so streaming state is reset. This also happens if the declaration is
  unchanged. A suspended materialized table is altered and stays suspended, and its stored
  savepoint is cleared, so a later `ALTER MATERIALIZED TABLE ... RESUME` starts the refresh job
  without state. A materialized table in full refresh mode is altered without changing its refresh
  workflow.

Re-executing a changed declaration against an existing materialized table inherits the same
schema-evolution rules as SQL's
[`ALTER MATERIALIZED TABLE ... AS`]({{< ref "docs/sql/materialized-table/statements" >}}#as-select_statement-1).
Only appending nullable columns at the end of the schema is supported.

If the continuous refresh job is running, the changes to the materialized table are computed and
validated before the job is stopped. A declaration rejected at that point by the rules above fails
with a `ValidationException` and leaves the refresh job running and the materialized table
unchanged. Otherwise the job is stopped with a savepoint and the changes are written to the
catalog. If that write fails, the materialized table is left `SUSPENDED` with its previous
definition, and `ALTER MATERIALIZED TABLE ... RESUME` restarts the refresh job from that
savepoint. If the new refresh job fails to start, the change is rolled back, the previous refresh
job restarts from the savepoint, and `execute()` fails with a `TableException`. If the previous
refresh job cannot be restarted either, for example because the cluster cannot be reached, the
materialized table is also left `SUSPENDED`, with its previous definition, and `execute()` fails
with a `TableException` caused by the error of that restart.

A changed freshness has no effect on an existing materialized table.

Re-executing against a materialized table that has `WITH` options or a distribution, such as one
created in SQL, resets those options and drops the distribution, like a
`CREATE OR ALTER MATERIALIZED TABLE` statement without those clauses. Its table comment and
partition keys are kept, and so are its watermark, primary key, and computed and virtual metadata
columns. Re-executing also records the session's default start mode again, replacing a
`START_MODE` that a SQL statement declared.

`execute()` fails with a `ValidationException` if a regular table or a permanent view exists at
the path; the Table API does not convert regular tables. It also fails if a temporary table or
view exists at the path, because that would shadow the materialized table.

## The stored query

The refresh job re-runs the query as SQL text, which is stored as the materialized table's
query. The query must therefore be expressible in SQL:

- User-defined functions must be registered in a catalog, not used inline, unless a custom
  `SqlFactory` (`EnvironmentSettings.Builder#withSqlFactory`) serializes them.
- Temporary functions, tables and views are not supported, because the refresh job may run the
  text in another session.
- Tables created from a `TableDescriptor` (`tEnv.from(TableDescriptor)`) and tables backed by a
  `DataStream` are not supported.
- Some window operations, such as session group windows, are not supported.

Such queries fail at `materializeInto(...)` with a `ValidationException`, before anything is
written.

Parts of the query given as SQL text (`TableEnvironment#sqlQuery`, `Expressions#callSql`) are not
checked for temporary objects, as with `CREATE MATERIALIZED TABLE` in SQL.

A materialized table created from SQL and later declared from the Table API (or the other way
round) always registers a change of its query, because the two APIs produce different query
texts.
