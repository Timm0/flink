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
without `START_MODE`, and `execute()` submits it.

## Requirements

Unless the declaration sets a `connector` option with `option(...)` (see
[Full declaration](#full-declaration)), the catalog must provide the storage of the materialized
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
Any other freshness fails at `materializeInto(...)`, or at
`MaterializedTableDescriptor.Builder#freshness(Duration)`, with a `ValidationException`.

The refresh strategy is automatic: a freshness below
`materialized-table.refresh-mode.freshness-threshold` runs a continuous
refresh job. A freshness at or above the threshold resolves to full refresh, which needs a
workflow scheduler, for example the SQL Gateway, and so fails on a plain `TableEnvironment` with a
`TableException` that names the freshness and the threshold option. That specific error only
applies to a freshness the engine could otherwise schedule. A freshness that cannot be expressed as
a cron schedule (for example 45 minutes) fails earlier instead, with a `ValidationException`, before
the engine gets to check for a workflow scheduler. Declare `continuousRefresh()` to keep a long
freshness continuous.

## Full declaration

```java
MaterializedTable enriched = tEnv.from("events")
        .select($("user_id"), $("amount"), $("ts"), $("ds"))
        .materializeInto("enriched_events", MaterializedTableDescriptor.newBuilder()
                .schema(Schema.newBuilder()
                        .columnByExpression("amount_cents", "amount * 100")
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build())
                .comment("events, kept fresh")
                .partitionedBy("ds")
                .option("format", "json")
                .freshness(Duration.ofMinutes(1))
                .continuousRefresh()
                .build())
        .execute();
```

The builder methods correspond to the clauses of the SQL statement: `schema(...)` to the column
list, `comment(...)` to `COMMENT`, `distributedBy...(...)` and `distributedInto(...)` to
`DISTRIBUTED BY`/`DISTRIBUTED INTO`, `partitionedBy(...)` to `PARTITIONED BY`, `option(...)` to
`WITH`, `freshness(...)` to `FRESHNESS`, and `continuousRefresh()`/`fullRefresh()` to
`REFRESH_MODE`.

A declared `Schema` behaves like the schema part of the SQL statement:

- A declared column with the name of a query column overrides its type. The new type must be
  implicitly castable from the query's type, and the column keeps the query's position.
- Computed columns and virtual metadata columns may be added. Any declared column the query does
  not produce is placed before the query's columns, in declaration order.
- A watermark and a primary key may be declared. Primary key columns must be `NOT NULL` in the
  query. If the materialized table's storage accepts updates (an upsert sink), the primary key
  must also contain one of the query's upsert keys with `table.exec.sink.require-on-conflict` at
  its default (`true`); a mismatch is rejected when the refresh job is submitted.
- Every declared physical or persisted metadata column must be produced by the query.
- Indexes and immutable columns are not supported.

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
Of the query's columns, only appending new ones at the end of the schema is supported: existing
ones cannot be dropped, and their types cannot change. Unless a declared `Schema` starts with a
physical column, the appended columns are stored as nullable. An explicitly declared
refresh strategy (`continuousRefresh()`/`fullRefresh()`) that differs from the table's current
refresh mode is rejected with a `ValidationException`. The default `automatic()` strategy never
triggers that check.

If the continuous refresh job is running, the changes to the materialized table are computed and
validated before the job is stopped. A declaration rejected at that point by the rules above, or
because a declared `Schema` conflicts with the query, such as a persisted column the query does not
produce, fails with a `ValidationException` and leaves the refresh job running and the materialized
table unchanged. Otherwise the job is stopped with a savepoint and the changes are written to the
catalog. If that write fails, the materialized table is left `SUSPENDED` with its previous
definition, and `ALTER MATERIALIZED TABLE ... RESUME` restarts the refresh job from that
savepoint. If the new refresh job fails to start, the change is rolled back, the previous refresh
job restarts from the savepoint, and `execute()` fails with a `TableException`. If the previous
refresh job cannot be restarted either, for example because the cluster cannot be reached, the
materialized table is also left `SUSPENDED`, with its previous definition, and `execute()` fails
with a `TableException` caused by the error of that restart.

A changed freshness, comment or partition keys has no effect on an existing materialized table.

The declared `WITH` options and distribution replace the stored ones, as in a
`CREATE OR ALTER MATERIALIZED TABLE` statement: a stored option the declaration omits is reset,
and a declaration without a distribution drops the stored one. This also applies to a materialized
table created in SQL. Re-executing also records the session's default start mode again, replacing
a `START_MODE` that a SQL statement declared.

Without a declared `Schema`, or with one that does not start with a physical column, re-executing
also keeps parts of the stored schema. The stored watermark stays as it is, even if the
declaration adds, changes or omits one, and so do stored computed columns, virtual metadata columns
and a primary key that the declaration omits. The [full declaration](#full-declaration) above has
this shape, because its schema starts with a computed column, so re-executing it with a different
watermark keeps the old one. Declaring a physical column first, like a SQL column list that starts
with a regular column, makes the watermark, the primary key and the computed and virtual metadata
columns follow the declaration.

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
