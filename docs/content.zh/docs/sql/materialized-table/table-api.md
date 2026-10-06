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
[`CREATE OR ALTER MATERIALIZED TABLE ... AS <query>`]({{< ref "docs/sql/materialized-table/statements" >}}),
and `execute()` submits it.

## Requirements

`execute()` submits the refresh job by running an `INSERT INTO` statement, which needs a
session-style cluster deployment target (`execution.target`). For example `remote`,
`yarn-session` or `kubernetes-session`. An unset target, `local`, and any application-mode target
(one ending in `application`, or `embedded`) are rejected before the job is submitted, but a
running refresh job that `execute()` replaces is stopped first. A `yarn`-/`kubernetes`-prefixed
target additionally needs its cluster id configured (`yarn.application.id` or
`kubernetes.cluster-id`, respectively), or submitting the refresh job fails with an error whose
cause names the missing option.
Re-executing a declaration whose materialized table has a running continuous refresh job
additionally needs `execution.checkpointing.savepoint-dir` set, to stop that job with a savepoint
(see [Create modes](#create-modes)).

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
(`materialized-table.default-start-mode`) in the catalog.

## Declaring freshness

```java
tEnv.from("orders").materializeInto("orders_copy", Duration.ofMinutes(1)).execute();
```

The refresh strategy is automatic: a freshness below
`materialized-table.refresh-mode.freshness-threshold` runs a continuous
refresh job. A freshness at or above the threshold resolves to full refresh, which needs a
workflow scheduler, for example the SQL Gateway, and so fails on a plain `TableEnvironment` with a
`TableException` that names the threshold. That specific error only applies to a freshness the
engine could otherwise schedule. A freshness that cannot be expressed as a cron schedule (for
example 45 minutes) fails earlier instead, with a `ValidationException`, before the engine gets to
check for a workflow scheduler. Declare `continuousRefresh()` to keep a long freshness continuous.

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
        .execute(StartMode.of(StartMode.StartModeKind.FROM_NOW));
```

A declared `Schema` behaves like the schema part of the SQL statement:

- A declared column with the name of a query column overrides its type. The new type must be
  implicitly castable from the query's type, and the column keeps the query's position.
- Computed columns and virtual metadata columns may be added. Any declared column the query does
  not produce is placed before the query's columns, in declaration order.
- A watermark and a primary key may be declared. Primary key columns must be `NOT NULL` in the
  query. If the materialized table's storage accepts updates (an upsert sink), the primary key
  must also contain one of the query's upsert keys with `table.exec.sink.require-on-conflict` at
  its default (`true`), a mismatch is rejected when the refresh job is submitted.
- Every declared physical or persisted metadata column must be produced by the query.
- Indexes and immutable columns are not supported.

## Create modes

`execute()` is `execute(CreateMode.createOrAlter())`, the equivalent of `CREATE OR ALTER
MATERIALIZED TABLE`:

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
- It converts an existing regular table, if
  `table.materialized-table.conversion-from-table.enabled` is set in the cluster configuration.
  The flag is read from the session's root configuration only. Setting it later with
  `tEnv.getConfig().set(...)` has no effect (see
  [Converting a Table to a Materialized Table]({{< ref "docs/sql/materialized-table/statements" >}}#converting-a-table-to-a-materialized-table)).

Re-executing a changed declaration against an existing materialized table inherits the same
schema-evolution rules as SQL's
[`ALTER MATERIALIZED TABLE ... AS`]({{< ref "docs/sql/materialized-table/statements" >}}#as-select_statement-1).
Only appending nullable columns at the end of the schema is supported. An explicitly declared
refresh strategy (`continuousRefresh()`/`fullRefresh()`) that differs from the table's current
refresh mode is rejected with a `ValidationException`. The default `automatic()` strategy never
triggers that check.

If the continuous refresh job is running, the changes to the materialized table are computed and
validated before the job is stopped. A declaration rejected at that point by the rules above, or
because a declared `Schema` conflicts with the query, such as a persisted column the query does not
produce, fails with a `ValidationException` and leaves the refresh job running and the
materialized table unchanged. Otherwise the job is stopped with a savepoint and the changes are
written to the catalog. If that write fails, the materialized table is left `SUSPENDED` with its
previous definition, and `ALTER MATERIALIZED TABLE ... RESUME` restarts the refresh job from that
savepoint. If the new refresh job fails to start, for example because of the primary key mismatch
described [above](#full-declaration), the change is rolled back, the previous refresh job restarts
from the savepoint, and `execute()` fails with a `TableException`. If the previous refresh job
cannot be restarted either, for example because the session's execution target is not supported,
the materialized table is also left `SUSPENDED`, with its previous definition, and `execute()`
fails with a `TableException` caused by the error of that restart.
Re-executing a declaration on a suspended materialized table alters it, the table stays suspended
and its savepoint is cleared.

A changed freshness, comment or partition keys has no effect on an existing materialized table.

Without a declared `Schema`, or with one that does not start with a physical column, re-executing
also keeps parts of the stored schema. The stored watermark stays as it is, even if the
declaration adds, changes or omits one, and so do stored computed columns, virtual metadata columns
and a primary key that the declaration omits. The [full declaration](#full-declaration) above has
this shape, because its schema starts with a computed column, so re-executing it with a different
watermark keeps the old one. Declaring a physical column first, like a SQL column list that starts
with a regular column, makes the watermark, the primary key and the computed and virtual metadata
columns follow the declaration.

`execute(CreateMode.failIfExists())` is the equivalent of `CREATE MATERIALIZED TABLE`. It fails if
a catalog object exists at the path and never alters or converts.

Both modes fail if a temporary table or view exists at the path, because it would shadow the
materialized table.

## Start mode

A start mode describes one deployment, not the table, so it is not part of the descriptor. Pass it
to `execute(StartMode)` or `execute(CreateMode, StartMode)`. `StartMode` is the equivalent of
`START_MODE = ...` in the SQL statement. A call without a start mode uses
`materialized-table.default-start-mode`, like a statement without `START_MODE`, even if an earlier
call passed a different one.

The refresh job does not apply the start mode. Every refresh job that `execute()` starts,
whether the first one or a restart from re-executing a declaration, starts from the sources' own
startup options, for example `scan.startup.mode`, or the
[dynamic table options]({{< ref "docs/sql/reference/queries/hints" >}}#dynamic-table-options)
specified in the query.

## Explaining a declaration

`materializeInto(...).explain()` renders the plan of the refresh pipeline without writing to the
catalog or touching a running refresh job.

## The stored query

The refresh job re-runs the query as SQL text, which is stored as the materialized table's
query. The query must therefore be expressible in SQL:

- Functions must be registered in a catalog, not used inline.
- Tables created from a `TableDescriptor` (`tEnv.from(TableDescriptor)`) and tables backed by a
  `DataStream` are not supported.
- Some window operations, such as session group windows, are not supported.

Such queries fail at `materializeInto(...)` with a `ValidationException`, before anything is
written.

A materialized table created from SQL and later declared from the Table API (or the other way
round) always registers a change of its query, because the two APIs produce different query
texts.
