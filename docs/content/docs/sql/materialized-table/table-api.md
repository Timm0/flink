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
and `execute()` submits it. `execute(CreateMode.failIfExists())` submits it as
`CREATE MATERIALIZED TABLE ... AS <query>` instead (see
[Refusing an existing object](#refusing-an-existing-object)).

## Requirements

Unless the declaration sets a `connector` option with `option(...)` (see
[Full declaration](#full-declaration)), the catalog must provide the storage of the materialized
table, as catalogs built for materialized tables do (see
[Deployment]({{< ref "docs/sql/materialized-table/deployment" >}})).

`execute()` submits the refresh job by running an `INSERT INTO` statement, which needs a
session-style cluster deployment target (`execution.target`), for example `remote`,
`yarn-session` or `kubernetes-session`. A `yarn`-/`kubernetes`-prefixed target also needs its
cluster id (`yarn.application.id` or `kubernetes.cluster-id`). If `execution.target` is set on the
`TableConfig`, the cluster id must be set there too, otherwise it is read from the `TableConfig`
or the execution environment's configuration, and the `TableConfig` wins. Whenever `execute()`
would start a refresh job, an unset target, `local`, `embedded`, an application-mode target or a
missing cluster id fails it with a `ValidationException` before anything is written. A redeploy
restarts the refresh job only with the target and cluster id recorded in the catalog. A different
target or cluster id fails the same way and keeps the job running. The catalog records no address,
so for `remote` only the target is compared.
Re-executing a declaration whose materialized table has a running continuous refresh job
additionally needs `execution.checkpointing.savepoint-dir` set, to stop that job with a savepoint
(see [Redeploying](#redeploying)).

A program deployed in application mode or submitted through the web UI must set `execution.target` and the
cluster id of a session cluster (or `rest.address` for `remote`) on its `TableConfig`, with
`TableConfig#set`, SQL `SET` or `EnvironmentSettings.Builder#withConfiguration`. If a program's
`TableConfig` sets `execution.target`, the refresh jobs it submits and controls ignore the options
of its execution environment's configuration that select a cluster. All other options, such as
`rest.port`, `kubernetes.namespace` and the client SSL settings, are read from both configurations. See
[Table API programs]({{< ref "docs/sql/materialized-table/deployment" >}}#table-api-programs) for
the options of each session cluster and for high availability.

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
`WITH`, `startMode(...)` to `START_MODE`, `freshness(...)` to `FRESHNESS`, and
`continuousRefresh()`/`fullRefresh()` to `REFRESH_MODE`.

A start mode passed to `execute(StartMode)` or `execute(CreateMode, StartMode)` overrides the
descriptor's for that call only.

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

`execute()` uses `CreateMode.createOrAlter()`, the default, which is the equivalent of
`CREATE OR ALTER MATERIALIZED TABLE`:

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
table created in SQL. Re-executing also replaces the stored start mode.

Without a declared `Schema`, or with one that does not start with a physical column, re-executing
also keeps parts of the stored schema. The stored watermark stays as it is, even if the
declaration adds, changes or omits one, and so do stored computed columns, virtual metadata columns
and a primary key that the declaration omits. The [full declaration](#full-declaration) above has
this shape, because its schema starts with a computed column, so re-executing it with a different
watermark keeps the old one. Declaring a physical column first, like a SQL column list that starts
with a regular column, makes the watermark, the primary key and the computed and virtual metadata
columns follow the declaration.

`CreateMode.createOrAlter()` fails with a `ValidationException` if a regular table or a permanent
view exists at the path. Both create modes also fail if a temporary table or view exists at the
path, because that would shadow the materialized table.

## Refusing an existing object

`execute(CreateMode.failIfExists())` is the equivalent of `CREATE MATERIALIZED TABLE`. It creates
the materialized table and refuses to touch anything that already exists at the path. This is
useful for a first deployment, or for any program where silently altering an existing table would
be wrong:

```java
try {
    tEnv.from("events")
            .materializeInto("silver")
            .execute(CreateMode.failIfExists());
} catch (ValidationException e) {
    // e.g. an object already exists at the path; it was not changed
}
```

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

## Looking up a materialized table

`TableEnvironment#fromMaterializedTable(path)` returns a handle to an existing materialized table,
for example one declared by another program or in SQL:

```java
for (String name : tEnv.listMaterializedTables("my_catalog", "my_db")) {
    String path = ObjectIdentifier.of("my_catalog", "my_db", name).asSerializableString();
    MaterializedTable mt = tEnv.fromMaterializedTable(path);
    System.out.printf(
            "%s: %s, freshness %s, mode %s%n  %s%n",
            name,
            mt.getRefreshStatus(),
            mt.getFreshness(),
            mt.getRefreshMode(),
            mt.getOriginalQuery());
}
```

`fromMaterializedTable` fails with a `ValidationException` if nothing exists at the path or the
object is not a materialized table. Temporary tables or views at the same path also fail the
lookup.

## Explaining a materialized table

```java
tEnv.from("orders")
        .groupBy($("ds"), $("region"))
        .select($("ds"), $("region"), $("amount").sum().as("revenue"))
        .materializeInto("revenue")
        .printExplain();

tEnv.fromMaterializedTable("revenue").printExplain();
```

`MaterializedPipeline#explain()` shows the plan of what `execute()` would run, as
`EXPLAIN CREATE OR ALTER MATERIALIZED TABLE` does.

`MaterializedTable#explain()` shows the plan of an `INSERT INTO` the materialized table with its
query, as defined when the handle was created.

## Reading a materialized table

`toTable()` reads the storage of a materialized table, as `tEnv.from(path)` does with its
identifier. It works on any handle, whether from `execute()` or from
`fromMaterializedTable(path)`. Use it to query the materialized table, or to declare another
materialized table on top of it:

```java
MaterializedTable bronze = tEnv.from("events")
        .where($("amount").isGreater(0))
        .materializeInto("bronze")
        .execute();

MaterializedTable silver = bronze.toTable()
        .groupBy($("ds"), $("region"))
        .select($("ds"), $("region"), $("amount").sum().as("revenue"))
        .materializeInto("silver")
        .execute();

MaterializedTable gold = silver.toTable()
        .groupBy($("region"))
        .select($("region"), $("revenue").sum().as("revenue"))
        .materializeInto("gold")
        .execute();
```

The stored query of each layer reads the layer above it, so `silver` is refreshed from what
`bronze` has materialized. Using `toQuery()` instead (see [Deriving a definition](#deriving-a-definition))
would copy the query of `bronze` and define `silver` directly against `events`.

## Deriving a definition

`toQuery()` returns the query that defines a materialized table, and `toDescriptor()` its
definition as a `MaterializedTableDescriptor`. Together they re-declare a materialized table with a
modified query:

```java
silver.toQuery()
        .where($("revenue").isGreater(0))
        .materializeInto("silver", silver.toDescriptor())
        .execute();
```

If its continuous refresh job is running, this restarts it without state (see
[Redeploying](#redeploying)).

`toQuery()` plans the stored query text. Every object the query references must be resolvable in
the current environment under the name it had when the materialized table was declared, including
the name of its catalog. Otherwise `toQuery()` fails with a `ValidationException`.

`toDescriptor()` describes the same definition as
[`SHOW CREATE MATERIALIZED TABLE`]({{< ref "docs/sql/reference/utility/show" >}}). Note that
`toDescriptor()` returns values as stored in the catalog, for example options, normalized refresh
strategy.

## Managing a materialized table's lifecycle

```java
MaterializedTable silver = tEnv.fromMaterializedTable("silver");

silver.suspend();
silver.resume(Map.of("sink.parallelism", "2"));
```

`suspend()` mirrors
[`ALTER MATERIALIZED TABLE ... SUSPEND`]({{< ref "docs/sql/materialized-table/statements" >}}#suspend).
It stops the continuous refresh job with a savepoint, which needs
`execution.checkpointing.savepoint-dir`, and records the materialized table as `SUSPENDED`.

`resume()` and `resume(options)` mirror
[`ALTER MATERIALIZED TABLE ... RESUME`]({{< ref "docs/sql/materialized-table/statements" >}}#resume).
They start a refresh job that restores the savepoint taken by `suspend()`. Redeploying or altering
the materialized table while it is suspended clears that savepoint (see [Redeploying](#redeploying)),
and the refresh job then starts without state. The options of `resume(options)` apply to that
refresh job only and are not stored. `resume()` returns once the refresh job is submitted and the
materialized table is recorded as `ACTIVATED`, not when the job is running. If the refresh job
cannot be started, the materialized table keeps its refresh status. On an `ACTIVATED` materialized
table whose refresh job has already ended, for example because it was cancelled outside the
materialized table, `resume()` starts a new refresh job without state. This works only while the
cluster still knows the ended job, otherwise `resume()` fails with a `TableException`. If
`execution.state-recovery.path` is set on the `TableConfig`, a refresh job that would start without
state restores that savepoint instead.

Like a redeploy, `resume()` starts the refresh job only with the target and cluster id recorded in
the catalog (see [Requirements](#requirements)). A different target or cluster id fails with a
`ValidationException` before anything is started, and the materialized table stays as it is.

The following operations need the SQL Gateway and fail in a `TableEnvironment`
- suspending, resuming or dropping a materialized table in full refresh mode,
- resuming a materialized table whose refresh job runs in its own application cluster.

Suspending a materialized table whose refresh job runs in its own application cluster works, and
ends that application cluster.

## Dropping a materialized table

```java
boolean dropped = tEnv.dropMaterializedTable("revenue");
```

`dropMaterializedTable(path, ignoreIfNotExists)` mirrors
[`DROP MATERIALIZED TABLE`]({{< ref "docs/sql/materialized-table/statements" >}}#drop-materialized-table),
except that a permanent table or view at the path counts as absent (see below).
If the continuous refresh job is running, it is cancelled before the materialized table is removed
from the catalog. A materialized table whose refresh status is `INITIALIZING` cannot be dropped.

`dropMaterializedTable(path)` is `dropMaterializedTable(path, true)`. It returns `true` if a
materialized table was found at the path and the drop was executed, and `false` if none exists at
the path. A permanent table or view at the path counts as absent. It is not dropped, and the call
returns `false`, whereas `DROP MATERIALIZED TABLE IF EXISTS`
fails in that case. With `ignoreIfNotExists = false`, a missing materialized table, or a table or
view at its path, fails with a `ValidationException`.

The method only drops permanent objects. If a temporary table or view shadows the materialized
table, the call fails with a `ValidationException`, also with `ignoreIfNotExists = true`. Drop the
temporary object first with `dropTemporaryTable` or `dropTemporaryView`.

A handle drops its materialized table with `drop()` or `drop(ignoreIfNotExists)`:

```java
MaterializedTable revenue = tEnv.fromMaterializedTable("revenue");
boolean dropped = revenue.drop();
```

`drop()` and `drop(ignoreIfNotExists)` behave like `dropMaterializedTable(path)` and
`dropMaterializedTable(path, ignoreIfNotExists)` for the handle's identifier. So `drop()` is
`drop(true)`. It returns `false` if the materialized table no longer exists, and `drop(false)`
fails with a `ValidationException` instead.
