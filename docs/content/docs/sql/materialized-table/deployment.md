---
title: Deployment
weight: 3
type: docs
aliases:
- /dev/table/materialized-table/deployment.html
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

# Introduction

Creating and operating materialized tables involves multiple components' collaborative work. This document will systematically explain the complete deployment solution for Materialized Tables, covering architectural overview, environment preparation, deployment procedures, and operational practices.

# Architecture Introduction

- **Client**: Could be any client that can interact with [Flink SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}}), such as [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}), [Flink JDBC Driver]({{< ref "docs/sql/interfaces/jdbc-driver" >}}) and so on.
- **Flink SQL Gateway**: Supports creating, altering, and dropping Materialized table. It also serves as an embedded workflow scheduler to periodically refresh full mode Materialized Table.
- **Flink Cluster**: The pipeline for refreshing Materialized Table will run on the Flink cluster.
- **Catalog**: Manages the creation, retrieval, modification, and deletion of the metadata of Materialized Table.
- **Catalog Store**: Supports catalog property persistence to automatically initialize catalogs for retrieving metadata in Materialized Table related operations.

{{< img src="/fig/materialized-table-architecture.svg" alt="Illustration of Flink Materialized Table Architecture" width="85%" >}}

# Deployment Preparation

## Flink Cluster Setup

Materialized Table refresh jobs currently support execution in these cluster environments:
* [Standalone clusters]({{<ref "docs/deployment/resource-providers/standalone/overview">}})
* [YARN clusters]({{<ref "docs/deployment/resource-providers/yarn" >}})
* [Kubernetes clusters]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}})

## Flink SQL Gateway Deployment

Materialized Tables in full refresh mode must be created through SQL Gateway, which schedules their refresh workflows. SQL Gateway requires specific configurations for metadata persistence and job scheduling. Materialized Tables in continuous refresh mode can also be created from Table API programs (see [Table API Programs](#table-api-programs)).

### Configure Catalog Store

Add catalog store configurations in `config.yaml` to persist catalog properties. 

```yaml
table:
  catalog-store:
    kind: file
    file:
      path: {path_to_catalog_store} # Replace with the actual path
```
Refer to [Catalog Store]({{<ref "docs/sql/catalogs">}}#catalog-store) for details.

### Configure Workflow Scheduler Plugin

Add workflow scheduler configurations in `config.yaml` for periodic refresh job scheduling. Currently, only the `embedded` scheduler is supported:

```yaml
workflow-scheduler:
  type: embedded
```

### Start SQL Gateway

Start the SQL Gateway using:

```shell
./sql-gateway.sh start
```

<span class="label label-danger">Note</span>
The Catalog must support creating materialized tables, which is currently only supported by [Paimon Catalog](https://paimon.apache.org/docs/master/concepts/table-types/#materialized-table).

# Operation Guide

## Connecting to SQL Gateway

Example using SQL Client:

```shell
./sql-client.sh gateway --endpoint {gateway_endpoint}:{gateway_port}
```

## Creating Materialized Tables

### Refresh Jobs Running on Standalone Cluster

```sql
Flink SQL> SET 'execution.target' = 'remote';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```

### Refresh Jobs Running in Session Mode

For session modes, pre-create session cluster as documented in  [yarn-session]({{< ref "docs/deployment/resource-providers/yarn" >}}#starting-a-flink-session-on-yarn) or [kubernetes-session]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}}#starting-a-flink-session-on-kubernetes)

**Kubernetes session mode:**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-session-1';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `kubernetes-session` and specify a valid `kubernetes.cluster-id` corresponding to an existing Kubernetes session cluster.

**YARN session mode:**

```sql
Flink SQL> SET 'execution.target' = 'yarn-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'yarn.application.id' = 'application-xxxx';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `yarn-session` and specify a valid `yarn.application.id` corresponding to an existing YARN session cluster.

### Refresh Jobs Running in Application Mode

**Kubernetes application mode:**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-application';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-application-1';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Set `execution.target` to `kubernetes-application`. The `kubernetes.cluster-id` is optional; if not set, it will be automatically generated.

**YARN application mode:**

```sql
Flink SQL> SET 'execution.target' = 'yarn-application';
[INFO] Execute statement succeeded.

Flink SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ... ;
[INFO] Execute statement succeeded.
```
Only set `execution.target` to yarn-application. The `yarn.application.id` doesn’t need to be set; it will be automatically generated during submission.

## Maintenance Operations

Cluster information (e.g., `execution.target` or `kubernetes.cluster-id`) is persisted in the catalog. Suspending or dropping a Materialized Table uses the recorded `execution.target` and cluster id, so they do not need to be set. The session must still be able to reach the cluster, for example through `rest.address` for `remote` or `kubernetes.namespace` for Kubernetes. Resuming a Materialized Table in continuous refresh mode, or altering an activated one, submits a new refresh job to the session's `execution.target` and cluster id, so set them first, as when creating the Materialized Table.

### Suspend Refresh Job

```sql
-- Suspend the MATERIALIZED TABLE refresh job
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table SUSPEND;
[INFO] Execute statement succeeded.
```

### Resume Refresh Job

```sql
-- Resume the MATERIALIZED TABLE refresh job
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table RESUME;
[INFO] Execute statement succeeded.
```

### Modify Query Definition

```sql
-- Modify the MATERIALIZED TABLE query definition
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table
> AS SELECT
> ... ;
[INFO] Execute statement succeeded.
```

# Table API Programs

A Table API program can create materialized tables in continuous refresh mode without the SQL
Gateway (see [Table API]({{< ref "docs/sql/materialized-table/table-api" >}})). It submits the
refresh job to a session cluster, with the execution target `remote`, `yarn-session` or
`kubernetes-session`. The refresh job runs independently of the program: it keeps running after
the program ends, and the materialized table can later be suspended, resumed or dropped from any
other program or from the SQL Gateway. For `remote`, the controlling program must configure the
cluster's `rest.address`. Restarting the refresh job from a Table API program, by resuming the
materialized table or by altering or redeploying it while it is activated, compares the program's
`execution.target` and, for YARN and Kubernetes, its cluster id with the recorded ones, and fails
if they differ. For `remote` only the target is compared, a program pointed  at another standalone
cluster resumes a suspended table there. The same applies to the same Kubernetes cluster id in
another namespace.

## Programs Deployed in Application Mode or Through the Web UI

Set the execution target and cluster id of a session cluster on the program's `TableConfig`, with
`TableConfig#set`, SQL `SET` or `EnvironmentSettings.Builder#withConfiguration`:

```java
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
tEnv.getConfig().set("execution.target", "kubernetes-session");
tEnv.getConfig().set("kubernetes.cluster-id", "mt-session-cluster");
```

| Session cluster | Options to set on the `TableConfig` |
|---|---|
| Standalone | `execution.target: remote` and `rest.address` |
| YARN session | `execution.target: yarn-session` and `yarn.application.id` |
| Kubernetes session | `execution.target: kubernetes-session` and `kubernetes.cluster-id` |

If the `TableConfig` sets `execution.target`, refresh jobs and their control ignore the options of
the program's own configuration that select a cluster: `rest.address`, `jobmanager.rpc.address`,
`kubernetes.cluster-id`, `yarn.application.id` and the high-availability settings. All other
options are read from both configurations, for example `rest.port`, `kubernetes.namespace`, the
client SSL settings and the program's jars. Set them on the `TableConfig` if the session cluster
needs different values.

High-availability settings are not needed to reach a session cluster. If you set them on the
`TableConfig`, set all the ones the cluster needs there. If `execution.target` comes from the
program's own configuration and that configuration enables high availability with a
`high-availability.cluster-id`, as `flink run` against a YARN session sets it, a cluster id set on
the `TableConfig` must name the same cluster.

Application-mode targets such as `kubernetes-application` are rejected for Table API programs,
whichever configuration sets them.
Deploying a refresh job in its own application cluster requires the SQL Gateway.

## Session Clusters of the Flink Kubernetes Operator

A `FlinkDeployment` without a job is a
[session cluster](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/overview/#session-cluster-deployments).
The operator deploys it with Flink's native Kubernetes integration, so refresh jobs can use it as
a `kubernetes-session` target, with `kubernetes.cluster-id` set to the name of the
`FlinkDeployment` and `kubernetes.namespace` set to its namespace. The program needs permission to
read services in that namespace. Alternatively, use the `remote` target with `rest.address` set to
the cluster's REST service, `<name>-rest.<namespace>`.

Refresh jobs submitted this way are not `FlinkSessionJob` resources. The operator does not manage
them, so they are not part of its upgrade or savepoint handling. Manage them with materialized table
statements, such as `ALTER MATERIALIZED TABLE ... SUSPEND`, `ALTER MATERIALIZED TABLE ... RESUME`
and `DROP MATERIALIZED TABLE`.
