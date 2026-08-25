---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Greenplum catalog

<Beta />

A Greenplum catalog is a kind of external catalog that enables you to query data in Greenplum (and
Greenplum-compatible engines such as Arenadata DB) without ingestion.

Reads are a **single-node** integration: StarRocks does not support creating materialized views on top of a
Greenplum catalog table, and every query against a Greenplum catalog table is executed as a single scan
against one coordinator connection rather than a parallel, distributed scan. Query execution relies on a
BE-side native (libpq) reader that streams query results through `COPY (<sql>) TO STDOUT`, so the BEs/CNs in
your StarRocks cluster must be able to open a network connection to the Greenplum coordinator.

Writes (`INSERT INTO`, see [Bulk write into a Greenplum table](#bulk-write-into-a-greenplum-table-insert-into))
go through a different, parallel path: the BEs expose a gpfdist-compatible endpoint that the Greenplum
segments pull rows from directly, so write throughput scales with the number of participating BEs and
Greenplum segments instead of going through a single coordinator connection.

## Prerequisites

- The FEs and BEs (or CNs) in your StarRocks cluster can reach the Greenplum coordinator host and port over
  the network.
- The Greenplum user configured in the catalog has read privileges on the schemas and tables you intend to
  query.

## Create a Greenplum catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the Greenplum catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### `comment`

The description of the Greenplum catalog. This parameter is optional.

#### `PROPERTIES`

The properties of the Greenplum catalog. `PROPERTIES` must include the following parameters:

| **Parameter**       | **Required** | **Default** | **Description**                                                                                     |
| -------------------- | ------------ | ----------- | ------------------------------------------------------------------------------------------------------ |
| type                  | Yes          |             | The type of the catalog. Set the value to `greenplum`.                                                  |
| greenplum.host        | Yes          |             | The hostname or IP address of the Greenplum coordinator.                                                |
| greenplum.port        | No           | `5432`      | The port on which the Greenplum coordinator accepts connections.                                        |
| greenplum.database    | Yes          |             | The name of the Greenplum database to connect to.                                                       |
| greenplum.user        | Yes          |             | The username used to connect to Greenplum.                                                              |
| greenplum.password    | Yes          |             | The password used to connect to Greenplum.                                                              |
| greenplum.transport   | No           | `copy`      | The data transport the BE uses to pull query results. Currently only `copy` (single-session libpq `COPY`) is supported. |
| greenplum.gpfdist.port | No          | `8907`      | The port on which the gpfdist listener on every BE/CN accepts connections from Greenplum segments during `INSERT INTO`. See [Bulk write into a Greenplum table](#bulk-write-into-a-greenplum-table-insert-into). |
| greenplum.gpfdist.scheme | No        | `gpfdist`   | The transport scheme used for the gpfdist URLs during `INSERT INTO`: `gpfdist` (plain) or `gpfdists` (mutual TLS). |
| greenplum.format.column_separator | No | `\t`      | The column separator used for the row format both in the Greenplum-side external-table DDL and by the BE sink encoder during `INSERT INTO`. |
| greenplum.format.null_marker | No    | `\N`        | The marker used to represent SQL `NULL` in the row format during `INSERT INTO`. |

### Examples

The following example creates a Greenplum catalog named `greenplum0`:

```SQL
CREATE EXTERNAL CATALOG greenplum0
PROPERTIES
(
    "type" = "greenplum",
    "greenplum.host" = "127.0.0.1",
    "greenplum.port" = "5432",
    "greenplum.database" = "gpdb",
    "greenplum.user" = "gpadmin",
    "greenplum.password" = "xxx"
);
```

## View Greenplum catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs
in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to
query the creation statement of an external catalog. The following example queries the creation statement of a
Greenplum catalog named `greenplum0`:

```SQL
SHOW CREATE CATALOG greenplum0;
```

## Drop a Greenplum catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop a Greenplum
catalog.

The following example drops a Greenplum catalog named `greenplum0`:

```SQL
DROP CATALOG greenplum0;
```

## Query a table in a Greenplum catalog

A Greenplum schema is exposed as a StarRocks database. That is, `<catalog_name>.<db_name>` maps to
`<greenplum_database>.<greenplum_schema>`, and `system` schemas (`pg_catalog`, `information_schema`, and the
`pg_toast`/`pg_temp` families) are not exposed.

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the Greenplum
   schemas available through the catalog:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the destination
   catalog in the current session:

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database in the
    current session:

    ```SQL
    USE <db_name>;
    ```

    Or, you can use [USE](../../sql-reference/sql-statements/Database/USE.md) to directly specify the active
    database in the destination catalog:

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) to query the
   destination table in the specified database:

   ```SQL
   SELECT * FROM <table_name>;
   ```

## Bulk write into a Greenplum table (`INSERT INTO`)

You can use [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) to bulk-append the
result of a StarRocks query into an existing Greenplum table.

### Syntax

```SQL
INSERT INTO <catalog_name>.<db_name>.<table_name>
SELECT ...
```

### Example

```SQL
INSERT INTO greenplum0.gp_schema.orders
SELECT order_id, customer_id, amount, order_date
FROM olap_db.orders_staging
WHERE order_date >= '2024-01-01';
```

### How it works

While the StarRocks fragments execute, each participating BE serves its share of the result rows over a
gpfdist-compatible endpoint. Concurrently, the FE drives a single Greenplum transaction that creates a
`READABLE EXTERNAL TABLE` pointing at those BE endpoints (one `LOCATION` URL per participating BE host),
runs `INSERT INTO <target> SELECT * FROM <external table>` so the Greenplum segments pull the rows directly
from the BEs, and drops the external table again — all inside one transaction. The properties
`greenplum.gpfdist.port`, `greenplum.gpfdist.scheme`, `greenplum.format.column_separator`, and
`greenplum.format.null_marker` (see [PROPERTIES](#properties)) control the gpfdist endpoint and the row
format used by both sides.

### Behavior notes

- **Append-only**: `INSERT INTO` always appends rows. `INSERT OVERWRITE` is not supported against a
  Greenplum catalog table.
- **Existing tables only**: The target table must already exist in Greenplum; `INSERT INTO` cannot create
  it.
- **Atomic on the Greenplum side**: The external-table creation, the `INSERT INTO ... SELECT`, and the
  external-table drop run inside a single Greenplum transaction. If the query fails, the row count reported
  by the BE sinks does not match the row count Greenplum inserted, or the transaction times out, the FE
  rolls the transaction back and no data is committed.
- **Requires the BE gpfdist listener**: Every BE/CN that can run a sink fragment must be reachable by the
  Greenplum segments on `greenplum.gpfdist.port`.
- **Requires `CREATE EXTERNAL TABLE` privilege**: The Greenplum user configured in the catalog (see
  `greenplum.user`) must be able to create and drop external tables in the target schema, in addition to the
  `INSERT` privilege on the target table.
- **Sink host count vs. segment count**: The number of distinct StarRocks BE hosts serving the write must
  not exceed the number of Greenplum primary segments; Greenplum rejects a readable external table with more
  `LOCATION` URLs than primary segments. Lower the sink parallelism if you hit this limit.

## Data type mapping

StarRocks maps Greenplum (PostgreSQL-wire-compatible) column types to StarRocks types as follows:

| **Greenplum type**                          | **StarRocks type** |
| -------------------------------------------- | ------------------- |
| BOOL                                         | BOOLEAN              |
| INT2                                         | SMALLINT             |
| INT4                                         | INT                  |
| INT8                                         | BIGINT               |
| FLOAT4                                       | FLOAT                |
| FLOAT8                                       | DOUBLE               |
| NUMERIC(p, s)                                | DECIMAL(p, s)         |
| CHAR(n)                                      | CHAR(n)               |
| VARCHAR(n)                                   | VARCHAR(n)            |
| TEXT                                         | VARCHAR               |
| BYTEA                                        | VARBINARY             |
| DATE                                         | DATE                  |
| TIME / TIME WITH TIME ZONE                   | TIME                  |
| TIMESTAMP / TIMESTAMP WITH TIME ZONE         | DATETIME              |
| JSON / JSONB                                 | JSON                  |
| UUID                                         | VARBINARY             |
| Other types                                  | UNKNOWN (unsupported) |

## Limitations

- **`INSERT INTO` is append-only**: `INSERT OVERWRITE`, `UPDATE`, `DELETE`, and `CREATE TABLE` against a
  Greenplum catalog table are not supported. See
  [Bulk write into a Greenplum table](#bulk-write-into-a-greenplum-table-insert-into).
- **No materialized views**: You cannot create a materialized view on top of a Greenplum catalog table.
- **Single-node scan**: Every read query against a Greenplum catalog table runs as a single scan issued from
  one BE/CN over one connection to the Greenplum coordinator; scans are not distributed across Greenplum
  segments. Plan accordingly for large result sets. `INSERT INTO` is not affected by this limitation: writes
  are parallelized across the participating BEs and Greenplum segments.
