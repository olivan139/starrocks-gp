---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# Greenplum catalog

<Beta />

Greenplum Catalog 是一种 External Catalog。通过 Greenplum Catalog，您不需要执行数据导入就可以直接查询
Greenplum（以及与 Greenplum 兼容的引擎，例如 Arenadata DB）里的数据。

当前读取是一个**单节点**的集成方案：StarRocks 不支持在 Greenplum Catalog 的表上创建物化视图，并且每一次针对
Greenplum Catalog 表的查询都是从单个 BE 节点通过一条连接向 Greenplum Coordinator 发起的单节点扫描，而不是分布式
并行扫描。查询执行依赖 BE 侧的原生（libpq）Reader，该 Reader 通过 `COPY (<sql>) TO STDOUT` 流式获取查询结果，
因此 StarRocks 集群中的 BE（或 CN）必须能够在网络上连接到 Greenplum Coordinator。

写入（`INSERT INTO`，参见[批量写入 Greenplum 表](#批量写入-greenplum-表-insert-into)）走的是另一条并行链路：
BE 会暴露一个兼容 gpfdist 协议的服务端点，由 Greenplum Segment 直接从该端点拉取数据，因此写入吞吐会随着参与
写入的 BE 数量和 Greenplum Segment 数量而扩展，而不是受限于单个 Coordinator 连接。

## 前提条件

- 确保 StarRocks 集群中的 FE 和 BE（或 CN）可以通过网络访问 Greenplum Coordinator 的主机和端口。
- 确保 Catalog 中配置的 Greenplum 用户对待查询的 Schema 和表拥有读权限。

## 创建 Greenplum Catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数说明

#### `catalog_name`

Greenplum Catalog 的名称。命名要求如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### `comment`

Greenplum Catalog 的描述。此参数为可选。

#### PROPERTIES

Greenplum Catalog 的属性，包含如下配置项：

| **参数**            | **是否必填** | **默认值** | **说明**                                                             |
| -------------------- | ------------ | ---------- | ---------------------------------------------------------------------- |
| type                  | 是           |            | 资源类型，固定取值为 `greenplum`。                                     |
| greenplum.host        | 是           |            | Greenplum Coordinator 的主机名或 IP 地址。                             |
| greenplum.port        | 否           | `5432`     | Greenplum Coordinator 接受连接的端口。                                 |
| greenplum.database    | 是           |            | 要连接的 Greenplum 数据库名称。                                        |
| greenplum.user        | 是           |            | 连接 Greenplum 使用的用户名。                                          |
| greenplum.password    | 是           |            | 连接 Greenplum 使用的用户密码。                                        |
| greenplum.transport   | 否           | `copy`     | BE 拉取查询结果所使用的数据传输方式，目前仅支持 `copy`（基于单会话 libpq `COPY`）。 |
| greenplum.gpfdist.port | 否          | `8907`     | `INSERT INTO` 写入时，每个 BE/CN 上的 gpfdist 监听端口，供 Greenplum Segment 连接。参见[批量写入 Greenplum 表](#批量写入-greenplum-表-insert-into)。 |
| greenplum.gpfdist.scheme | 否        | `gpfdist`  | `INSERT INTO` 写入时 gpfdist URL 使用的传输协议：`gpfdist`（明文）或 `gpfdists`（双向 TLS）。 |
| greenplum.format.column_separator | 否 | `\t`     | `INSERT INTO` 写入时，Greenplum 侧外部表 DDL 与 BE 侧编码器共用的行格式列分隔符。 |
| greenplum.format.null_marker | 否    | `\N`       | `INSERT INTO` 写入时，用于表示 SQL `NULL` 的行格式标记。 |

### 创建示例

以下示例创建了一个名为 `greenplum0` 的 Greenplum Catalog：

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

## 查看 Greenplum Catalog

您可以通过 [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) 查询当前所在
StarRocks 集群里所有 Catalog：

```SQL
SHOW CATALOGS;
```

您也可以通过 [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) 查询
某个 External Catalog 的创建语句。例如，通过如下命令查询 Greenplum Catalog `greenplum0` 的创建语句：

```SQL
SHOW CREATE CATALOG greenplum0;
```

## 删除 Greenplum Catalog

您可以通过 [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) 删除一个 Greenplum
Catalog。

例如，通过如下命令删除 Greenplum Catalog `greenplum0`：

```SQL
DROP CATALOG greenplum0;
```

## 查询 Greenplum Catalog 中的表数据

一个 Greenplum Schema 会被映射为一个 StarRocks 数据库，即 `<catalog_name>.<db_name>` 对应
`<greenplum_database>.<greenplum_schema>`；`pg_catalog`、`information_schema` 以及 `pg_toast`/`pg_temp`
系列的系统 Schema 不会暴露给 StarRocks。

1. 通过 [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) 查看该 Catalog 下
   可用的 Greenplum Schema：

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 通过 [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) 切换当前会话生效的
   Catalog：

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    再通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 指定当前会话生效的数据库：

    ```SQL
    USE <db_name>;
    ```

    或者，也可以通过 [USE](../../sql-reference/sql-statements/Database/USE.md) 直接将会话切换到目标
    Catalog 下的指定数据库：

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 通过 [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) 查询目标数据库
   中的目标表：

   ```SQL
   SELECT * FROM <table_name>;
   ```

## 批量写入 Greenplum 表（`INSERT INTO`）

您可以使用 [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) 将 StarRocks 查询结果
批量追加写入 Greenplum 中已存在的表。

### 语法

```SQL
INSERT INTO <catalog_name>.<db_name>.<table_name>
SELECT ...
```

### 示例

```SQL
INSERT INTO greenplum0.gp_schema.orders
SELECT order_id, customer_id, amount, order_date
FROM olap_db.orders_staging
WHERE order_date >= '2024-01-01';
```

### 工作原理

在 StarRocks 各 Fragment 执行的同时，每个参与写入的 BE 会通过一个兼容 gpfdist 协议的端点提供其负责的结果行。
与此同时，FE 会在 Greenplum 侧驱动一个单独的事务：创建一张指向这些 BE 端点的 `READABLE EXTERNAL TABLE`
（每个参与写入的 BE 主机对应一条 `LOCATION` URL），执行
`INSERT INTO <目标表> SELECT * FROM <外部表>` 使 Greenplum Segment 直接从各 BE 拉取数据，最后删除该外部表——
以上操作都在同一个事务内完成。`greenplum.gpfdist.port`、`greenplum.gpfdist.scheme`、
`greenplum.format.column_separator` 和 `greenplum.format.null_marker`（参见 [PROPERTIES](#properties)）用于
控制 gpfdist 端点以及双方共用的行格式。

### 行为说明

- **仅支持追加写入**：`INSERT INTO` 总是以追加方式写入数据。不支持对 Greenplum Catalog 表执行
  `INSERT OVERWRITE`。
- **仅支持已存在的表**：目标表必须已经在 Greenplum 中存在，`INSERT INTO` 不会自动创建目标表。
- **在 Greenplum 侧是原子的**：外部表的创建、`INSERT INTO ... SELECT`、以及外部表的删除都运行在同一个
  Greenplum 事务内。如果查询失败、BE Sink 上报的行数与 Greenplum 实际插入的行数不一致，或者事务超时，FE 会
  回滚该事务，不会提交任何数据。
- **依赖 BE 的 gpfdist 监听服务**：每一个可能承担写入 Sink Fragment 的 BE/CN，都必须能够在
  `greenplum.gpfdist.port` 端口上被 Greenplum Segment 访问到。
- **需要 `CREATE EXTERNAL TABLE` 权限**：Catalog 中配置的 Greenplum 用户（即 `greenplum.user`）除了需要对
  目标表拥有 `INSERT` 权限外，还必须能够在目标 Schema 下创建和删除外部表。
- **写入端 BE 数量不能超过 Segment 数量**：参与写入的 StarRocks BE 主机数量不能超过 Greenplum 主 Segment
  （Primary Segment）的数量；否则 Greenplum 会拒绝 `LOCATION` URL 数量超过主 Segment 数量的可读外部表。如果
  触发该限制，请降低写入的并行度。

## 数据类型映射

StarRocks 将 Greenplum（与 PostgreSQL 线协议兼容）的列类型按如下方式映射为 StarRocks 类型：

| **Greenplum 类型**                           | **StarRocks 类型**   |
| --------------------------------------------- | --------------------- |
| BOOL                                          | BOOLEAN                |
| INT2                                          | SMALLINT               |
| INT4                                          | INT                    |
| INT8                                          | BIGINT                 |
| FLOAT4                                        | FLOAT                  |
| FLOAT8                                        | DOUBLE                 |
| NUMERIC(p, s)                                 | DECIMAL(p, s)           |
| CHAR(n)                                       | CHAR(n)                 |
| VARCHAR(n)                                    | VARCHAR(n)              |
| TEXT                                          | VARCHAR                 |
| BYTEA                                         | VARBINARY               |
| DATE                                          | DATE                    |
| TIME / TIME WITH TIME ZONE                    | TIME                    |
| TIMESTAMP / TIMESTAMP WITH TIME ZONE          | DATETIME                |
| JSON / JSONB                                  | JSON                    |
| UUID                                          | VARBINARY               |
| 其他类型                                       | UNKNOWN（不支持）        |

## 使用限制

- **`INSERT INTO` 仅支持追加写入**：不支持对 Greenplum Catalog 表执行 `INSERT OVERWRITE`、`UPDATE`、
  `DELETE` 或 `CREATE TABLE`。参见[批量写入 Greenplum 表](#批量写入-greenplum-表-insert-into)。
- **不支持物化视图**：不能在 Greenplum Catalog 的表上创建物化视图。
- **单节点扫描**：针对 Greenplum Catalog 表的每一次读查询都是由单个 BE（或 CN）通过一条连接向 Greenplum
  Coordinator 发起的单节点扫描，扫描不会分散到各个 Greenplum Segment 上并行执行。对于返回结果集较大的查询，
  请提前评估性能影响。该限制不影响 `INSERT INTO`：写入会在参与写入的 BE 与 Greenplum Segment 之间并行执行。
