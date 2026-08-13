# Greenplum Connector — FE ⇄ BE Contract (Milestone 1)

Hand-off document for the C++ BE implementation. Describes exactly what the FE (Java, already implemented) sends
to the BE, and what the BE-side scan is expected to do with it. Companion docs: `ADB_CONNECTOR_DESIGN.MD` (§15),
`ADB_CONNECTOR_IMPLEMENTATION_GUIDE.md`, `ADB_GPFDIST_PROTOCOL_NOTES.md`.

## What the FE produces

For `SELECT ... FROM <greenplum_catalog>.<schema>.<table> ...` the FE plans a **single-node, UNPARTITIONED
fragment** containing a `GREENPLUM_SCAN_NODE`, with an exchange above it. `getScanRangeLocations()` returns
null (same shape as the JDBC scan): the scan runs on one BE. Parallel scan ranges arrive in a later milestone
(`TGreenplumScanRange` does not exist yet — deliberately).

### 1. Connection info — `TGreenplumTable` (in `TTableDescriptor`, field 37)

```thrift
struct TGreenplumTable {           // gensrc/thrift/Descriptors.thrift
    1: optional string host
    2: optional string port        // always set; FE defaults it to "5432"
    3: optional string database
    4: optional string user
    5: optional string passwd
}
```

- Arrives in the query's `TDescriptorTable`; `TTableDescriptor.tableType == TTableType::GREENPLUM_TABLE`.
- Credentials are per-query plan payload. **BE must not persist or log them.**
- Connect with libpq: `host=<host> port=<port> dbname=<database> user=<user> password=<passwd>`.
  Recommended additional parameters: `connect_timeout`, and **always** `options='-c TimeZone=UTC'`
  (see type mapping §5 of the design doc — `timestamptz` correctness depends on it).

### 2. Query shape — `TGreenplumScanNode` (in `TPlanNode`, field 86)

```thrift
struct TGreenplumScanNode {        // gensrc/thrift/PlanNodes.thrift
  1: optional Types.TTupleId tuple_id
  2: optional string table_name    // fully qualified + quoted: "schema"."table"
  3: optional list<string> columns // each already double-quoted: "col"
  4: optional list<string> filters // postgres-dialect boolean exprs (already quoted/rewritten)
  5: optional i64 limit            // -1 = no limit
  6: optional string sql           // complete, ready-to-run SELECT — see below
  7: optional string transport     // "copy" in M1
}
```

`node_type == TPlanNodeType::GREENPLUM_SCAN_NODE`. `TPlanNode.connector_catalog_type` is set to `"greenplum"`.

**`sql` is the primary field.** It is the fully assembled statement:
`SELECT "a", "b" FROM "schema"."tbl" WHERE (<f1>) AND (<f2>) LIMIT n` — columns, filters and limit already
applied, identifiers double-quoted, string literals escaped by the FE. The intended M1 execution is:

```
COPY (<sql>) TO STDOUT (FORMAT csv, HEADER false, NULL '\N')   -- BE picks the exact COPY options
```

then decode CSV rows into the chunk according to the tuple descriptor (`tuple_id` → slots, in the same order
as `columns`). `columns`/`filters`/`limit` are provided redundantly for transports that prefer assembling
their own statement; if you use them instead of `sql`, note `filters` entries must be ANDed and each wrapped
in parentheses, and `limit == -1` means absent.

`transport` is `"copy"` for now — dispatch on it so gpfdist transports can be added without a thrift change.

### 3. Row count / result expectations

- When `columns` would be empty (e.g. `count(*)`), FE emits `columns = ["*"]` and `sql = SELECT * ...` —
  the BE only needs the row count in that case but will receive full rows; this matches JDBC scan behavior.
- Predicates not pushed down (non-translatable ones) remain in the BE plan as regular conjuncts on the scan
  node — the pipeline applies them; the BE Greenplum reader itself does not need to re-filter.

## What the BE needs to implement (M1)

1. `Connector`/`DataSourceProvider`/`DataSource` for `ConnectorType` "greenplum" — registered in
   `be/src/connector/connector_registry_init.cpp`; new module dir `be/src/connector/greenplum/` declared in
   `be/module_boundary_manifest.json`.
2. Dispatch: `TPlanNodeType::GREENPLUM_SCAN_NODE` → the new data source (grep how `JDBC_SCAN_NODE` routes into
   `ConnectorScanNode` / the connector framework and mirror).
3. libpq vendored in `thirdparty/` (links against existing openssl/krb5).
4. The COPY-CSV → `Chunk` decode (reuse `be/src/formats/csv/`); type mapping per `ADB_CONNECTOR_DESIGN.md` §5 —
   the FE resolver maps GP types to SR types (int2/4/8, float4/8, numeric→DECIMAL, varchar/text, bool, date,
   timestamp→DATETIME), so the CSV decode must parse into exactly the slot types of the tuple descriptor.
5. Cancellation: on fragment cancel, `PQcancel` the in-flight COPY.

## Invariants

- New thrift fields are all `optional`; never reuse ordinals (`TTableDescriptor:37`, `TPlanNode:86`,
  `TTableType::GREENPLUM_TABLE` appended last, `TPlanNodeType::GREENPLUM_SCAN_NODE` appended last).
- FE guarantees `sql` is valid PostgreSQL for GP6+ (double-quoted identifiers, no backticks, no Oracle syntax).
- Session-level requirements the BE must set on every connection: `TimeZone=UTC`.
- The BE must fail the query loudly on decode/type mismatch — never silently null out unparseable values.
