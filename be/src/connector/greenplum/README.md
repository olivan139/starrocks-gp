# Greenplum (ADB) connector — BE data plane

gpfdist-only design: this BE **never connects to Greenplum**. The FE drives the
GP master over pgjdbc; GP *segments* connect to us. Contract:
`GREENPLUM_BE_CONTRACT.md`; wire spec: `ADB_GPFDIST_PROTOCOL_NOTES.md` (repo root).

## File map / stage map

| File | Stage | State |
|---|---|---|
| `gpfdist_protocol.{h,cpp}` | 2 | **done** — pure request/response/frame codecs, no I/O; tests in `be/test/connector/greenplum/` |
| `gpfdist_session.{h,cpp}` | 2/3 | **done** — session registry + POST seq state machine (PushSession), block handout (PullSession), tombstones/TTL; tests ditto |
| `greenplum_connector.{h,cpp}` | 1 | **wired** — registered connector + DataSource skeleton; `open/get_next` are TODO(stage2) with the intended flow spelled out inline |
| `greenplum_codec.{h,cpp}` | 1 | **TODO** — TEXT rows ⇄ Chunk; write the type-edge-case UTs first (numeric/date/timestamptz/escapes); wrap `formats/csv/` converters |
| `gpfdist_server.h` | 2 | **design sketch** — libevent listener; connection state machine documented in the header; keep ALL logic in protocol/session, only socket plumbing here |

Integration points already patched (all one-liners): `connector.h`(+enum),
`connector.cpp`, `connector_registry_init.cpp`, `exec_factory.cpp`
(`GREENPLUM_SCAN_NODE` → `ConnectorScanNode`), `exec_node.cpp`,
`connector/CMakeLists.txt`, `common/config.h` (`greenplum_gpfdist_*`),
`be/test/CMakeLists.txt`.

## Build & test

```bash
./thirdparty/build-thirdparty-darwin.sh -j14   # once
./build.sh --be
./run-be-ut.sh --gtest_filter='Gpfdist*'       # protocol + session suites
```

## The two bugs this structure is designed to prevent

1. **Duplicate data on network retry**: segments resend the same `X-GP-SEQ`;
   `PushSession::on_post` ACKs and drops duplicates. Never "fix" that.
2. **Duplicate data on read-back**: one block goes to exactly one segment
   (`PullSession::next_block`). Serving a block twice = silent duplication.
