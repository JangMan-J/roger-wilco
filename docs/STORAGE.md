# Roger-Wilco Storage Architecture

This document maps the SQLite hot/cold storage engine as built in `src/sql/`, `src/cold.rs`, `src/coordinator.rs`, `src/daemon.rs`, `src/session.rs`, and `src/audit.rs`. Every box and arrow names an actual function, SQLite table, or on-disk file in the repository.

## 1. Processes and On-Disk Files

- **Coordinator (`coordinator::run` -> `CoordinatorStore::open`)**: resolves `--data <path>` via `sql::coordinator_root` to `<data>.slices/`, holds an exclusive OS lock on `<data>.slices/LOCK` via `durlog::open_exclusive_lock`, imports any pre-cutover JSONL `coordinator.log` read-only once via `sql::import::maybe_import_coordinator_log` (leaving `<data>` untouched on disk), and runs one dedicated group-commit `SliceWriter` (`writer_loop` / `run_batch`) per `slice-NN/hot.sqlite` (`WAL`, `synchronous=FULL`) alongside `slice-NN/cold/day-<bucket>.cold` (`ColdIndex`).
- **Host Daemon (`daemon::run` -> `DaemonStore::open`)**: holds `<daemon_data>.lock`, imports any pre-cutover JSONL `daemon.log` read-only once via `sql::import::maybe_import_daemon_log`, and keeps only active `pending_deliveries` and `unconfirmed_receipts` in `<daemon_data>.sqlite` (`WAL`, `synchronous=FULL`).
- **Sessions (`session::serve`, `mcp::run`, Optional gateway modules)**: keep append-only JSONL `.store` files (`DurLog::open`) and atomic `.store.cursor` files, enforcing Store-before-Ack.
- **Concurrent Verifiers (`audit::run` -> `sql::audit_ro::read_coordinator_records` / `read_daemon_records`)**: open `hot.sqlite` and `<daemon_data>.sqlite` with `SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX` inside `BEGIN DEFERRED` snapshots plus `ColdIndex::load`, allowing any number of concurrent read-only `roger audit` readers alongside live writers.

```mermaid
flowchart LR
    subgraph CoordProc["Coordinator (coordinator::run)"]
        CW["SliceWriter::writer_loop / run_batch"]
        CI["ColdIndex (ColdFilter per day)"]
        IMPC["sql::import::maybe_import_coordinator_log"]
    end

    subgraph CoordDisk["<data>.slices/ (sql::coordinator_root)"]
        CLOCK["LOCK (durlog::open_exclusive_lock)"]
        HOT[("slice-00/hot.sqlite (WAL, synchronous=FULL)\ntables: meta, live_sessions, live_claims,\ntimeline, streams, seen_ids, broadcasts,\nenvelopes, hot_bodies, positions, receipts")]
        COLD[("slice-00..NN/cold/day-<bucket>.cold\n(RWCOLD01 + SHA256 trailer)")]
        LEG_C[("coordinator.log\n(pre-cutover JSONL, read-only)")]
    end

    subgraph DaemonProc["Host Daemon (daemon::run)"]
        DS["DaemonStore (src/sql/daemon.rs)"]
        IMPD["sql::import::maybe_import_daemon_log"]
    end

    subgraph DaemonDisk["Host Daemon Files"]
        DLOCK["<daemon>.lock"]
        DDB[("<daemon>.sqlite (WAL, synchronous=FULL)\ntables: meta, pending_deliveries,\nunconfirmed_receipts")]
        LEG_D[("daemon.log\n(pre-cutover JSONL, read-only)")]
    end

    subgraph SessProc["Session (session::serve / mcp::run)"]
        SLOG["DurLog::open / DurLog::append"]
        SSTORE[("<address>.store + <address>.store.cursor")]
    end

    subgraph AuditProc["Concurrent Verifiers (audit::run x N)"]
        ARO["sql::audit_ro::open_ro\n(SQLITE_OPEN_READ_ONLY | NO_MUTEX)"]
    end

    LEG_C -. "one-time read-only import" .-> IMPC
    IMPC --> HOT
    CW -- "BEGIN IMMEDIATE ... COMMIT" --> HOT
    CW -- "ColdIndex::seal_day" --> COLD
    COLD -- "ColdIndex::read_body" --> CI

    CoordProc -- "TCP deliver_of / ack_reply" --> DaemonProc
    LEG_D -. "one-time read-only import" .-> IMPD
    IMPD --> DDB
    DS --> DDB

    DaemonProc -- "Unix / TCP deliver / ack" --> SessProc
    SLOG --> SSTORE

    HOT -. "concurrent read-only snapshot" .-> ARO
    COLD -. "verified SHA256 read" .-> ARO
    DDB -. "concurrent read-only snapshot" .-> ARO
    SSTORE -. "fs::read" .-> AuditProc
```

## 2. One Mail's Path (Send to Confirmed Receipt and Daemon Row Deletion)

Every Mail id is indexed under `seen_ids(mail_id PRIMARY KEY)` and `envelopes(mail_id UNIQUE)` and is never reused. `Accepted` is answered only after `SliceWriter::run_batch` commits durably (`synchronous=FULL`). On the recipient host, `DaemonStore::settle_ack` atomically deletes the `pending_deliveries` row and inserts an `unconfirmed_receipts` row; once the coordinator commits the reader's `positions` and `receipts` rows and answers `acked`, `DaemonStore::confirm_receipt` deletes the `unconfirmed_receipts` row.

```mermaid
sequenceDiagram
    participant Sender as Sender (session::send_mail)
    participant D1 as Sender Daemon (daemon::local_loop)
    participant Coord as Coordinator (coordinator::send)
    participant SW as SliceWriter (sql::coord::run_batch)
    participant D2 as Recipient Daemon (daemon::handle_deliver)
    participant DDB as DaemonStore (<daemon>.sqlite)
    participant Recip as Recipient (session::serve)
    participant Store as Session Store (DurLog::append)

    Sender->>D1: {"type":"send","mail","from","to","body","headers"}
    D1->>Coord: daemon::upstream_request -> send
    Coord->>SW: CoordinatorStore::send_directed
    Note over SW: Check seen_ids + broadcasts\nInsert envelopes + hot_bodies + positions + seen_ids\nCOMMIT (WAL, synchronous=FULL)
    SW-->>Coord: SendStatus::Committed { article_num }
    Coord-->>D1: coordinator::answer_then_push -> {"type":"accepted"}
    D1-->>Sender: ACCEPTED mail=<id>
    Coord->>D2: net::deliver_msg {"type":"deliver",...}
    D2->>DDB: DaemonStore::insert_deliver -> INSERT pending_deliveries
    D2->>Recip: daemon::delivery -> {"type":"deliver",...}
    Recip->>Store: DurLog::append + sync_all (record r-N)
    Recip->>D2: {"type":"ack","mail","record":"r-N"}
    D2->>DDB: DaemonStore::settle_ack\nDELETE pending_deliveries + INSERT unconfirmed_receipts
    D2-->>Recip: {"type":"acked","mail","record":"r-N"}
    D2->>Coord: net::ack_msg {"type":"ack","mail","session","record"}
    Coord->>SW: CoordinatorStore::record_ack\nINSERT receipts + sql::coord::advance_position(positions)
    SW-->>Coord: AckResult::Recorded
    Coord-->>D2: coordinator::ack_reply -> {"type":"acked","mail","session","record"}
    D2->>DDB: DaemonStore::confirm_receipt\nDELETE FROM unconfirmed_receipts
```

## 3. Body Lifecycle: Hot Tier (0-30 Days) -> Cold Sweep -> Late-Reader Cold Restore

Metadata (`envelopes`, `seen_ids`, `streams`, `positions`, `receipts`, `timeline`) stays in `hot.sqlite` indefinitely. Bodies start in `hot_bodies`. On the first commit after a day boundary (`day_bucket > last_swept_day`, unless `--no-cold-sweep` is set), `CoordinatorStore` spawns an off-send-path sweep that reads bodies older than `retain_ms` (default 30 days), seals them into `slice-NN/cold/day-<bucket>.cold` via `ColdIndex::seal_day` (`RWCOLD01` header, sorted JSON records with per-body SHA-256 `digest`, trailing whole-file `SHA256:` digest, atomic rename, read-only permissions, and an in-memory 6-probe `ColdFilter`), and then marks `envelopes.in_cold = 1` and deletes those rows from `hot_bodies`. When a late reader connects (`register` -> `CoordinatorStore::pending_for_session` or `hello` -> `pending_for_host`) with `article_num > positions.position` on a row where `in_cold = 1`, `ColdIndex::read_body` checks the day's `ColdFilter` in memory and, on a hit, verifies and restores the body from `day-<bucket>.cold`.

```mermaid
flowchart TD
    A["CoordinatorStore::send_directed / send_broadcast\nInsert envelope (in_cold = 0) + hot_bodies(mail_id, body)"] --> B{"Commit crosses day_bucket\nand sweep_enabled?"}
    B -- "No (or --no-cold-sweep)" --> H["Body stays in hot_bodies\nServed directly from hot.sqlite"]
    B -- "Yes (off-send-path background thread)" --> C["Select envelopes + hot_bodies\nwhere in_cold = 0 and accepted_at_ms <= now_ms - retain_ms"]
    C --> D["ColdIndex::seal_day\nWrite day-<bucket>.cold.tmp (RWCOLD01 + records + SHA256 trailer)\nsync_all -> readonly -> rename -> durlog::sync_dir\nPopulate in-memory ColdFilter"]
    D --> E["SliceWriter::run_batch\nUPDATE envelopes SET in_cold = 1\nDELETE FROM hot_bodies WHERE mail_id = ?"]
    E --> F["Late Reader Register / Reconnect\nCoordinatorStore::pending_for_session / pending_for_host\n(envelopes.article_num > positions.position AND in_cold = 1)"]
    F --> G{"ColdFilter::may_contain(mail_id)?"}
    G -- "Hit" --> R["ColdIndex::read_body -> cold::read_cold_file\nVerify SHA256 trailer + record digest\nPush identical net::deliver_msg to Host Daemon"]
```

## 4. Named Group Post Fanout and Per-Reader Positions

A Group post (`session::send_group_mail` / `roger adapter --groups` / `roger mcp --groups` / `roger-mcp-gateway` / `roger-ws-gateway`) commits **one** `envelopes` row (`stream = "g:<group>"`, monotonic `article_num` from `streams.next_article`, `kind = "group"`) and **one** `hot_bodies` row via `CoordinatorStore::send_group`, then queries `sql::coord::subscribers_of` (`positions WHERE stream = ? AND subscribed = 1`) and pushes `coordinator::deliver_of` to each subscribed reader's connected `Host Daemon`. Each reader writes its local `.store` once (`DurLog::append`) and sends `ack`; `CoordinatorStore::record_ack` inserts `receipts(mail_id, reader, ...)` and calls `sql::coord::advance_position` to advance that reader's single `positions(stream, reader)` row across contiguous acked article numbers.

```mermaid
flowchart LR
    P["Publisher\n(session::send_group / mcp / gateway)"] -- "send {group, mail, body}" --> CS["CoordinatorStore::send_group\n(src/sql/coord.rs)"]
    CS -- "1 row in streams (next_article++)\n1 row in envelopes (kind='group')\n1 row in hot_bodies\n1 row in seen_ids" --> SDB[("slice-NN/hot.sqlite")]
    SDB -- "sql::coord::subscribers_of\n(SELECT reader FROM positions\nWHERE stream='g:<group>' AND subscribed=1)" --> FAN["coordinator::send\npush deliver_of (kind='group', group, article)"]

    FAN --> R1["Reader 1 Daemon -> session::serve\nDurLog::append(r-1) -> ack"]
    FAN --> R2["Reader 2 Daemon -> mcp::run (--groups)\nDurLog::append(r-4) -> ack"]
    FAN --> R3["Reader 3 Daemon -> roger-mcp-gateway / roger-ws-gateway\nStore::append(r-9) -> ack"]

    R1 -- "CoordinatorStore::record_ack" --> POS1["positions('g:<group>', Reader 1)\nposition=article_num, record_id='r-1'"]
    R2 -- "CoordinatorStore::record_ack" --> POS2["positions('g:<group>', Reader 2)\nposition=article_num, record_id='r-4'"]
    R3 -- "CoordinatorStore::record_ack" --> POS3["positions('g:<group>', Reader 3)\nposition=article_num, record_id='r-9'"]
```

## 5. Multi-Slice Writers (`--slices <n>`) and Cold Archive Shipping (`roger-cold-shipper`)

When `roger coordinator --slices <n>` runs with `n > 1`, `CoordinatorStore` opens `slice-00/hot.sqlite` through `slice-(n-1)/hot.sqlite`, each owned by its own independent group-commit `SliceWriter` thread (`sql::coord::writer_loop`). Streams (`m:<address>`, `g:<group>`) route to `sha256::hash_pair(stream.as_bytes()).0 % num_slices`, while `seen_ids` and `broadcasts` route to `sha256::hash_pair(mail_id.as_bytes()).0 % num_slices` so Mail id non-reuse holds globally across all slices while writes scale across independent `WAL` writers. Each slice directory `slice-NN/` is self-contained (`hot.sqlite` + `cold/`) and movable as files.

Alongside the core, the standalone Optional module `modules/cold-shipper/` (`roger-cold-shipper`) scans `slice-*/cold/day-*.cold` via `archive::ship` (or event-driven `archive::watch_stdin`), verifies `format::verify_cold_file` (`RWCOLD01`, per-record `digest`, and `SHA256:` trailer), atomically writes read-only copies plus `manifest.json` (`roger-cold-manifest/1`) to an archive destination, checks archive integrity via `archive::verify_archive`, and restores missing local day files via `archive::restore`.

```mermaid
flowchart LR
    SEND["coordinator::send"] -- "sha256::hash_pair(stream) % N" --> SW0["Slice 00 SliceWriter\nslice-00/hot.sqlite"]
    SEND -- "sha256::hash_pair(stream) % N" --> SWK["Slice NN SliceWriter\nslice-NN/hot.sqlite"]
    SEND -- "sha256::hash_pair(mail_id) % N" --> IDS["Global Mail-id shard\nseen_ids / broadcasts"]

    SW0 -- "ColdIndex::seal_day" --> C0[("slice-00/cold/day-<d>.cold")]
    SWK -- "ColdIndex::seal_day" --> CK[("slice-NN/cold/day-<d>.cold")]

    C0 --> SHIP["roger-cold-shipper\narchive::ship / archive::watch_stdin\nformat::verify_cold_file"]
    CK --> SHIP
    SHIP -- "atomic write + readonly" --> ARCH[("Archive Destination\n<dest>/slice-NN/day-<d>.cold\n<dest>/manifest.json")]
    ARCH -- "archive::verify_archive / archive::restore" --> CK
```
