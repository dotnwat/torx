//! slatedb-node runs one SlateDB process for the torx suite: a writer, a
//! reader, a standalone compactor, a compaction worker, or a garbage
//! collector, over a database in an S3 bucket. The suite drives it over a
//! small HTTP API of JSON requests, and kills, pauses, and restarts it.
//!
//! Usage:
//!
//!   slatedb-node ROLE --listen ADDR --db PATH [--settings FILE] [--options FILE]
//!                     [--clock-offset-ms N] [--seed N] [--merge-append]
//!                     [--segment-prefix-len N]
//!   slatedb-node settings        print the default Settings as JSON
//!
//! ROLE is writer, reader, compactor, worker, or gc. --settings is the
//! writer's Settings as JSON; --options is the reader's DbReaderOptions,
//! the compactor's CompactorOptions, the worker's CompactionWorkerOptions,
//! or the collector's GarbageCollectorOptions, as JSON. The object store is
//! S3, from the environment: SLATEDB_S3_ENDPOINT, SLATEDB_S3_BUCKET,
//! SLATEDB_S3_ACCESS_KEY (which names this process to the store),
//! SLATEDB_S3_TIMEOUT_MS.
//!
//! --merge-append installs a merge operator that appends an operand to the
//! value with a space between, in every role that reads or compacts; every
//! process of a database must agree on it. --segment-prefix-len splits the
//! writer's and reader's keys into segments (RFC 0024) by their first N
//! bytes.
//!
//! Every process keeps its own clock, SlateDB's clock offset by
//! --clock-offset-ms, which POST /clock moves at any time: the suite skews
//! and jumps the clocks of the processes that share a database.

use std::collections::HashMap;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, DurabilityLevel,
    GarbageCollectorOptions, PutOptions, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::{ClientOptions, ObjectStore, RetryConfig};
use slatedb::config::MergeOptions;
use slatedb::{
    CompactionWorkerBuilder, CompactorBuilder, Db, DbReader, ErrorKind, GarbageCollectorBuilder,
    IsolationLevel, MergeOperator, MergeOperatorError, PrefixExtractor, PrefixTarget, WriteBatch,
};
use slatedb_common::clock::{DefaultSystemClock, SystemClock, SystemClockTicker};

/// A clock offset from SlateDB's own by an amount the suite sets: a process
/// whose clock is skewed from its peers', or jumps.
#[derive(Debug)]
struct SkewedClock {
    inner: DefaultSystemClock,
    offset_ms: AtomicI64,
}

impl SystemClock for SkewedClock {
    fn now(&self) -> DateTime<Utc> {
        self.inner.now() + chrono::Duration::milliseconds(self.offset_ms.load(Ordering::SeqCst))
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.inner.sleep(duration)
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}

/// A merge operator that appends: the merge of a value and an operand is
/// the value, a space, and the operand. Appending is associative, so
/// SlateDB may fold operands in any grouping.
struct AppendOperator;

impl MergeOperator for AppendOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        Ok(match existing_value {
            Some(e) if !e.is_empty() => {
                let mut v = Vec::with_capacity(e.len() + 1 + value.len());
                v.extend_from_slice(&e);
                v.push(b' ');
                v.extend_from_slice(&value);
                Bytes::from(v)
            }
            _ => value,
        })
    }
}

/// A segment extractor (RFC 0024) whose segment is a key's first n bytes.
struct FixedPrefix {
    len: usize,
    name: String,
}

impl PrefixExtractor for FixedPrefix {
    fn name(&self) -> &str {
        &self.name
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let (PrefixTarget::Point(key) | PrefixTarget::Prefix(key)) = target;
        (key.len() >= self.len).then_some(self.len)
    }
}

struct Args {
    role: String,
    listen: String,
    db: String,
    settings: Option<String>,
    options: Option<String>,
    clock_offset_ms: i64,
    seed: Option<u64>,
    merge_append: bool,
    segment_prefix_len: usize,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut it = std::env::args().skip(1);
    let role = it.next().ok_or_else(|| anyhow!("usage: slatedb-node ROLE [flags]"))?;
    let mut a = Args {
        role,
        listen: "127.0.0.1:0".into(),
        db: "db".into(),
        settings: None,
        options: None,
        clock_offset_ms: 0,
        seed: None,
        merge_append: false,
        segment_prefix_len: 0,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| anyhow!("{flag} needs a value"));
        match flag.as_str() {
            "--listen" => a.listen = val()?,
            "--db" => a.db = val()?,
            "--settings" => a.settings = Some(val()?),
            "--options" => a.options = Some(val()?),
            "--clock-offset-ms" => a.clock_offset_ms = val()?.parse()?,
            "--seed" => a.seed = Some(val()?.parse()?),
            "--merge-append" => a.merge_append = true,
            "--segment-prefix-len" => a.segment_prefix_len = val()?.parse()?,
            _ => bail!("unknown flag {flag}"),
        }
    }
    Ok(a)
}

fn env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("{name} is not set"))
}

fn object_store() -> anyhow::Result<Arc<dyn ObjectStore>> {
    let timeout = std::env::var("SLATEDB_S3_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000u64);
    let store = AmazonS3Builder::new()
        .with_endpoint(env("SLATEDB_S3_ENDPOINT")?)
        .with_bucket_name(env("SLATEDB_S3_BUCKET")?)
        .with_access_key_id(env("SLATEDB_S3_ACCESS_KEY")?)
        .with_secret_access_key("secret")
        .with_region("us-east-1")
        .with_virtual_hosted_style_request(false)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(Duration::from_millis(timeout))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .with_retry(RetryConfig {
            max_retries: 3,
            retry_timeout: Duration::from_millis(timeout * 3),
            ..Default::default()
        })
        .build()?;
    Ok(Arc::new(store))
}

/// Reads a JSON file of the fields to change in T's defaults, and merges it
/// over them: the suite names only what it changes.
fn read_json<T>(path: &Option<String>) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de> + Serialize + Default,
{
    let Some(p) = path else {
        return Ok(T::default());
    };
    let s = std::fs::read_to_string(p).with_context(|| format!("reading {p}"))?;
    let over: Value = serde_json::from_str(&s).with_context(|| format!("parsing {p}"))?;
    let mut base = serde_json::to_value(T::default())?;
    merge(&mut base, over);
    serde_json::from_value(base).with_context(|| format!("decoding {p}"))
}

/// Parses a duration written as "250ms", "5s", or "2m".
fn parse_duration(s: &str) -> Option<Duration> {
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit())?);
    let n: u64 = num.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n * 60)),
        _ => None,
    }
}

/// Merges over into base: objects key by key, anything else replaced. Some
/// of SlateDB's options write a duration as "100ms", others as
/// {"secs", "nanos"}; a duration string over the latter is converted, so
/// the suite writes every duration the same way.
fn merge(base: &mut Value, over: Value) {
    if let (Value::Object(b), Value::String(s)) = (&*base, &over) {
        if b.contains_key("secs") && b.contains_key("nanos") {
            if let Some(d) = parse_duration(s) {
                *base = json!({"secs": d.as_secs(), "nanos": d.subsec_nanos()});
                return;
            }
        }
    }
    match (base, over) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(bv) => merge(bv, v),
                    None => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (b, o) => *b = o,
    }
}

/// What a handler answers: JSON, or an error with SlateDB's kind of it.
struct ApiError {
    status: StatusCode,
    kind: String,
    msg: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.msg, "kind": self.kind}))).into_response()
    }
}

impl From<slatedb::Error> for ApiError {
    fn from(e: slatedb::Error) -> Self {
        let kind = match e.kind() {
            ErrorKind::Transaction => "transaction".to_string(),
            ErrorKind::Closed(r) => format!("closed-{r:?}").to_lowercase(),
            ErrorKind::Unavailable => "unavailable".to_string(),
            ErrorKind::Invalid => "invalid".to_string(),
            ErrorKind::Data => "data".to_string(),
            ErrorKind::Internal => "internal".to_string(),
            other => format!("{other:?}").to_lowercase(),
        };
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            kind,
            msg: format!("{e}"),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            kind: "request".into(),
            msg: format!("{e:#}"),
        }
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

/// The database a process serves, when it serves one.
enum Handle {
    Writer(Db),
    Reader(DbReader),
    None,
}

struct App {
    role: String,
    /// The database's path and object store, and the merge operator and
    /// segment extractor every handle on it takes: for clones of it, and
    /// readers of those.
    path: String,
    store: Arc<dyn ObjectStore>,
    merge_op: Option<Arc<dyn MergeOperator + Send + Sync>>,
    extractor: Option<Arc<dyn PrefixExtractor>>,
    /// Snapshots opened by POST /snapshot/open, by ID, until closed.
    snapshots: std::sync::Mutex<HashMap<u64, Arc<slatedb::DbSnapshot>>>,
    next_snapshot: std::sync::atomic::AtomicU64,
    clock: Arc<SkewedClock>,
    handle: Handle,
    /// Set by a background task that ended -- a compactor, a collector, a
    /// worker -- with its result.
    ended: std::sync::Mutex<Option<String>>,
    ready: AtomicBool,
}

#[derive(Deserialize)]
struct KeyReq {
    key: String,
    #[serde(default)]
    durability: Option<String>,
    #[serde(default)]
    dirty: bool,
}

#[derive(Deserialize)]
struct PutReq {
    key: String,
    value: Option<String>, // None deletes
    #[serde(default)]
    durable: bool,
}

#[derive(Deserialize)]
struct BatchReq {
    ops: Vec<BatchOp>,
    #[serde(default)]
    durable: bool,
}

#[derive(Deserialize)]
struct BatchOp {
    key: String,
    value: Option<String>,
}

#[derive(Deserialize)]
struct ScanReq {
    start: Option<String>,
    end: Option<String>,
    #[serde(default)]
    durability: Option<String>,
    #[serde(default)]
    dirty: bool,
}

#[derive(Deserialize)]
struct TxnReq {
    #[serde(default)]
    isolation: Option<String>,
    ops: Vec<TxnOp>,
    #[serde(default)]
    durable: bool,
}

#[derive(Deserialize)]
struct TxnOp {
    op: String, // get, put, del, append
    key: String,
    #[serde(default)]
    value: Option<String>,
}

#[derive(Deserialize)]
struct ClockReq {
    offset_ms: i64,
}

#[derive(Serialize)]
struct Kv {
    key: String,
    value: String,
    seq: u64,
}

fn durability(d: &Option<String>) -> anyhow::Result<DurabilityLevel> {
    match d.as_deref() {
        None | Some("memory") => Ok(DurabilityLevel::Memory),
        Some("remote") => Ok(DurabilityLevel::Remote),
        Some(other) => bail!("unknown durability {other}"),
    }
}

fn text(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn unavailable(msg: &str) -> ApiError {
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        kind: "role".into(),
        msg: msg.into(),
    }
}

impl App {
    fn writer(&self) -> Result<&Db, ApiError> {
        match &self.handle {
            Handle::Writer(db) => Ok(db),
            _ => Err(unavailable("not a writer")),
        }
    }
}

async fn health(State(app): State<Arc<App>>) -> Response {
    if let Some(ended) = app.ended.lock().unwrap().clone() {
        return (StatusCode::SERVICE_UNAVAILABLE, ended).into_response();
    }
    if app.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ok").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "starting").into_response()
    }
}

async fn status(State(app): State<Arc<App>>) -> ApiResult {
    let mut out = json!({
        "role": app.role,
        "clock_offset_ms": app.clock.offset_ms.load(Ordering::SeqCst),
        "ended": *app.ended.lock().unwrap(),
    });
    if let Handle::Writer(db) = &app.handle {
        let st = db.status();
        out["durable_seq"] = json!(st.durable_seq);
        out["close_reason"] = json!(st.close_reason.map(|r| format!("{r:?}").to_lowercase()));
        out["manifest_id"] = json!(st.current_manifest.id());
    }
    Ok(Json(out))
}

async fn set_clock(State(app): State<Arc<App>>, Json(req): Json<ClockReq>) -> ApiResult {
    let old = app.clock.offset_ms.swap(req.offset_ms, Ordering::SeqCst);
    log::info!("clock offset {old}ms -> {}ms", req.offset_ms);
    Ok(Json(json!({"offset_ms": req.offset_ms, "was": old})))
}

async fn do_get(State(app): State<Arc<App>>, Json(req): Json<KeyReq>) -> ApiResult {
    let opts = ReadOptions {
        durability_filter: durability(&req.durability)?,
        dirty: req.dirty,
        ..ReadOptions::default()
    };
    let kv = match &app.handle {
        Handle::Writer(db) => db.get_key_value_with_options(req.key.as_bytes(), &opts).await?,
        Handle::Reader(r) => r.get_key_value_with_options(req.key.as_bytes(), &opts).await?,
        Handle::None => return Err(unavailable("no database")),
    };
    Ok(Json(match kv {
        Some(kv) => json!({"value": text(&kv.value), "seq": kv.seq}),
        None => json!({"value": null}),
    }))
}

async fn do_scan(State(app): State<Arc<App>>, Json(req): Json<ScanReq>) -> ApiResult {
    let opts = ScanOptions {
        durability_filter: durability(&req.durability)?,
        dirty: req.dirty,
        ..ScanOptions::default()
    };
    let start = match req.start {
        Some(s) => Bound::Included(Bytes::from(s)),
        None => Bound::Unbounded,
    };
    let end = match req.end {
        Some(s) => Bound::Excluded(Bytes::from(s)),
        None => Bound::Unbounded,
    };
    let mut iter = match &app.handle {
        Handle::Writer(db) => db.scan_with_options((start, end), &opts).await?,
        Handle::Reader(r) => r.scan_with_options((start, end), &opts).await?,
        Handle::None => return Err(unavailable("no database")),
    };
    let mut kvs = Vec::new();
    while let Some(kv) = iter.next().await? {
        kvs.push(Kv {
            key: text(&kv.key),
            value: text(&kv.value),
            seq: kv.seq,
        });
    }
    Ok(Json(json!({ "kvs": kvs })))
}

/// The answer to a write: its sequence number, and whether it was awaited
/// to durability. An error after the write was accepted says so in its
/// kind ("durable:..."), since the write may then survive.
async fn finish_write(
    handle: Result<slatedb::WriteHandle, slatedb::Error>,
    durable: bool,
) -> ApiResult {
    let handle = handle?;
    let seq = handle.seqnum();
    if durable {
        if let Err(e) = handle.await_durable().await {
            let mut err = ApiError::from(e);
            err.kind = format!("durable:{}", err.kind);
            err.msg = format!("seq {seq}: {}", err.msg);
            return Err(err);
        }
    }
    Ok(Json(json!({ "seq": seq })))
}

async fn do_put(State(app): State<Arc<App>>, Json(req): Json<PutReq>) -> ApiResult {
    let db = app.writer()?;
    let w = WriteOptions::default();
    let h = match &req.value {
        Some(v) => {
            db.put_with_options(req.key.as_bytes(), v.as_bytes(), &PutOptions::default(), &w)
                .await
        }
        None => db.delete_with_options(req.key.as_bytes(), &w).await,
    };
    finish_write(h, req.durable).await
}

/// Merges an operand into a key: with --merge-append, appends it.
async fn do_merge(State(app): State<Arc<App>>, Json(req): Json<PutReq>) -> ApiResult {
    let db = app.writer()?;
    let v = req.value.clone().ok_or_else(|| anyhow!("merge needs a value"))?;
    let h = db
        .merge_with_options(
            req.key.as_bytes(),
            v.as_bytes(),
            &MergeOptions::default(),
            &WriteOptions::default(),
        )
        .await;
    finish_write(h, req.durable).await
}

async fn do_batch(State(app): State<Arc<App>>, Json(req): Json<BatchReq>) -> ApiResult {
    let db = app.writer()?;
    let mut batch = WriteBatch::new();
    for op in &req.ops {
        match &op.value {
            Some(v) => batch.put(op.key.as_bytes(), v.as_bytes()),
            None => batch.delete(op.key.as_bytes()),
        }
    }
    let h = db.write_with_options(batch, &WriteOptions::default()).await;
    finish_write(h, req.durable).await
}

/// Runs a transaction: its operations in order, then a commit. A get
/// answers the value read; an append reads the key, appends the value to
/// what it read with a space between, writes the result, and answers what
/// it read.
async fn do_txn(State(app): State<Arc<App>>, Json(req): Json<TxnReq>) -> ApiResult {
    let db = app.writer()?;
    let level = match req.isolation.as_deref() {
        None | Some("si") => IsolationLevel::Snapshot,
        Some("ssi") => IsolationLevel::SerializableSnapshot,
        Some(other) => return Err(anyhow!("unknown isolation {other}").into()),
    };
    #[cfg(feature = "async-api")]
    let txn = db.begin(level).await?;
    #[cfg(not(feature = "async-api"))]
    let txn = db.begin(level)?;
    let mut results: Vec<Value> = Vec::new();
    for op in &req.ops {
        match op.op.as_str() {
            "get" => {
                let v = txn.get(op.key.as_bytes()).await?;
                results.push(json!(v.as_ref().map(text)));
            }
            "put" => {
                let v = op.value.clone().ok_or_else(|| anyhow!("put needs a value"))?;
                txn.put(op.key.as_bytes(), v.as_bytes())?;
                results.push(Value::Null);
            }
            "del" => {
                txn.delete(op.key.as_bytes())?;
                results.push(Value::Null);
            }
            "append" => {
                let v = op.value.clone().ok_or_else(|| anyhow!("append needs a value"))?;
                let old = txn.get(op.key.as_bytes()).await?;
                let new = match &old {
                    Some(o) if !o.is_empty() => format!("{} {v}", text(o)),
                    _ => v,
                };
                txn.put(op.key.as_bytes(), new.as_bytes())?;
                results.push(json!(old.as_ref().map(text)));
            }
            other => return Err(anyhow!("unknown txn op {other}").into()),
        }
    }
    let handle = txn.commit_with_options(&WriteOptions::default()).await?;
    let Some(handle) = handle else {
        return Ok(Json(json!({ "results": results, "seq": null })));
    };
    let seq = handle.seqnum();
    if req.durable {
        if let Err(e) = handle.await_durable().await {
            let mut err = ApiError::from(e);
            err.kind = format!("durable:{}", err.kind);
            err.msg = format!("seq {seq}: {}", err.msg);
            return Err(err);
        }
    }
    Ok(Json(json!({ "results": results, "seq": seq })))
}

#[derive(Deserialize)]
struct SnapshotReq {
    id: u64,
}

/// Opens a snapshot of the writer's database and answers its ID and the
/// sequence number it reads at.
async fn snapshot_open(State(app): State<Arc<App>>) -> ApiResult {
    let db = app.writer()?;
    #[cfg(feature = "async-api")]
    let snap = db.snapshot().await?;
    #[cfg(not(feature = "async-api"))]
    let snap = db.snapshot()?;
    let seq = snap.seq();
    let id = app.next_snapshot.fetch_add(1, Ordering::SeqCst);
    app.snapshots.lock().unwrap().insert(id, snap);
    Ok(Json(json!({ "id": id, "seq": seq })))
}

/// Scans every key of a snapshot.
async fn snapshot_scan(State(app): State<Arc<App>>, Json(req): Json<SnapshotReq>) -> ApiResult {
    let snap = app
        .snapshots
        .lock()
        .unwrap()
        .get(&req.id)
        .cloned()
        .ok_or_else(|| anyhow!("no snapshot {}", req.id))?;
    let mut iter = snap
        .scan_with_options::<std::ops::RangeFull>(.., &ScanOptions::default())
        .await?;
    let mut kvs = Vec::new();
    while let Some(kv) = iter.next().await? {
        kvs.push(Kv {
            key: text(&kv.key),
            value: text(&kv.value),
            seq: kv.seq,
        });
    }
    Ok(Json(json!({ "kvs": kvs })))
}

async fn snapshot_close(State(app): State<Arc<App>>, Json(req): Json<SnapshotReq>) -> ApiResult {
    app.snapshots.lock().unwrap().remove(&req.id);
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
struct CloneReq {
    name: String,
}

/// Checkpoints the writer's database -- every write it has taken, flushed
/// first -- and clones the checkpoint to the path name.
async fn clone_create(State(app): State<Arc<App>>, Json(req): Json<CloneReq>) -> ApiResult {
    let db = app.writer()?;
    let cp = db
        .create_checkpoint(
            slatedb::config::CheckpointScope::All,
            &slatedb::config::CheckpointOptions::default(),
        )
        .await?;
    let admin = slatedb::admin::Admin::builder(req.name.as_str(), app.store.clone()).build();
    admin
        .create_clone_builder_from_source(slatedb::admin::CloneSourceSpec::with_checkpoint(
            app.path.as_str(),
            cp.id,
        ))
        .build()
        .await?;
    Ok(Json(
        json!({ "checkpoint": cp.id.to_string(), "manifest_id": cp.manifest_id }),
    ))
}

/// Opens a reader on the clone at path name, scans every key, and closes it.
async fn clone_scan(State(app): State<Arc<App>>, Json(req): Json<CloneReq>) -> ApiResult {
    let mut b = DbReader::builder(req.name.as_str(), app.store.clone());
    if let Some(op) = app.merge_op.clone() {
        b = b.with_merge_operator(op);
    }
    if let Some(x) = app.extractor.clone() {
        b = b.with_segment_extractor(x);
    }
    let reader = b.build().await?;
    let result = async {
        let mut iter = reader
            .scan_with_options::<std::ops::RangeFull>(.., &ScanOptions::default())
            .await?;
        let mut kvs = Vec::new();
        while let Some(kv) = iter.next().await? {
            kvs.push(Kv {
                key: text(&kv.key),
                value: text(&kv.value),
                seq: kv.seq,
            });
        }
        Ok::<_, slatedb::Error>(kvs)
    }
    .await;
    let _ = reader.close().await;
    Ok(Json(json!({ "kvs": result? })))
}

async fn do_flush(State(app): State<Arc<App>>) -> ApiResult {
    app.writer()?.flush().await?;
    Ok(Json(json!({})))
}

async fn serve(app: Arc<App>, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/clock", post(set_clock))
        .route("/get", post(do_get))
        .route("/scan", post(do_scan))
        .route("/put", post(do_put))
        .route("/batch", post(do_batch))
        .route("/merge", post(do_merge))
        .route("/txn", post(do_txn))
        .route("/flush", post(do_flush))
        .route("/snapshot/open", post(snapshot_open))
        .route("/snapshot/scan", post(snapshot_scan))
        .route("/snapshot/close", post(snapshot_close))
        .route("/clone", post(clone_create))
        .route("/clone/scan", post(clone_scan))
        .with_state(app);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn sigterm() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    term.recv().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();
    let args = parse_args()?;
    if args.role == "settings" {
        println!("{}", Settings::default().to_json_string()?);
        return Ok(());
    }
    let clock = Arc::new(SkewedClock {
        inner: DefaultSystemClock::new(),
        offset_ms: AtomicI64::new(args.clock_offset_ms),
    });
    let sys: Arc<dyn SystemClock> = clock.clone();
    let merge_op: Option<Arc<dyn MergeOperator + Send + Sync>> = args
        .merge_append
        .then(|| Arc::new(AppendOperator) as Arc<dyn MergeOperator + Send + Sync>);
    let extractor: Option<Arc<dyn PrefixExtractor>> = (args.segment_prefix_len > 0).then(|| {
        Arc::new(FixedPrefix {
            len: args.segment_prefix_len,
            name: format!("fixed_{}_byte", args.segment_prefix_len),
        }) as Arc<dyn PrefixExtractor>
    });
    let store = object_store()?;
    let store_for_app = store.clone();
    let path = args.db.clone();
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    log::info!(
        "slatedb-node {} on {} for {} (clock offset {}ms)",
        args.role,
        listener.local_addr()?,
        path,
        args.clock_offset_ms
    );

    // Background roles run their loop in a task, and serve /health and
    // /clock meanwhile; a loop that ends is reported by /health.
    let mut task: Option<tokio::task::JoinHandle<anyhow::Result<()>>> = None;
    let mut stopper: Option<Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send>> =
        None;
    let handle = match args.role.as_str() {
        "writer" => {
            let settings: Settings = read_json(&args.settings)?;
            log::info!("settings: {}", settings.to_json_string()?);
            let mut b = Db::builder(path.as_str(), store)
                .with_settings(settings)
                .with_system_clock(sys.clone());
            if let Some(op) = merge_op.clone() {
                b = b.with_merge_operator(op);
            }
            if let Some(x) = extractor.clone() {
                b = b.with_segment_extractor(x);
            }
            if let Some(seed) = args.seed {
                b = b.with_seed(seed);
            }
            Handle::Writer(b.build().await?)
        }
        "reader" => {
            let opts: DbReaderOptions = read_json(&args.options)?;
            let mut b = DbReader::builder(path.as_str(), store)
                .with_options(opts)
                .with_system_clock(sys.clone());
            if let Some(op) = merge_op.clone() {
                b = b.with_merge_operator(op);
            }
            if let Some(x) = extractor.clone() {
                b = b.with_segment_extractor(x);
            }
            if let Some(seed) = args.seed {
                b = b.with_seed(seed);
            }
            Handle::Reader(b.build().await?)
        }
        "compactor" => {
            let opts: CompactorOptions = read_json(&args.options)?;
            let mut b = CompactorBuilder::new(path.as_str(), store)
                .with_options(opts)
                .with_system_clock(sys.clone());
            if let Some(op) = merge_op.clone() {
                b = b.with_merge_operator(op);
            }
            if let Some(seed) = args.seed {
                b = b.with_seed(seed);
            }
            let c = Arc::new(b.build());
            let c2 = c.clone();
            task = Some(tokio::spawn(async move { Ok(c2.run().await?) }));
            stopper = Some(Box::new(move || {
                Box::pin(async move {
                    let _ = c.stop().await;
                })
            }));
            Handle::None
        }
        "worker" => {
            let opts: CompactionWorkerOptions = read_json(&args.options)?;
            let mut b = CompactionWorkerBuilder::new(path.as_str(), store)
                .with_options(opts)
                .with_system_clock(sys.clone());
            if let Some(op) = merge_op.clone() {
                b = b.with_merge_operator(op);
            }
            if let Some(seed) = args.seed {
                b = b.with_seed(seed);
            }
            let w = Arc::new(b.build().await?);
            let w2 = w.clone();
            task = Some(tokio::spawn(async move { Ok(w2.run().await?) }));
            stopper = Some(Box::new(move || {
                Box::pin(async move {
                    let _ = w.stop().await;
                })
            }));
            Handle::None
        }
        "gc" => {
            let opts: GarbageCollectorOptions = read_json(&args.options)?;
            let mut b = GarbageCollectorBuilder::new(path.as_str(), store)
                .with_options(opts)
                .with_system_clock(sys.clone());
            if let Some(seed) = args.seed {
                b = b.with_seed(seed);
            }
            let g = Arc::new(b.build());
            let g2 = g.clone();
            task = Some(tokio::spawn(async move { Ok(g2.run().await?) }));
            stopper = Some(Box::new(move || {
                Box::pin(async move {
                    let _ = g.stop().await;
                })
            }));
            Handle::None
        }
        other => bail!("unknown role {other}"),
    };

    let app = Arc::new(App {
        role: args.role.clone(),
        path: args.db.clone(),
        store: store_for_app,
        merge_op: merge_op.clone(),
        extractor: extractor.clone(),
        snapshots: std::sync::Mutex::new(HashMap::new()),
        // Snapshot IDs start somewhere new in each process, so an ID from
        // a process that has since been replaced names nothing here.
        next_snapshot: std::sync::atomic::AtomicU64::new(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64)
                << 16,
        ),
        clock,
        handle,
        ended: std::sync::Mutex::new(None),
        ready: AtomicBool::new(true),
    });
    if let Some(t) = task {
        let app2 = app.clone();
        tokio::spawn(async move {
            let res = match t.await {
                Ok(Ok(())) => "ended".to_string(),
                Ok(Err(e)) => format!("failed: {e:#}"),
                Err(e) => format!("panicked: {e}"),
            };
            log::error!("{} loop {res}", app2.role);
            *app2.ended.lock().unwrap() = Some(res);
        });
    }
    let server = tokio::spawn(serve(app.clone(), listener));
    sigterm().await;
    log::info!("SIGTERM: closing");
    match &app.handle {
        Handle::Writer(db) => {
            if let Err(e) = db.close().await {
                log::error!("close: {e}");
            }
        }
        Handle::Reader(r) => {
            if let Err(e) = r.close().await {
                log::error!("close: {e}");
            }
        }
        Handle::None => {}
    }
    if let Some(stop) = stopper {
        stop().await;
    }
    server.abort();
    log::info!("closed");
    Ok(())
}
