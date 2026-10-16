// KuduNode serves the Kudu Java client over HTTP, for the torx Kudu suite.
//
// Kudu has no Go client, so the suite's jobs reach Kudu through this
// program: each request is one client operation -- a batch of writes, a
// scan, a transaction's begin, commit, or rollback -- and its answer says
// what Kudu answered, with the timestamps Kudu assigned: a write's commit
// timestamp, and a scan's snapshot timestamp, which the suite's checker
// orders the history by.
//
// A request names its client: each client is a KuduClient of its own, with
// its own propagated timestamp, as each of an application's processes
// would have. Requests are JSON objects; so are answers.
//
// It runs from source, with no build step (JEP 330/458):
//
//	java -cp 'jars/*' KuduNode.java --listen HOST:PORT --masters A,B,C
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPartitioner;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduTransaction;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.ReplicaSelection;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Status;

public final class KuduNode {
  private static String masters;
  private static final Map<String, KuduClient> clients = new ConcurrentHashMap<>();
  private static final Map<String, KuduTransaction> txns = new ConcurrentHashMap<>();
  private static final AtomicLong txnSeq = new AtomicLong();

  // The scanner's snapshot timestamp -- the one the client asked for, or
  // the one the first tablet server chose -- is not public.
  private static Field asyncScannerField;
  private static Method snapshotTimestamp;

  interface Handler {
    Object handle(Map<String, Object> req) throws Exception;
  }

  public static void main(String[] args) throws Exception {
    String listen = null;
    for (int i = 0; i + 1 < args.length; i += 2) {
      switch (args[i]) {
        case "--listen" -> listen = args[i + 1];
        case "--masters" -> masters = args[i + 1];
        default -> throw new IllegalArgumentException("unknown flag " + args[i]);
      }
    }
    if (listen == null || masters == null) {
      throw new IllegalArgumentException("usage: KuduNode --listen HOST:PORT --masters ADDRS");
    }
    asyncScannerField = KuduScanner.class.getDeclaredField("asyncScanner");
    asyncScannerField.setAccessible(true);
    snapshotTimestamp = AsyncKuduScanner.class.getDeclaredMethod("getSnapshotTimestamp");
    snapshotTimestamp.setAccessible(true);

    int colon = listen.lastIndexOf(':');
    HttpServer server = HttpServer.create(
        new InetSocketAddress(listen.substring(0, colon), Integer.parseInt(listen.substring(colon + 1))), 256);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    route(server, "/health", req -> Map.of("ok", true));
    route(server, "/create_table", KuduNode::createTable);
    route(server, "/drop_table", KuduNode::dropTable);
    route(server, "/alter_table", KuduNode::alterTable);
    route(server, "/write", KuduNode::write);
    route(server, "/read", KuduNode::read);
    route(server, "/tablets", KuduNode::tablets);
    route(server, "/partition", KuduNode::partition);
    route(server, "/txn/begin", KuduNode::txnBegin);
    route(server, "/txn/commit", KuduNode::txnCommit);
    route(server, "/txn/rollback", KuduNode::txnRollback);
    route(server, "/client/close", KuduNode::clientClose);
    server.start();
    System.err.println("kudu-node: serving on " + listen + ", masters " + masters);
  }

  private static void route(HttpServer server, String path, Handler h) {
    server.createContext(path, ex -> serve(ex, h));
  }

  private static void serve(HttpExchange ex, Handler h) throws IOException {
    int code = 200;
    Object out;
    try {
      Map<String, Object> req = Map.of();
      byte[] body = readAll(ex.getRequestBody());
      if (body.length > 0) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) Json.parse(new String(body, StandardCharsets.UTF_8));
        req = m;
      }
      out = h.handle(req);
    } catch (BadRequest e) {
      code = 400;
      out = Map.of("error", e.getMessage());
    } catch (Throwable t) {
      code = 500;
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      out = Map.of("error", t.toString(), "stack", sw.toString());
    }
    byte[] b = Json.write(out).getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(code, b.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(b);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    in.transferTo(bo);
    return bo.toByteArray();
  }

  static final class BadRequest extends Exception {
    BadRequest(String msg) {
      super(msg);
    }
  }

  // --- clients ---

  private static KuduClient client(Map<String, Object> req) throws BadRequest {
    String name = str(req, "client", null);
    if (name == null) {
      throw new BadRequest("no client");
    }
    long timeout = num(req, "timeout_ms", 10_000);
    return clients.computeIfAbsent(name, n -> new KuduClient.KuduClientBuilder(masters)
        .defaultOperationTimeoutMs(timeout)
        .defaultAdminOperationTimeoutMs(Math.max(timeout, 30_000))
        .build());
  }

  private static Object clientClose(Map<String, Object> req) throws Exception {
    KuduClient c = clients.remove(str(req, "client", ""));
    if (c != null) {
      tables.remove(c);
      c.close();
    }
    return Map.of("ok", c != null);
  }

  // --- tables ---

  // The suite's table: a key, a value naming the write that wrote it, and a
  // payload whose bytes derive from the value, so a read can be checked byte
  // for byte.
  private static Schema schema(Map<String, Object> req) {
    ColumnSchema.Encoding venc = ColumnSchema.Encoding.valueOf(str(req, "value_encoding", "AUTO_ENCODING"));
    ColumnSchema.Encoding penc = ColumnSchema.Encoding.valueOf(str(req, "payload_encoding", "AUTO_ENCODING"));
    ColumnSchema.CompressionAlgorithm comp =
        ColumnSchema.CompressionAlgorithm.valueOf(str(req, "compression", "DEFAULT_COMPRESSION"));
    return new Schema(List.of(
        new ColumnSchema.ColumnSchemaBuilder("k", Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("v", Type.STRING).nullable(true).encoding(venc).compressionAlgorithm(comp).build(),
        new ColumnSchema.ColumnSchemaBuilder("p", Type.STRING).nullable(true).encoding(penc).compressionAlgorithm(comp).build()));
  }

  // A table with a non-unique primary key: k, and the auto-incrementing id
  // Kudu adds, which the tablet server assigns each row it inserts.
  private static Schema nonUniqueSchema() {
    return new Schema(List.of(
        new ColumnSchema.ColumnSchemaBuilder("k", Type.INT64).nonUniqueKey(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("v", Type.STRING).nullable(true).build()));
  }

  private static Object createTable(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    String table = str(req, "table", null);
    CreateTableOptions o = new CreateTableOptions().setNumReplicas((int) num(req, "replicas", 3));
    int buckets = (int) num(req, "hash_buckets", 0);
    if (buckets > 1) {
      o.addHashPartitions(List.of("k"), buckets);
    }
    Schema s = Boolean.TRUE.equals(req.get("non_unique")) ? nonUniqueSchema() : schema(req);
    o.setRangePartitionColumns(List.of("k"));
    List<Object> splits = list(req, "range_splits");
    for (Object split : splits) {
      PartialRow r = s.newPartialRow();
      r.addLong("k", ((Number) split).longValue());
      o.addSplitRow(r);
    }
    Map<String, Object> extra = map(req, "extra_configs");
    if (!extra.isEmpty()) {
      Map<String, String> e = new LinkedHashMap<>();
      extra.forEach((k, v) -> e.put(k, String.valueOf(v)));
      o.setExtraConfigs(e);
    }
    if (req.containsKey("wait")) {
      o.setWait((Boolean) req.get("wait"));
    }
    try {
      c.createTable(table, s, o);
    } catch (KuduException e) {
      return failure(e, c);
    }
    return Map.of("ok", true);
  }

  private static Object dropTable(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    try {
      c.deleteTable(str(req, "table", null));
    } catch (KuduException e) {
      return failure(e, c);
    }
    return Map.of("ok", true);
  }

  // alter_table changes a table's options -- extra configs, for one: the
  // history a tablet keeps, its flush and compaction policy.
  private static Object alterTable(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    AlterTableOptions o = new AlterTableOptions();
    Map<String, Object> extra = map(req, "extra_configs");
    if (!extra.isEmpty()) {
      Map<String, String> e = new LinkedHashMap<>();
      extra.forEach((k, v) -> e.put(k, String.valueOf(v)));
      o.alterExtraConfigs(e);
    }
    try {
      c.alterTable(str(req, "table", null), o);
    } catch (KuduException e) {
      return failure(e, c);
    }
    return Map.of("ok", true);
  }

  // Each client's open tables: a session takes only operations made from
  // a table its own client opened.
  private static final Map<KuduClient, Map<String, KuduTable>> tables = new ConcurrentHashMap<>();

  private static KuduTable table(KuduClient c, Map<String, Object> req) throws KuduException, BadRequest {
    String name = str(req, "table", null);
    if (name == null) {
      throw new BadRequest("no table");
    }
    Map<String, KuduTable> mine = tables.computeIfAbsent(c, x -> new ConcurrentHashMap<>());
    KuduTable t = mine.get(name);
    if (t == null) {
      t = c.openTable(name);
      mine.put(name, t);
    }
    return t;
  }

  private static Object tablets(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    List<Object> out = new ArrayList<>();
    try {
      KuduTable t = table(c, req);
      for (LocatedTablet lt : t.getTabletsLocations(num(req, "timeout_ms", 10_000))) {
        List<Object> replicas = new ArrayList<>();
        for (LocatedTablet.Replica r : lt.getReplicas()) {
          replicas.add(Map.of("host", r.getRpcHost(), "port", r.getRpcPort(), "role", r.getRole()));
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", new String(lt.getTabletId(), StandardCharsets.UTF_8));
        m.put("replicas", replicas);
        out.add(m);
      }
    } catch (KuduException e) {
      return failure(e, c);
    }
    return Map.of("tablets", out);
  }

  // --- writes ---

  private static Operation op(KuduTable t, Map<String, Object> o) throws BadRequest {
    String kind = str(o, "op", null);
    Operation op = switch (kind) {
      case "insert" -> t.newInsert();
      case "insert_ignore" -> t.newInsertIgnore();
      case "upsert" -> t.newUpsert();
      case "upsert_ignore" -> t.newUpsertIgnore();
      case "update" -> t.newUpdate();
      case "update_ignore" -> t.newUpdateIgnore();
      case "delete" -> t.newDelete();
      case "delete_ignore" -> t.newDeleteIgnore();
      default -> throw new BadRequest("unknown op " + kind);
    };
    PartialRow r = op.getRow();
    Schema s = t.getSchema();
    r.addLong("k", num(o, "k", 0));
    if (o.containsKey("id")) {
      r.addLong(Schema.getAutoIncrementingColumnName(), num(o, "id", 0));
    }
    if (!kind.startsWith("delete")) {
      r.addString("v", str(o, "v", ""));
      if (s.hasColumn("p")) {
        r.addString("p", str(o, "p", ""));
      }
    }
    return op;
  }

  // write applies a batch of operations in one session: one operation is
  // flushed as it is applied, several at once. Each answer is the
  // operation's: Kudu flushes each tablet's operations as a request of its
  // own, and answers each with that request's timestamp.
  private static Object write(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    long before = c.getLastPropagatedTimestamp();
    List<Object> ops = list(req, "ops");
    KuduSession s;
    String txn = str(req, "txn", null);
    if (txn != null) {
      KuduTransaction t = txns.get(txn);
      if (t == null) {
        throw new BadRequest("no transaction " + txn);
      }
      s = t.newKuduSession();
    } else {
      s = c.newSession();
    }
    s.setTimeoutMillis(num(req, "timeout_ms", 10_000));
    List<Object> results = new ArrayList<>();
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      KuduTable t = table(c, req);
      List<OperationResponse> resps = new ArrayList<>();
      if (ops.size() == 1) {
        s.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        @SuppressWarnings("unchecked")
        Map<String, Object> o = (Map<String, Object>) ops.get(0);
        resps.add(s.apply(op(t, o)));
      } else {
        s.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        s.setMutationBufferSpace(Math.max(1000, ops.size()));
        for (Object x : ops) {
          @SuppressWarnings("unchecked")
          Map<String, Object> o = (Map<String, Object>) x;
          OperationResponse r = s.apply(op(t, o));
          if (r != null) {
            resps.add(r);
          }
        }
        resps.addAll(s.flush());
      }
      // A flush answers its operations in the order they were applied.
      for (OperationResponse r : resps) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (r == null) {
          // A batch that failed as a whole leaves its operations unanswered.
          m.put("code", "Unknown");
          m.put("ts", -1L);
          results.add(m);
          continue;
        }
        if (r.hasRowError()) {
          RowError e = r.getRowError();
          m.put("code", code(e.getErrorStatus()));
          m.put("msg", e.getErrorStatus().toString());
          m.put("k", e.getOperation().getRow().getLong("k"));
        } else {
          m.put("code", "OK");
        }
        m.put("ts", r.getWriteTimestampRaw());
        m.put("server", r.getTsUUID() == null ? "" : r.getTsUUID());
        results.add(m);
      }
      out.put("results", results);
    } catch (KuduException e) {
      out.putAll(failure(e, c));
      out.put("results", results);
    } finally {
      try {
        s.close();
      } catch (KuduException e) {
        // the session's operations were answered above, or failed with it
      }
    }
    out.put("propagated_before", before);
    out.put("propagated_after", c.getLastPropagatedTimestamp());
    return out;
  }

  // --- reads ---

  // read scans the table in a read mode, at a snapshot timestamp or at one
  // the server picks, for the keys named, or a range of them, or every key.
  // A snapshot scan of several tablets reads each at the timestamp the
  // first one picked; a read-your-writes scan picks one per tablet, so the
  // suite reads one key at a time with it.
  private static Object read(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    long before = c.getLastPropagatedTimestamp();
    Map<String, Object> out = new LinkedHashMap<>();
    List<Object> rows = new ArrayList<>();
    // The tablet servers that served the rows: each row names one by its
    // index here.
    List<Object> servers = new ArrayList<>();
    Map<String, Integer> serverIndex = new LinkedHashMap<>();
    KuduScanner sc = null;
    try {
      KuduTable t = table(c, req);
      KuduScanner.KuduScannerBuilder b = c.newScannerBuilder(t)
          .scanRequestTimeout(num(req, "timeout_ms", 10_000));
      String mode = str(req, "mode", "snapshot");
      long ts = num(req, "ts", 0);
      switch (mode) {
        case "snapshot" -> b.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
        case "ryw" -> b.readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES);
        case "latest" -> b.readMode(AsyncKuduScanner.ReadMode.READ_LATEST);
        // A diff scan, as an incremental backup takes: the rows that
        // changed between start_ts and ts, as of ts, those deleted marked.
        case "diff" -> b.diffScan(num(req, "start_ts", 0), ts);
        default -> throw new BadRequest("unknown read mode " + mode);
      }
      boolean auto = t.getSchema().hasAutoIncrementingColumn();
      if (!mode.equals("diff")) {
        // A diff scan projects every column and IS_DELETED itself.
        b.setProjectedColumnNames(auto
            ? List.of("k", "v", Schema.getAutoIncrementingColumnName())
            : List.of("k", "v", "p"));
        if (ts > 0) {
          b.snapshotTimestampRaw(ts);
        }
      }
      if ("closest".equals(str(req, "replica", "leader"))) {
        b.replicaSelection(ReplicaSelection.CLOSEST_REPLICA);
      } else {
        b.replicaSelection(ReplicaSelection.LEADER_ONLY);
      }
      if (Boolean.TRUE.equals(req.get("fault_tolerant"))) {
        b.setFaultTolerant(true);
      }
      ColumnSchema k = t.getSchema().getColumn("k");
      List<Object> keys = list(req, "keys");
      if (!keys.isEmpty()) {
        List<Long> ks = new ArrayList<>();
        for (Object x : keys) {
          ks.add(((Number) x).longValue());
        }
        b.addPredicate(ks.size() == 1
            ? KuduPredicate.newComparisonPredicate(k, KuduPredicate.ComparisonOp.EQUAL, ks.get(0))
            : KuduPredicate.newInListPredicate(k, ks));
      }
      if (req.containsKey("lo")) {
        b.addPredicate(KuduPredicate.newComparisonPredicate(k, KuduPredicate.ComparisonOp.GREATER_EQUAL, num(req, "lo", 0)));
      }
      if (req.containsKey("hi")) {
        b.addPredicate(KuduPredicate.newComparisonPredicate(k, KuduPredicate.ComparisonOp.LESS, num(req, "hi", 0)));
      }
      sc = b.build();
      while (sc.hasMoreRows()) {
        RowResultIterator it = sc.nextRows();
        String uuid = it.getTsUUID() == null ? "" : it.getTsUUID();
        Integer si = serverIndex.get(uuid);
        if (si == null) {
          si = servers.size();
          serverIndex.put(uuid, si);
          servers.add(uuid);
        }
        while (it.hasNext()) {
          RowResult r = it.next();
          List<Object> row = new ArrayList<>(6);
          row.add(r.getLong("k"));
          row.add(r.isNull("v") ? null : r.getString("v"));
          row.add(auto || r.isNull("p") ? null : r.getString("p"));
          row.add(si);
          row.add(r.hasIsDeleted() && r.isDeleted());
          row.add(auto ? r.getLong(Schema.getAutoIncrementingColumnName()) : -1L);
          rows.add(row);
        }
      }
      out.put("rows", rows);
    } catch (KuduException e) {
      out.putAll(failure(e, c));
    } finally {
      out.put("servers", servers);
      if (sc != null) {
        out.put("snapshot_ts", (long) snapshotTimestamp.invoke(asyncScannerField.get(sc)));
        try {
          sc.close();
        } catch (KuduException e) {
          // the scan is over either way
        }
      }
    }
    out.put("propagated_before", before);
    out.put("propagated_after", c.getLastPropagatedTimestamp());
    return out;
  }

  // partition names the tablet each key is in, as the client's partitioner
  // places it.
  private static Object partition(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    try {
      KuduTable t = table(c, req);
      KuduPartitioner p = new KuduPartitioner.KuduPartitionerBuilder(t).build();
      List<String> ids = new ArrayList<>(p.getTabletMap().keySet());
      List<Object> out = new ArrayList<>();
      for (Object x : list(req, "keys")) {
        PartialRow row = t.getSchema().newPartialRow();
        row.addLong("k", ((Number) x).longValue());
        out.add(ids.get(p.partitionRow(row)));
      }
      return Map.of("tablets", out);
    } catch (KuduException e) {
      return failure(e, c);
    }
  }

  // --- transactions ---

  private static Object txnBegin(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    try {
      KuduTransaction t = c.newTransaction();
      String id = str(req, "client", "") + "/txn-" + txnSeq.incrementAndGet();
      txns.put(id, t);
      return Map.of("txn", id);
    } catch (KuduException e) {
      return failure(e, c);
    }
  }

  private static Object txnCommit(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    KuduTransaction t = txns.remove(str(req, "txn", ""));
    if (t == null) {
      throw new BadRequest("no transaction " + str(req, "txn", ""));
    }
    long before = c.getLastPropagatedTimestamp();
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      t.commit();
      out.put("ok", true);
    } catch (KuduException e) {
      out.putAll(failure(e, c));
    } finally {
      t.close();
    }
    out.put("propagated_before", before);
    out.put("propagated_after", c.getLastPropagatedTimestamp());
    return out;
  }

  private static Object txnRollback(Map<String, Object> req) throws Exception {
    KuduClient c = client(req);
    KuduTransaction t = txns.remove(str(req, "txn", ""));
    if (t == null) {
      throw new BadRequest("no transaction " + str(req, "txn", ""));
    }
    Map<String, Object> out = new LinkedHashMap<>();
    try {
      t.rollback();
      out.put("ok", true);
    } catch (KuduException e) {
      out.putAll(failure(e, c));
    } finally {
      t.close();
    }
    return out;
  }

  // --- errors ---

  private static Map<String, Object> failure(KuduException e, KuduClient c) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("code", code(e.getStatus()));
    m.put("error", e.getStatus().toString());
    return m;
  }

  private static String code(Status s) {
    if (s.ok()) return "OK";
    if (s.isAlreadyPresent()) return "AlreadyPresent";
    if (s.isNotFound()) return "NotFound";
    if (s.isTimedOut()) return "TimedOut";
    if (s.isServiceUnavailable()) return "ServiceUnavailable";
    if (s.isNetworkError()) return "NetworkError";
    if (s.isIllegalState()) return "IllegalState";
    if (s.isInvalidArgument()) return "InvalidArgument";
    if (s.isAborted()) return "Aborted";
    if (s.isIOError()) return "IOError";
    if (s.isCorruption()) return "Corruption";
    if (s.isRemoteError()) return "RemoteError";
    if (s.isRuntimeError()) return "RuntimeError";
    if (s.isNotSupported()) return "NotSupported";
    if (s.isNotAuthorized()) return "NotAuthorized";
    if (s.isIncomplete()) return "Incomplete";
    if (s.isUninitialized()) return "Uninitialized";
    if (s.isConfigurationError()) return "ConfigurationError";
    if (s.isEndOfFile()) return "EndOfFile";
    return "Unknown";
  }

  // --- request fields ---

  private static String str(Map<String, Object> m, String k, String dflt) {
    Object v = m.get(k);
    return v == null ? dflt : v.toString();
  }

  private static long num(Map<String, Object> m, String k, long dflt) {
    Object v = m.get(k);
    return v == null ? dflt : ((Number) v).longValue();
  }

  @SuppressWarnings("unchecked")
  private static List<Object> list(Map<String, Object> m, String k) {
    Object v = m.get(k);
    return v == null ? List.of() : (List<Object>) v;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Map<String, Object> m, String k) {
    Object v = m.get(k);
    return v == null ? Map.of() : (Map<String, Object>) v;
  }

  // Json is just enough JSON: objects, arrays, strings, integers (as Long,
  // so a timestamp keeps every bit), other numbers, booleans, and null.
  static final class Json {
    private final String s;
    private int i;

    private Json(String s) {
      this.s = s;
    }

    static Object parse(String s) {
      Json p = new Json(s);
      Object v = p.value();
      p.ws();
      if (p.i != s.length()) {
        throw new IllegalArgumentException("json: trailing data at " + p.i);
      }
      return v;
    }

    private void ws() {
      while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
        i++;
      }
    }

    private Object value() {
      ws();
      char ch = s.charAt(i);
      switch (ch) {
        case '{': {
          i++;
          Map<String, Object> m = new LinkedHashMap<>();
          ws();
          if (s.charAt(i) == '}') {
            i++;
            return m;
          }
          while (true) {
            ws();
            String k = string();
            ws();
            expect(':');
            m.put(k, value());
            ws();
            if (s.charAt(i) == ',') {
              i++;
              continue;
            }
            expect('}');
            return m;
          }
        }
        case '[': {
          i++;
          List<Object> l = new ArrayList<>();
          ws();
          if (s.charAt(i) == ']') {
            i++;
            return l;
          }
          while (true) {
            l.add(value());
            ws();
            if (s.charAt(i) == ',') {
              i++;
              continue;
            }
            expect(']');
            return l;
          }
        }
        case '"':
          return string();
        case 't':
          i += 4;
          return Boolean.TRUE;
        case 'f':
          i += 5;
          return Boolean.FALSE;
        case 'n':
          i += 4;
          return null;
        default: {
          int start = i;
          while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
            i++;
          }
          String num = s.substring(start, i);
          if (num.matches("-?\\d+")) {
            return Long.parseLong(num);
          }
          return Double.parseDouble(num);
        }
      }
    }

    private void expect(char c) {
      if (s.charAt(i) != c) {
        throw new IllegalArgumentException("json: expected " + c + " at " + i);
      }
      i++;
    }

    private String string() {
      expect('"');
      StringBuilder b = new StringBuilder();
      while (true) {
        char ch = s.charAt(i++);
        if (ch == '"') {
          return b.toString();
        }
        if (ch != '\\') {
          b.append(ch);
          continue;
        }
        char e = s.charAt(i++);
        switch (e) {
          case 'n' -> b.append('\n');
          case 't' -> b.append('\t');
          case 'r' -> b.append('\r');
          case 'b' -> b.append('\b');
          case 'f' -> b.append('\f');
          case 'u' -> {
            b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
            i += 4;
          }
          default -> b.append(e);
        }
      }
    }

    static String write(Object v) {
      StringBuilder b = new StringBuilder();
      write(b, v);
      return b.toString();
    }

    private static void write(StringBuilder b, Object v) {
      if (v == null) {
        b.append("null");
      } else if (v instanceof String str) {
        b.append('"');
        for (int i = 0; i < str.length(); i++) {
          char c = str.charAt(i);
          switch (c) {
            case '"' -> b.append("\\\"");
            case '\\' -> b.append("\\\\");
            case '\n' -> b.append("\\n");
            case '\r' -> b.append("\\r");
            case '\t' -> b.append("\\t");
            default -> {
              if (c < 0x20) {
                b.append(String.format("\\u%04x", (int) c));
              } else {
                b.append(c);
              }
            }
          }
        }
        b.append('"');
      } else if (v instanceof Map<?, ?> m) {
        b.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : m.entrySet()) {
          if (!first) {
            b.append(',');
          }
          first = false;
          write(b, e.getKey().toString());
          b.append(':');
          write(b, e.getValue());
        }
        b.append('}');
      } else if (v instanceof List<?> l) {
        b.append('[');
        for (int i = 0; i < l.size(); i++) {
          if (i > 0) {
            b.append(',');
          }
          write(b, l.get(i));
        }
        b.append(']');
      } else if (v instanceof Number || v instanceof Boolean) {
        b.append(v);
      } else {
        write(b, v.toString());
      }
    }
  }
}
