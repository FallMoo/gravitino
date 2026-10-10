/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.lakehouse.iceberg.managed;

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogPoller;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Integration tests for the managed write path (design doc section 8, tests 5 and 6 plus the
 * surrounding primitives).
 *
 * <p>They run a real H2 entity store, a real {@code RelationalEntityStore} (with a fault injector
 * on the table entity write) and a real {@link IcebergManagedCatalogOperations} on a local
 * warehouse, so the atomicity claim is exercised end to end instead of being mocked away.
 */
public class TestIcebergManagedCatalogOperations {

  private static final String METALAKE = "managed_metalake";
  private static final String CATALOG = "managed_catalog";
  private static final String SCHEMA = "db";

  private static Path entityDbDir;
  private static Path warehouseDir;
  private static Config config;
  private static JDBCBackend backend;
  private static FailingTableEntityStore store;
  private static String entityJdbcUrl;

  private static final class FailingTableEntityStore extends RelationalEntityStore {
    private final AtomicBoolean failTablePuts = new AtomicBoolean(false);

    private void failTablePuts(boolean fail) {
      failTablePuts.set(fail);
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
        throws IOException {
      if (e instanceof TableEntity && failTablePuts.get()) {
        throw new IOException("injected table_meta write failure");
      }
      super.put(e, overwritten);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    entityDbDir = Files.createTempDirectory("gravitino-managed-ops-entity-");
    warehouseDir = Files.createTempDirectory("gravitino-managed-ops-warehouse-");
    entityJdbcUrl =
        String.format("jdbc:h2:file:%s/entity;DB_CLOSE_DELAY=-1;MODE=MYSQL", entityDbDir);

    config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
        .thenReturn(Configs.DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(entityJdbcUrl);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("org.h2.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(20);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
        .thenReturn(10_000L);
    Mockito.when(config.get(CACHE_ENABLED)).thenReturn(false);

    SqlSessionFactoryHelper.getInstance().close();
    GravitinoEnv env = GravitinoEnv.getInstance();
    FieldUtils.writeField(env, "config", config, true);
    FieldUtils.writeField(env, "idGenerator", RandomIdGenerator.INSTANCE, true);

    backend = new JDBCBackend();
    backend.initialize(config);
    EntityIdService.initialize(new RelationalEntityStoreIdResolver());

    store = new FailingTableEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    FieldUtils.writeField(
        store, "entityChangeLogPoller", Mockito.mock(EntityChangeLogPoller.class), true);
    FieldUtils.writeField(env, "entityStore", store, true);
    FieldUtils.writeField(env, "catalogManager", Mockito.mock(CatalogManager.class), true);
    FieldUtils.writeField(
        env, "internalSchemaDispatcher", new AlwaysExistingSchemaDispatcher(), true);

    // The Iceberg V1 tables live in the same database as the Gravitino entities.
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123")) {
      connection.setAutoCommit(true);
      ManagedIcebergMetadataStore.createIcebergTablesIfNotExists(connection);
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (backend != null) {
      backend.close();
    }
    FileUtils.deleteDirectory(entityDbDir.toFile());
    FileUtils.deleteDirectory(warehouseDir.toFile());
  }

  private static IcebergManagedCatalogOperations newOps() {
    IcebergManagedCatalogOperations ops = new IcebergManagedCatalogOperations();
    ops.initialize(conf(), null, null);
    return ops;
  }

  /**
   * The managed operations as the Iceberg REST entry initializes them: with the catalog info, which
   * carries the metalake the REST identifiers are resolved against.
   */
  private static IcebergManagedCatalogOperations newOpsForRest() {
    IcebergManagedCatalogOperations ops = new IcebergManagedCatalogOperations();
    ops.initialize(conf(), catalogInfo(), null);
    return ops;
  }

  private static CatalogInfo catalogInfo() {
    return new CatalogInfo(
        1L,
        CATALOG,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        "managed test catalog",
        Collections.emptyMap(),
        null,
        Namespace.of(METALAKE));
  }

  /** A schema dispatcher that reports every schema as existing, so the entity-store focus holds. */
  private static final class AlwaysExistingSchemaDispatcher implements SchemaDispatcher {
    @Override
    public NameIdentifier[] listSchemas(Namespace namespace) {
      return new NameIdentifier[0];
    }

    @Override
    public org.apache.gravitino.Schema loadSchema(NameIdentifier ident) {
      return Mockito.mock(org.apache.gravitino.Schema.class);
    }

    @Override
    public org.apache.gravitino.Schema createSchema(
        NameIdentifier ident, String comment, Map<String, String> properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.gravitino.Schema alterSchema(NameIdentifier ident, SchemaChange... changes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropSchema(NameIdentifier ident, boolean cascade) {
      throw new UnsupportedOperationException();
    }
  }

  @BeforeEach
  public void resetState() throws Exception {
    store.failTablePuts(false);
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123");
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM iceberg_tables");
      statement.execute("DELETE FROM iceberg_namespace_properties");
      statement.execute("DELETE FROM table_meta");
      statement.execute("DELETE FROM schema_meta");
      statement.execute("DELETE FROM catalog_meta");
      statement.execute("DELETE FROM metalake_meta");
    }
    // Each test gets a clean warehouse so orphan-file assertions cannot see another test's files.
    Path warehouseTables = warehouseDir.resolve("db");
    if (Files.exists(warehouseTables)) {
      FileUtils.deleteDirectory(warehouseTables.toFile());
    }

    insertEntity(metalakeEntity());
    insertEntity(catalogEntity());
    insertEntity(schemaEntity());
  }

  private static <E extends Entity & HasIdentifier> void insertEntity(E entity) throws IOException {
    store.put(entity, false);
  }

  private static AuditInfo auditInfo() {
    return AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }

  private static BaseMetalake metalakeEntity() {
    return BaseMetalake.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(METALAKE)
        .withAuditInfo(auditInfo())
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static CatalogEntity catalogEntity() {
    return CatalogEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(CATALOG)
        .withNamespace(Namespace.of(METALAKE))
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("lakehouse-iceberg")
        .withComment("")
        .withProperties(Collections.emptyMap())
        .withAuditInfo(auditInfo())
        .build();
  }

  private static SchemaEntity schemaEntity() {
    return SchemaEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(SCHEMA)
        .withNamespace(Namespace.of(METALAKE, CATALOG))
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo())
        .build();
  }

  private static Column[] columns() {
    return new Column[] {
      Column.of("id", Types.IntegerType.get(), "id"),
      Column.of("data", Types.StringType.get(), "data")
    };
  }

  private static NameIdentifier tableIdent(String name) {
    return NameIdentifier.of(NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA), name);
  }

  private static long countIcebergRows(String tableName) throws SQLException {
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT COUNT(*) FROM iceberg_tables WHERE table_name = '%s'", tableName))) {
      return rs.next() ? rs.getLong(1) : -1L;
    }
  }

  private static long countTableMetaRows(String tableName) throws SQLException {
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT COUNT(*) FROM table_meta WHERE table_name = '%s' AND deleted_at = 0",
                    tableName))) {
      return rs.next() ? rs.getLong(1) : -1L;
    }
  }

  private static Optional<String> selectMetadataLocation(String tableName) throws SQLException {
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_name = '%s'",
                    tableName))) {
      return rs.next() ? Optional.ofNullable(rs.getString(1)) : Optional.empty();
    }
  }

  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRawSqlOnTheSessionConnectionNeedsAnExplicitResolution() throws Exception {
    // Platform behaviour that the managed write path has to work around: MyBatis only issues a
    // JDBC rollback for a session it considers dirty, and JdbcTransaction.close() resets
    // auto-commit, which makes the driver commit a statement it never tracked.
    org.apache.gravitino.storage.relational.utils.SessionUtils.beginTransaction();
    java.sql.Connection connection =
        org.apache.gravitino.storage.relational.utils.SessionUtils.currentConnection();
    insertRawRow(connection, "t_mybatis_close");
    org.apache.gravitino.storage.relational.utils.SessionUtils.rollbackTransaction();
    assertEquals(
        1L,
        countIcebergRows("t_mybatis_close"),
        "the pitfall disappeared: MyBatis now rolls back untracked statements itself");

    // Resolving the JDBC transaction explicitly gives the expected outcome.
    org.apache.gravitino.storage.relational.utils.SessionUtils.beginTransaction();
    java.sql.Connection explicit =
        org.apache.gravitino.storage.relational.utils.SessionUtils.currentConnection();
    insertRawRow(explicit, "t_explicit_rollback");
    org.apache.gravitino.storage.relational.utils.SessionUtils.rollbackTransactionAndConnection();
    assertEquals(0L, countIcebergRows("t_explicit_rollback"));

    // And the explicit commit makes both a raw statement and a nested entity write durable.
    org.apache.gravitino.storage.relational.utils.SessionUtils.beginTransaction();
    insertRawRow(
        org.apache.gravitino.storage.relational.utils.SessionUtils.currentConnection(),
        "t_explicit_commit");
    org.apache.gravitino.storage.relational.utils.SessionUtils.commitTransactionAndConnection();
    assertEquals(1L, countIcebergRows("t_explicit_commit"));
  }

  private static void insertRawRow(java.sql.Connection connection, String tableName)
      throws SQLException {
    try (java.sql.PreparedStatement ps =
        connection.prepareStatement(
            "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name,"
                + " metadata_location, previous_metadata_location, iceberg_type)"
                + " VALUES (?, 'db', ?, 'loc1', NULL, 'TABLE')")) {
      ps.setString(1, CATALOG);
      ps.setString(2, tableName);
      ps.executeUpdate();
    }
  }

  @Test
  public void testCreateTableWritesThePointerAndTheEntityTogether() throws Exception {
    IcebergManagedCatalogOperations ops = newOps();
    NameIdentifier ident = tableIdent("t_success");

    Table table =
        ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    assertNotNull(table);
    assertEquals("t_success", table.name());
    assertEquals(2, table.columns().length);

    assertEquals(1L, countIcebergRows("t_success"), "the iceberg_tables row is missing");
    assertEquals(1L, countTableMetaRows("t_success"), "the table_meta row is missing");

    String metadataLocation = selectMetadataLocation("t_success").orElseThrow();
    assertEquals(
        0,
        ManagedIcebergMetadataStore.parseVersion(metadataLocation),
        "the first metadata file must be version 0: " + metadataLocation);

    // The read path resolves the table from the metadata file.
    Table loaded = ops.loadTable(ident);
    assertEquals(2, loaded.columns().length);
    assertEquals(1, ops.listTables(NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA)).length);
    assertTrue(ops.tableExists(ident));
  }

  /**
   * Design doc test 5/6 (inverted): when the Gravitino entity write fails, managed mode must leave
   * no {@code iceberg_tables} row behind, must surface the failure, and must not leave a metadata
   * file that nobody references. This is exactly the federated defect the design removes.
   */
  @Test
  public void testCreateTableRollsTheIcebergRowBackWhenTheEntityWriteFails() throws Exception {
    IcebergManagedCatalogOperations ops = newOps();
    NameIdentifier ident = tableIdent("t_failure");

    store.failTablePuts(true);
    RuntimeException failure =
        assertThrows(
            RuntimeException.class,
            () ->
                ops.createTable(
                    ident, columns(), "spike", Collections.emptyMap(), new Transform[0]));
    assertTrue(
        String.valueOf(failure.getMessage()).contains("Failed to create table")
            || String.valueOf(failure.getCause()).contains("injected"),
        "the failure must be surfaced, got: " + failure);

    assertEquals(0L, countIcebergRows("t_failure"), "the iceberg_tables row was not rolled back");
    assertEquals(0L, countTableMetaRows("t_failure"), "the table_meta row exists");

    // The failure is honest: the table does not exist.
    assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
    assertFalse(ops.tableExists(ident));

    // No orphan metadata file (design doc section 5.7): the warehouse only has empty directories.
    try (Stream<Path> paths = Files.walk(warehouseDir)) {
      assertTrue(
          paths.noneMatch(p -> p.toString().endsWith(".metadata.json")),
          "an orphan metadata file was left behind");
    }
  }

  @Test
  public void testCommitAdvancesThePointerAndTheEntity() throws Exception {
    IcebergManagedCatalogOperations ops = newOps();
    NameIdentifier ident = tableIdent("t_commit");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base =
        TableMetadataParser.read(
            new HadoopFileIO(new Configuration()),
            selectMetadataLocation("t_commit").orElseThrow());
    Schema newSchema =
        new Schema(
            NestedField.required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()),
            NestedField.required(2, "data", org.apache.iceberg.types.Types.StringType.get()),
            NestedField.optional(3, "extra", org.apache.iceberg.types.Types.StringType.get()));
    TableMetadata updated = base.updateSchema(newSchema);

    TableMetadata committed = ops.commit(ident, base, updated);
    assertEquals(3, committed.schema().columns().size());

    String newLocation = selectMetadataLocation("t_commit").orElseThrow();
    assertEquals(
        1, ManagedIcebergMetadataStore.parseVersion(newLocation), "the version did not advance");
    assertEquals(1L, countTableMetaRows("t_commit"));

    // Committing against the stale base must fail and must not move the pointer.
    org.apache.iceberg.exceptions.CommitFailedException e =
        assertThrows(
            org.apache.iceberg.exceptions.CommitFailedException.class,
            () -> ops.commit(ident, base, updated));
    assertTrue(e.getMessage().contains("concurrently"), e.getMessage());
    assertEquals(newLocation, selectMetadataLocation("t_commit").orElseThrow());
  }

  @Test
  public void testDropAndRenameMoveBothStores() throws Exception {
    IcebergManagedCatalogOperations ops = newOps();
    NameIdentifier ident = tableIdent("t_dropme");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    assertTrue(ops.renameTable(ident, tableIdent("t_renamed")));
    assertEquals(0L, countIcebergRows("t_dropme"));
    assertEquals(1L, countIcebergRows("t_renamed"));
    assertEquals(0L, countTableMetaRows("t_dropme"));
    assertEquals(1L, countTableMetaRows("t_renamed"));

    assertTrue(ops.dropTable(tableIdent("t_renamed")));
    assertEquals(0L, countIcebergRows("t_renamed"));
    assertEquals(0L, countTableMetaRows("t_renamed"));
    assertThrows(NoSuchTableException.class, () -> ops.loadTable(tableIdent("t_renamed")));
    assertFalse(
        ops.dropTable(tableIdent("t_renamed")), "dropping a missing table must return false");
    assertNull(selectMetadataLocation("t_renamed").orElse(null));
  }

  // ---------------------------------------------------------------------------------------------
  // Commit handler (design doc sections 5.9.1 / 5.9.3 and tests 10, 11, 14)
  // ---------------------------------------------------------------------------------------------

  private static final class CountingCommitOps extends IcebergManagedCatalogOperations {
    private final AtomicInteger commitAttempts = new AtomicInteger();
    private final AtomicInteger readAttempts = new AtomicInteger();
    private final java.util.Set<String> attemptThreads =
        java.util.Collections.synchronizedSet(new java.util.LinkedHashSet<>());
    private final java.util.List<Integer> sessionCountsAtAttempt =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private final boolean failFirstCommit;
    private final boolean alwaysFail;

    private CountingCommitOps(boolean failFirstCommit, boolean alwaysFail) {
      this.failFirstCommit = failFirstCommit;
      this.alwaysFail = alwaysFail;
    }

    @Override
    public TableMetadata readMetadata(NameIdentifier ident) throws NoSuchTableException {
      readAttempts.incrementAndGet();
      return super.readMetadata(ident);
    }

    @Override
    public TableMetadata commit(NameIdentifier ident, TableMetadata base, TableMetadata updated) {
      int attempt = commitAttempts.incrementAndGet();
      attemptThreads.add(Thread.currentThread().getName());
      try {
        sessionCountsAtAttempt.add(sessionCount());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (alwaysFail) {
        throw new org.apache.iceberg.exceptions.CommitFailedException(
            "simulated permanent concurrent writer");
      }
      if (failFirstCommit && attempt == 1) {
        // Simulate another writer winning the race with an unrelated change, so the retry still has
        // real work to do and the handler has to re-read the base.
        super.commit(ident, base, base.replaceProperties(Map.of("concurrent", "writer")));
        throw new org.apache.iceberg.exceptions.CommitFailedException(
            "simulated concurrent writer");
      }
      return super.commit(ident, base, updated);
    }
  }

  private static <T extends IcebergManagedCatalogOperations> T newOpsOfType(
      java.util.function.Supplier<T> supplier) {
    T ops = supplier.get();
    ops.initialize(conf(), null, null);
    return ops;
  }

  private static Map<String, String> conf() {
    return ImmutableMap.<String, String>builder()
        .put(IcebergConstants.CATALOG_BACKEND_MODE, "managed")
        .put(IcebergConstants.CATALOG_BACKEND, "jdbc")
        .put(IcebergConstants.WAREHOUSE, warehouseDir.toUri().toString())
        .put(IcebergConstants.CATALOG_BACKEND_NAME, CATALOG)
        .build();
  }

  private static org.apache.iceberg.rest.requests.UpdateTableRequest setPropertyRequest(
      NameIdentifier ident, TableMetadata base, String key, String value) {
    return org.apache.iceberg.rest.requests.UpdateTableRequest.create(
        org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper
            .buildIcebergTableIdentifier(ident),
        java.util.List.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(base.uuid())),
        java.util.List.of(new org.apache.iceberg.MetadataUpdate.SetProperties(Map.of(key, value))));
  }

  @Test
  public void testCommitHandlerRetriesAfterAConcurrentWriter() throws Exception {
    CountingCommitOps ops = newOpsOfType(() -> new CountingCommitOps(true, false));
    NameIdentifier ident = tableIdent("t_retry");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base = ops.readMetadata(ident);
    TableMetadata result =
        new ManagedIcebergCommitHandler(ops)
            .commit(ident, setPropertyRequest(ident, base, "retried", "true"));
    // Snapshot the counter before the assertions below read the metadata again.
    int readsByHandler = ops.readAttempts.get() - 1 /* the read above */;

    assertEquals(2, readsByHandler, "the handler must re-read the base on retry");
    assertEquals(2, ops.commitAttempts.get(), "the handler must retry exactly once");
    assertEquals("true", result.properties().get("retried"), "the retry lost the update");
    assertEquals(
        "writer",
        result.properties().get("concurrent"),
        "the retry must build on the winner's metadata");
    assertEquals(
        "true",
        ops.readMetadata(ident).properties().get("retried"),
        "the committed metadata is missing the retried property");
    assertEquals(1L, countTableMetaRows("t_retry"), "table_meta disappeared");
  }

  @Test
  public void testCommitHandlerGivesUpAfterRetries() throws Exception {
    CountingCommitOps ops = newOpsOfType(() -> new CountingCommitOps(false, true));
    NameIdentifier ident = tableIdent("t_exhausted");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base = ops.readMetadata(ident);
    org.apache.iceberg.rest.requests.UpdateTableRequest request =
        setPropertyRequest(ident, base, "exhausted", "true");

    assertThrows(
        org.apache.iceberg.exceptions.CommitFailedException.class,
        () -> new ManagedIcebergCommitHandler(ops).commit(ident, request));
    assertEquals(5, ops.commitAttempts.get(), "expected 1 attempt plus 4 retries");
    assertFalse(
        ops.readMetadata(ident).properties().containsKey("exhausted"),
        "a failed commit must not move the pointer");
  }

  @Test
  public void testCommitHandlerDoesNotRetryAValidationFailure() throws Exception {
    CountingCommitOps ops = newOpsOfType(() -> new CountingCommitOps(false, false));
    NameIdentifier ident = tableIdent("t_validation");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base = ops.readMetadata(ident);
    org.apache.iceberg.rest.requests.UpdateTableRequest request =
        org.apache.iceberg.rest.requests.UpdateTableRequest.create(
            org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper
                .buildIcebergTableIdentifier(ident),
            java.util.List.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID("not-the-uuid")),
            java.util.List.of(
                new org.apache.iceberg.MetadataUpdate.SetProperties(Map.of("k", "v"))));

    assertThrows(
        ManagedIcebergCommitHandler.ValidationFailure.class,
        () -> new ManagedIcebergCommitHandler(ops).commit(ident, request));
    assertEquals(0, ops.commitAttempts.get(), "a validation failure must not be retried");
    assertEquals(
        base.metadataFileLocation(),
        ops.readMetadata(ident).metadataFileLocation(),
        "a validation failure must not write a new metadata file");
  }

  @Test
  public void testDataOnlyCommitLeavesTheEntityUntouched() throws Exception {
    IcebergManagedCatalogOperations ops = newOps();
    NameIdentifier ident = tableIdent("t_data_only");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base = ops.readMetadata(ident);
    ops.commitDataOnly(ident, base, base.replaceProperties(Map.of("data.only", "true")));

    TableEntity afterDataOnly = store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
    assertFalse(
        afterDataOnly.properties().containsKey("data.only"),
        "a data-only commit must not synchronize table_meta");

    TableMetadata second = ops.readMetadata(ident);
    ops.commit(ident, second, second.replaceProperties(Map.of("structure.change", "true")));
    assertTrue(
        store
            .get(ident, Entity.EntityType.TABLE, TableEntity.class)
            .properties()
            .containsKey("structure.change"),
        "a structure/property commit must synchronize table_meta");
  }

  // ---------------------------------------------------------------------------------------------
  // Iceberg REST entry surface (design doc sections 5.9.1 and 5.10, tests 7 / 9 / 13)
  // ---------------------------------------------------------------------------------------------

  private static org.apache.iceberg.rest.requests.CreateTableRequest restCreateRequest(
      String name, boolean stageCreate) {
    org.apache.iceberg.rest.requests.CreateTableRequest.Builder builder =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(name)
            .withSchema(
                new org.apache.iceberg.Schema(
                    org.apache.iceberg.types.Types.NestedField.required(
                        1, "id", org.apache.iceberg.types.Types.IntegerType.get()),
                    org.apache.iceberg.types.Types.NestedField.optional(
                        2, "name", org.apache.iceberg.types.Types.StringType.get())))
            .withPartitionSpec(org.apache.iceberg.PartitionSpec.unpartitioned())
            .withWriteOrder(org.apache.iceberg.SortOrder.unsorted())
            // A mutable map: Iceberg's builder probes it with a null key, which Map.of rejects.
            .setProperties(new java.util.HashMap<>(Map.of("owner", "spike")));
    if (stageCreate) {
      builder.stageCreate();
    }
    return builder.build();
  }

  @Test
  public void testRestCreateLoadListAndMetadataLocation() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of(SCHEMA);
    org.apache.iceberg.catalog.TableIdentifier identifier =
        org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_rest");

    org.apache.iceberg.rest.responses.LoadTableResponse created =
        ops.createTable(namespace, restCreateRequest("t_rest", false));
    assertEquals(2, created.tableMetadata().schema().columns().size());
    assertTrue(ops.tableExists(identifier));
    assertEquals(1L, countIcebergRows("t_rest"));
    assertEquals(1L, countTableMetaRows("t_rest"));
    assertTrue(
        Files.exists(
            Paths.get(java.net.URI.create(created.tableMetadata().metadataFileLocation()))),
        "the metadata file is missing");

    org.apache.iceberg.rest.responses.LoadTableResponse loaded = ops.loadTable(identifier);
    assertEquals(
        created.tableMetadata().metadataFileLocation(),
        loaded.tableMetadata().metadataFileLocation());

    org.apache.iceberg.rest.responses.ListTablesResponse listed = ops.listTables(namespace);
    assertEquals(
        java.util.List.of(identifier),
        listed.identifiers().stream()
            .filter(t -> t.name().equals("t_rest"))
            .collect(java.util.stream.Collectors.toList()));
    assertEquals(
        Optional.of(created.tableMetadata().metadataFileLocation()),
        ops.metadataLocation(identifier));
    assertEquals(
        Optional.empty(),
        ops.metadataLocation(
            org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_missing")));
  }

  @Test
  public void testRestStageCreateWritesNeitherRowNorEntity() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of(SCHEMA);

    org.apache.iceberg.rest.responses.LoadTableResponse staged =
        ops.createTable(namespace, restCreateRequest("t_staged", true));

    assertNotNull(staged.tableMetadata(), "a staged create still returns metadata");
    assertFalse(
        ops.tableExists(org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_staged")));
    assertEquals(0L, countIcebergRows("t_staged"), "a staged create must not write a row");
    assertEquals(0L, countTableMetaRows("t_staged"), "a staged create must not write an entity");
    assertNull(
        staged.tableMetadata().metadataFileLocation(),
        "a staged create must not write a metadata file");
    assertFalse(
        Files.exists(Paths.get(java.net.URI.create(staged.tableMetadata().location()))),
        "a staged create must not create the table directory");
  }

  @Test
  public void testRestUpdateTableCommitsThroughTheHandler() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of(SCHEMA);
    org.apache.iceberg.catalog.TableIdentifier identifier =
        org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_rest_update");
    ops.createTable(namespace, restCreateRequest("t_rest_update", false));

    org.apache.iceberg.TableMetadata base = ops.loadTable(identifier).tableMetadata();
    org.apache.iceberg.rest.requests.UpdateTableRequest request =
        org.apache.iceberg.rest.requests.UpdateTableRequest.create(
            identifier,
            java.util.List.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(base.uuid())),
            java.util.List.of(
                new org.apache.iceberg.MetadataUpdate.SetProperties(Map.of("rest", "true"))));

    org.apache.iceberg.rest.responses.LoadTableResponse updated =
        ops.updateTable(identifier, request);
    assertEquals("true", updated.tableMetadata().properties().get("rest"));
    assertTrue(
        store
            .get(tableIdent("t_rest_update"), Entity.EntityType.TABLE, TableEntity.class)
            .properties()
            .containsKey("rest"),
        "the REST commit did not synchronize table_meta");
    assertEquals(
        updated.tableMetadata().metadataFileLocation(),
        ops.metadataLocation(identifier).orElseThrow());
  }

  @Test
  public void testRestRegisterRenameAndDrop() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of(SCHEMA);
    org.apache.iceberg.catalog.TableIdentifier identifier =
        org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_rest_reg");

    // Create a table to obtain a real metadata file, then register that file under a new name.
    ops.createTable(namespace, restCreateRequest("t_rest_source", false));
    String metadataLocation =
        ops.metadataLocation(
                org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_rest_source"))
            .orElseThrow();

    org.apache.iceberg.rest.responses.LoadTableResponse registered =
        ops.registerTable(
            namespace,
            org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest.builder()
                .name("t_rest_reg")
                .metadataLocation(metadataLocation)
                .build());
    assertEquals(2, registered.tableMetadata().schema().columns().size());
    assertTrue(ops.tableExists(identifier));

    org.apache.iceberg.catalog.TableIdentifier renamed =
        org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_rest_renamed");
    ops.renameTable(identifier, renamed);
    assertFalse(ops.tableExists(identifier));
    assertTrue(ops.tableExists(renamed));

    ops.dropTable(renamed, false);
    assertFalse(ops.tableExists(renamed));
    assertEquals(0L, countIcebergRows("t_rest_renamed"));
  }

  // ---------------------------------------------------------------------------------------------
  // Namespace operations (design doc section 5.11)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testCreateLoadListAndDropNamespace() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of("ns_a");

    org.apache.iceberg.rest.responses.CreateNamespaceResponse created =
        ops.createNamespace(
            org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
                .withNamespace(namespace)
                .setProperties(new java.util.HashMap<>(Map.of("owner", "spike")))
                .build());
    assertEquals(namespace, created.namespace());
    assertEquals("spike", created.properties().get("owner"));
    assertTrue(created.properties().containsKey("location"), "a location must always be stored");

    assertTrue(ops.namespaceExists(namespace));
    assertEquals(
        created.properties(),
        ops.loadNamespace(namespace).properties(),
        "loadNamespace must return the stored properties");

    // The Gravitino schema entity is written in the same transaction.
    NameIdentifier schemaIdent = NameIdentifier.of(Namespace.of(METALAKE, CATALOG), "ns_a");
    assertNotNull(store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class));
    assertEquals(2L, countNamespacePropertyRows("ns_a"), "two properties must be stored");

    // A namespace created with no properties still exists, because a location is injected.
    org.apache.iceberg.catalog.Namespace empty =
        org.apache.iceberg.catalog.Namespace.of("ns_empty");
    ops.createNamespace(
        org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
            .withNamespace(empty)
            .build());
    assertTrue(ops.namespaceExists(empty));

    assertEquals(
        java.util.List.of(namespace, empty),
        ops.listNamespaces(org.apache.iceberg.catalog.Namespace.empty()).namespaces());

    ops.dropNamespace(namespace);
    assertFalse(ops.namespaceExists(namespace));
    assertEquals(0L, countNamespacePropertyRows("ns_a"));
    assertThrows(
        NoSuchEntityException.class,
        () -> store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  @Test
  public void testDropNamespaceRejectsANonEmptyNamespace() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of("ns_full");
    ops.createNamespace(
        org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
            .withNamespace(namespace)
            .build());
    ops.createTable(namespace, restCreateRequest("t_in_ns", false));

    assertThrows(
        org.apache.iceberg.exceptions.NamespaceNotEmptyException.class,
        () -> ops.dropNamespace(namespace));
    assertTrue(ops.namespaceExists(namespace), "a failed drop must not delete the namespace");
  }

  @Test
  public void testUpdateNamespaceProperties() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of("ns_props");
    ops.createNamespace(
        org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
            .withNamespace(namespace)
            .setProperties(new java.util.HashMap<>(Map.of("owner", "spike")))
            .build());

    org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest request =
        updateNamespaceRequest(java.util.List.of("owner"), Map.of("team", "lakehouse"));

    org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse response =
        ops.updateNamespaceProperties(namespace, request);
    assertEquals(java.util.List.of("owner"), response.removed());
    assertTrue(response.updated().contains("team"));

    Map<String, String> stored = ops.loadNamespace(namespace).properties();
    assertFalse(stored.containsKey("owner"));
    assertEquals("lakehouse", stored.get("team"));
    // The Gravitino schema entity keeps the same property view.
    assertEquals(
        stored,
        store
            .get(
                NameIdentifier.of(Namespace.of(METALAKE, CATALOG), "ns_props"),
                Entity.EntityType.SCHEMA,
                SchemaEntity.class)
            .properties());
  }

  private static org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest
      updateNamespaceRequest(java.util.List<String> removals, Map<String, String> updates) {
    return org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest.builder()
        // Mutable collections: Iceberg's builder probes them with a null key, which immutable
        // collections reject.
        .removeAll(new java.util.ArrayList<>(removals))
        .updateAll(new java.util.HashMap<>(updates))
        .build();
  }

  private static long countNamespacePropertyRows(String namespace) throws SQLException {
    try (Connection connection = DriverManager.getConnection(entityJdbcUrl, "root", "123");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT COUNT(*) FROM iceberg_namespace_properties WHERE namespace = '%s'",
                    namespace))) {
      return rs.next() ? rs.getLong(1) : -1L;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Cross-entry compatibility and retry thread binding (design tests 7 and 18)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNativeIcebergJdbcCatalogReadsWhatManagedModeWrote() throws Exception {
    IcebergManagedCatalogOperations ops = newOpsForRest();
    org.apache.iceberg.catalog.Namespace namespace =
        org.apache.iceberg.catalog.Namespace.of(SCHEMA);
    ops.createTable(namespace, restCreateRequest("t_roundtrip", false));

    // A native Iceberg JdbcCatalog pointed at the same database must see exactly the table managed
    // mode wrote: this is the round-trip guarantee of the design (test 7).
    Map<String, String> nativeProperties = new java.util.HashMap<>();
    nativeProperties.put("uri", entityJdbcUrl);
    nativeProperties.put("warehouse", warehouseDir.toUri().toString());
    nativeProperties.put("jdbc.user", "root");
    nativeProperties.put("jdbc.password", "123");
    org.apache.iceberg.catalog.Catalog nativeCatalog =
        org.apache.iceberg.CatalogUtil.loadCatalog(
            "org.apache.iceberg.jdbc.JdbcCatalog", CATALOG, nativeProperties, null);
    // JdbcCatalog implements AutoCloseable; the Catalog interface does not expose it.
    try {
      org.apache.iceberg.catalog.TableIdentifier identifier =
          org.apache.iceberg.catalog.TableIdentifier.of(namespace, "t_roundtrip");
      assertTrue(nativeCatalog.tableExists(identifier));
      org.apache.iceberg.Table nativeTable = nativeCatalog.loadTable(identifier);
      assertEquals(2, nativeTable.schema().columns().size());
      assertEquals("spike", nativeTable.properties().get("owner"));
      assertEquals(
          ops.metadataLocation(identifier).orElseThrow(),
          ((org.apache.iceberg.BaseTable) nativeTable)
              .operations()
              .current()
              .metadataFileLocation(),
          "the native catalog must resolve the same metadata file");
    } finally {
      ((AutoCloseable) nativeCatalog).close();
    }
  }

  @Test
  public void testCommitRetriesKeepTheTransactionOffAnyThreadPool() throws Exception {
    CountingCommitOps ops = newOpsOfType(() -> new CountingCommitOps(true, false));
    NameIdentifier ident = tableIdent("t_thread_binding");
    ops.createTable(ident, columns(), "spike", Collections.emptyMap(), new Transform[0]);

    TableMetadata base = ops.readMetadata(ident);
    String callerThread = Thread.currentThread().getName();
    assertEquals(0, sessionCount(), "no transaction may be bound before the commit");

    new ManagedIcebergCommitHandler(ops)
        .commit(ident, setPropertyRequest(ident, base, "thread", "bound"));

    // R13: Tasks.foreach(...) without executeWith(...) must run every attempt on the calling
    // thread,
    // and the transaction must be released between attempts.
    assertEquals(
        java.util.Set.of(callerThread),
        ops.attemptThreads,
        "a commit retry escaped to another thread");
    assertTrue(ops.sessionCountsAtAttempt.stream().allMatch(count -> count <= 1));
    assertEquals(0, sessionCount(), "the retry loop leaked a bound transaction");
  }

  /** Reads the reference-counted session count of the Gravitino transaction thread-local. */
  private static int sessionCount() throws Exception {
    java.lang.reflect.Method method =
        Class.forName("org.apache.gravitino.storage.relational.session.SqlSessions")
            .getDeclaredMethod("getSessionCount");
    method.setAccessible(true);
    return (int) method.invoke(null);
  }
}
