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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.ManagedIcebergCatalogOps;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The managed-mode implementation layer (design doc sections 5.1, 5.7 and 5.8).
 *
 * <p>Gravitino orchestrates every operation: the Iceberg SDK is only used to build and evolve
 * {@link TableMetadata}, the metadata JSON is written with a public {@code FileIO}, and all SQL -
 * the {@code iceberg_tables} pointer as well as the Gravitino entity - runs on the Gravitino entity
 * store connection inside one transaction. The Iceberg SDK never opens a database connection.
 */
public class IcebergManagedCatalogOperations extends ManagedTableOperations
    implements CatalogOperations, ManagedIcebergCatalogOps {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergManagedCatalogOperations.class);

  /** SQL state class for integrity constraint violations. */
  private static final String SQL_STATE_INTEGRITY_CONSTRAINT = "23";

  private String metalakeName;
  private String catalogName;
  private String warehouse;
  private ManagedIcebergMetadataStore metadataStore;
  private FileIO fileIO;
  private ManagedIcebergCommitHandler commitHandler;

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    // Re-run the fail-fast preconditions: the catalog-level check happens while the wrapper is
    // built, this one guarantees the operations themselves never start in a broken environment.
    IcebergManagedCatalogPrerequisites.validate(conf);

    this.catalogName = conf.get(IcebergConstants.CATALOG_BACKEND_NAME);
    this.warehouse = conf.get(IcebergConstants.WAREHOUSE);
    this.metadataStore = new ManagedIcebergMetadataStore(catalogName);
    // The Iceberg REST entry addresses tables through the catalog's metalake; CatalogInfo carries
    // it as the catalog namespace.
    this.metalakeName =
        info == null || info.namespace() == null || info.namespace().length() == 0
            ? null
            : info.namespace().level(0);
    this.commitHandler = new ManagedIcebergCommitHandler(this);

    Map<String, String> icebergProperties = new IcebergConfig(conf).getIcebergCatalogProperties();
    String ioImpl = conf.get(IcebergConstants.IO_IMPL);
    if (StringUtils.isBlank(ioImpl)) {
      ioImpl = ResolvingFileIO.class.getName();
    }
    this.fileIO = CatalogUtil.loadFileIO(ioImpl, icebergProperties, null);
    LOG.info("Initialized managed Iceberg catalog operations for catalog {}", catalogName);
  }

  @Override
  public void close() {
    if (fileIO != null) {
      try {
        fileIO.close();
      } catch (Exception e) {
        LOG.warn("Failed to close the FileIO of managed Iceberg catalog {}", catalogName, e);
      }
    }
  }

  @Override
  protected EntityStore store() {
    return GravitinoEnv.getInstance().entityStore();
  }

  @Override
  protected SupportsSchemas schemas() {
    return GravitinoEnv.getInstance().internalSchemaDispatcher();
  }

  @Override
  protected IdGenerator idGenerator() {
    return GravitinoEnv.getInstance().idGenerator();
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      org.apache.gravitino.Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    // Managed mode owns its connection: a successful probe of the Iceberg V1 tables proves both the
    // entity store connection and the fixed schema are usable.
    try {
      runInTransaction(
          connection -> {
            IcebergManagedCatalogPrerequisites.checkIcebergTablesAccessible(connection);
            return null;
          });
    } catch (RuntimeException e) {
      throw new org.apache.gravitino.exceptions.ConnectionFailedException(
          e,
          "Failed to reach the Iceberg V1 tables in the Gravitino entity store: %s",
          e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Read path (design doc section 5.10)
  // ---------------------------------------------------------------------------------------------

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    Preconditions.checkArgument(namespace != null, "namespace must not be null");
    String tableNamespace = schemaNameToTableNamespace(namespace.level(namespace.length() - 1));
    List<String> tableNames =
        runInTransaction(connection -> metadataStore.listTableNames(connection, tableNamespace));
    return tableNames.stream()
        .map(name -> NameIdentifier.of(namespace, name))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    return IcebergTable.fromIcebergTable(readMetadata(ident), ident.name());
  }

  // ---------------------------------------------------------------------------------------------
  // Iceberg REST entry surface (design doc sections 5.9.1 and 5.10)
  // ---------------------------------------------------------------------------------------------

  @Override
  public LoadTableResponse createTable(
      org.apache.iceberg.catalog.Namespace namespace, CreateTableRequest request) {
    NameIdentifier ident = toNameIdentifier(TableIdentifier.of(namespace, request.name()));
    String location =
        StringUtils.isNotBlank(request.location())
            ? request.location()
            : defaultTableLocation(ident);
    // CreateTableRequest leaves spec/writeOrder null when the client did not ask for any.
    org.apache.iceberg.PartitionSpec spec =
        request.spec() == null ? org.apache.iceberg.PartitionSpec.unpartitioned() : request.spec();
    org.apache.iceberg.SortOrder writeOrder =
        request.writeOrder() == null
            ? org.apache.iceberg.SortOrder.unsorted()
            : request.writeOrder();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(), spec, writeOrder, location, request.properties());

    if (request.stageCreate()) {
      // A staged create writes neither a row nor an entity: the client is expected to follow up
      // with an isCreate commit (design doc section 5.9.1).
      return LoadTableResponse.builder().withTableMetadata(metadata).build();
    }

    createTableFromMetadata(ident, metadata, metadata.properties());
    return LoadTableResponse.builder().withTableMetadata(readMetadata(ident)).build();
  }

  @Override
  public LoadTableResponse loadTable(TableIdentifier identifier) {
    return LoadTableResponse.builder()
        .withTableMetadata(readMetadata(toNameIdentifier(identifier)))
        .build();
  }

  @Override
  public LoadTableResponse updateTable(TableIdentifier identifier, UpdateTableRequest request) {
    return LoadTableResponse.builder()
        .withTableMetadata(commitHandler.commit(toNameIdentifier(identifier), request))
        .build();
  }

  @Override
  public LoadTableResponse registerTable(
      org.apache.iceberg.catalog.Namespace namespace, RegisterTableRequest request) {
    NameIdentifier ident = toNameIdentifier(TableIdentifier.of(namespace, request.name()));
    registerTable(ident, request.metadataLocation());
    return LoadTableResponse.builder().withTableMetadata(readMetadata(ident)).build();
  }

  @Override
  public ListTablesResponse listTables(org.apache.iceberg.catalog.Namespace namespace) {
    String schemaName =
        IcebergIdentifierUtils.icebergNamespaceToSchemaName(
            namespace, HierarchicalSchemaUtil.schemaSeparator());
    NameIdentifier[] identifiers = listTables(Namespace.of(metalakeName, catalogName, schemaName));
    ListTablesResponse.Builder builder = ListTablesResponse.builder();
    for (NameIdentifier identifier : identifiers) {
      builder.add(IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(identifier));
    }
    return builder.build();
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return tableExists(toNameIdentifier(identifier));
  }

  @Override
  public void dropTable(TableIdentifier identifier, boolean purge) {
    NameIdentifier ident = toNameIdentifier(identifier);
    if (purge) {
      purgeTable(ident);
    } else {
      dropTable(ident);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    renameTable(toNameIdentifier(from), toNameIdentifier(to));
  }

  @Override
  public Optional<String> metadataLocation(TableIdentifier identifier) {
    NameIdentifier ident = toNameIdentifier(identifier);
    if (!tableExists(ident)) {
      return Optional.empty();
    }
    return Optional.of(selectTableRowOrThrow(ident).metadataLocation());
  }

  private NameIdentifier toNameIdentifier(TableIdentifier identifier) {
    Preconditions.checkArgument(
        metalakeName != null,
        "The managed Iceberg catalog operations were not initialized with a catalog info carrying"
            + " the metalake name.");
    return IcebergIdentifierUtils.toGravitinoTableIdentifier(
        metalakeName, catalogName, identifier, HierarchicalSchemaUtil.schemaSeparator());
  }

  // ---------------------------------------------------------------------------------------------
  // Write primitives (design doc section 5.8)
  // ---------------------------------------------------------------------------------------------

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(
        indexes == null || indexes.length == 0, "Iceberg catalog does not support indexes");

    NameIdentifier schemaIdent = schemaIdentifierOf(ident);
    if (!schemas().schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }

    Map<String, String> effectiveProperties = ensureStringIdentifier(properties);

    IcebergTable newTable =
        IcebergTable.builder()
            .withName(ident.name())
            .withColumns(toIcebergColumns(columns))
            .withComment(comment)
            .withPartitioning(partitioning == null ? new Transform[0] : partitioning)
            .withSortOrders(sortOrders == null ? new SortOrder[0] : sortOrders)
            .withProperties(effectiveProperties)
            .withDistribution(
                distribution == null || Distributions.NONE.equals(distribution)
                    ? defaultDistribution(partitioning, sortOrders)
                    : distribution)
            .withAuditInfo(currentAuditInfo())
            .build();

    CreateTableRequest request = newTable.toCreateTableRequest();
    String location =
        StringUtils.isNotBlank(request.location())
            ? request.location()
            : defaultTableLocation(ident);

    // CreateTableRequest leaves spec/writeOrder null when the caller did not ask for any.
    org.apache.iceberg.PartitionSpec spec =
        request.spec() == null ? org.apache.iceberg.PartitionSpec.unpartitioned() : request.spec();
    org.apache.iceberg.SortOrder writeOrder =
        request.writeOrder() == null
            ? org.apache.iceberg.SortOrder.unsorted()
            : request.writeOrder();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(), spec, writeOrder, location, request.properties());

    return persistNewTable(
        ident,
        metadata,
        columns,
        comment,
        effectiveProperties,
        partitioning,
        distribution,
        sortOrders,
        indexes);
  }

  /**
   * Persists an already built {@link TableMetadata} as a brand new managed table (used by the
   * {@code isCreate} commit branch, where the metadata comes from applying the REST updates to an
   * empty base). Writes metadata version 0 and stores the Iceberg row and the Gravitino entity in
   * one transaction.
   *
   * @param ident the table identifier
   * @param metadata the metadata to persist
   * @param properties the Gravitino properties (a string identifier is injected when missing)
   * @return the created table
   * @throws TableAlreadyExistsException if the table already exists
   */
  public Table createTableFromMetadata(
      NameIdentifier ident, TableMetadata metadata, Map<String, String> properties)
      throws TableAlreadyExistsException {
    NameIdentifier schemaIdent = schemaIdentifierOf(ident);
    if (!schemas().schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }
    IcebergTable table = IcebergTable.fromIcebergTable(metadata, ident.name());
    return persistNewTable(
        ident,
        metadata,
        table.columns(),
        table.comment(),
        ensureStringIdentifier(properties),
        table.partitioning(),
        table.distribution(),
        table.sortOrder(),
        new Index[0]);
  }

  private Table persistNewTable(
      NameIdentifier ident,
      TableMetadata metadata,
      Column[] columns,
      String comment,
      Map<String, String> effectiveProperties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    // Iceberg numbers the very first metadata file as version 0; later commits advance from the
    // version parsed out of the current metadata file name (design doc section 5.5).
    ManagedIcebergMetadataStore.MetadataWriteResult write =
        metadataStore.writeMetadata(fileIO, metadata, 0);
    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));

    boolean persisted = false;
    try {
      runInWriteTransaction(
          connection -> {
            metadataStore.insertTable(connection, tableNamespace, ident.name(), write.location());
            // Nested entity write: SessionUtils defers the commit to the outer boundary, so the
            // iceberg_tables row and table_meta land together.
            super.createTable(
                ident,
                columns,
                comment,
                effectiveProperties,
                partitioning,
                distribution,
                sortOrders,
                indexes);
            return null;
          });
      persisted = true;
    } catch (SQLException e) {
      if (isIntegrityConstraintViolation(e)) {
        throw new TableAlreadyExistsException(e, "Table %s already exists", ident);
      }
      throw new RuntimeException("Failed to write the Iceberg table row for " + ident, e);
    } finally {
      // Only clean up a file this call really created (design doc section 5.7).
      if (!persisted && write.written()) {
        metadataStore.deleteMetadataFileQuietly(fileIO, write.location());
      }
    }

    return IcebergTable.fromIcebergTable(
        TableMetadataParser.read(fileIO, write.location()), ident.name());
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // The shared primitive is `commit` below; translating Gravitino's TableChange[] onto it is the
    // Gravitino-native entry work (design doc sections 5.8.2 and 5.9.2), which is deliberately left
    // to the native-entry phase.
    throw new UnsupportedOperationException(
        "Altering a managed Iceberg table through the Gravitino native API is not implemented yet;"
            + " use the Iceberg REST endpoint.");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    return dropTableRowAndEntity(ident, false /* purge */);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dropTableRowAndEntity(ident, true /* purge */);
  }

  /**
   * Renames a table row and its Gravitino entity in one transaction (design doc section 5.8.4).
   *
   * @param from the current identifier
   * @param to the target identifier
   * @return {@code true} when the table was renamed
   * @throws NoSuchTableException if the source table does not exist
   * @throws NoSuchSchemaException if the target schema is missing
   * @throws TableAlreadyExistsException if the target table already exists
   */
  public boolean renameTable(NameIdentifier from, NameIdentifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    NameIdentifier toSchema = schemaIdentifierOf(to);
    if (!schemas().schemaExists(toSchema)) {
      throw new NoSuchSchemaException("Schema %s does not exist", toSchema);
    }

    String fromNamespace =
        schemaNameToTableNamespace(from.namespace().level(from.namespace().length() - 1));
    String toNamespace =
        schemaNameToTableNamespace(to.namespace().level(to.namespace().length() - 1));

    return runWriteOrFail(
        connection -> {
          if (metadataStore.tableExists(connection, toNamespace, to.name())) {
            throw new TableAlreadyExistsException("Table %s already exists", to);
          }
          int updated =
              metadataStore.renameTable(
                  connection, fromNamespace, from.name(), toNamespace, to.name());
          if (updated == 0) {
            throw new NoSuchTableException("Table %s does not exist", from);
          }
          updateTableEntity(
              from,
              entity ->
                  TableEntity.builder()
                      .withId(entity.id())
                      .withName(to.name())
                      .withNamespace(to.namespace())
                      .withComment(entity.comment())
                      .withColumns(entity.columns())
                      .withProperties(entity.properties())
                      .withAuditInfo(entity.auditInfo())
                      .withPartitioning(entity.partitioning())
                      .withDistribution(entity.distribution())
                      .withSortOrders(entity.sortOrders())
                      .withIndexes(entity.indexes())
                      .build());
          return true;
        });
  }

  /**
   * Registers an existing metadata file without writing a new one (design doc section 5.8.5).
   *
   * @param ident the table identifier
   * @param metadataLocation the existing metadata file to register
   * @return the registered table
   * @throws TableAlreadyExistsException if the table already exists
   */
  public Table registerTable(NameIdentifier ident, String metadataLocation)
      throws TableAlreadyExistsException {
    // Reading validates the Iceberg metadata format and version.
    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
    IcebergTable registered = IcebergTable.fromIcebergTable(metadata, ident.name());

    Map<String, String> finalProperties = ensureStringIdentifier(metadata.properties());

    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));
    try {
      runInWriteTransaction(
          connection -> {
            metadataStore.insertTable(connection, tableNamespace, ident.name(), metadataLocation);
            super.createTable(
                ident,
                registered.columns(),
                registered.comment(),
                finalProperties,
                registered.partitioning(),
                registered.distribution(),
                registered.sortOrder(),
                new Index[0]);
            return null;
          });
    } catch (SQLException e) {
      if (isIntegrityConstraintViolation(e)) {
        throw new TableAlreadyExistsException(e, "Table %s already exists", ident);
      }
      throw new RuntimeException("Failed to register the Iceberg table row for " + ident, e);
    }

    // registerTable reuses an existing file, so there is never a newly written file to clean up.
    return registered;
  }

  /**
   * The shared alter/commit primitive (design doc section 5.8.2): write the new metadata file, then
   * update the Iceberg pointer and the Gravitino entity in one transaction.
   *
   * @param ident the table identifier
   * @param base the metadata the caller based its update on
   * @param updated the new metadata
   * @return the committed table
   * @throws NoSuchTableException if the table does not exist
   * @throws CommitFailedException if the metadata pointer changed concurrently
   */
  public TableMetadata commit(NameIdentifier ident, TableMetadata base, TableMetadata updated)
      throws NoSuchTableException, CommitFailedException {
    return commitInternal(ident, base, updated, true /* syncEntity */);
  }

  /**
   * The data-only commit (design doc section 5.9.3): updates that only advance the data version
   * (snapshots, statistics, encryption keys) move the {@code iceberg_tables} pointer without
   * touching {@code table_meta}.
   *
   * @param ident the table identifier
   * @param base the metadata the caller based its update on
   * @param updated the new metadata
   * @return the committed metadata
   * @throws NoSuchTableException if the table does not exist
   * @throws CommitFailedException if the metadata pointer changed concurrently
   */
  public TableMetadata commitDataOnly(
      NameIdentifier ident, TableMetadata base, TableMetadata updated)
      throws NoSuchTableException, CommitFailedException {
    return commitInternal(ident, base, updated, false /* syncEntity */);
  }

  /**
   * Reads the current metadata of a managed table.
   *
   * @param ident the table identifier
   * @return the metadata stored at the current {@code iceberg_tables} pointer
   * @throws NoSuchTableException if the table does not exist
   */
  public TableMetadata readMetadata(NameIdentifier ident) throws NoSuchTableException {
    return TableMetadataParser.read(fileIO, selectTableRowOrThrow(ident).metadataLocation());
  }

  private TableMetadata commitInternal(
      NameIdentifier ident, TableMetadata base, TableMetadata updated, boolean syncEntity)
      throws NoSuchTableException, CommitFailedException {
    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));
    String expectedLocation = base.metadataFileLocation();
    int version = ManagedIcebergMetadataStore.parseVersion(expectedLocation) + 1;
    ManagedIcebergMetadataStore.MetadataWriteResult write =
        metadataStore.writeMetadata(fileIO, updated, version);

    boolean persisted = false;
    try {
      runInWriteTransaction(
          connection -> {
            // Layer 1: fail before attempting the UPDATE when the row is already stale (the same
            // pre-check Iceberg's JdbcTableOperations performs).
            ManagedIcebergMetadataStore.TableRow row =
                metadataStore
                    .selectTable(connection, tableNamespace, ident.name())
                    .orElseThrow(() -> new NoSuchTableException("Table %s does not exist", ident));
            if (!expectedLocation.equals(row.metadataLocation())) {
              throw new CommitFailedException(
                  "Failed to commit %s: the metadata location changed concurrently (expected %s,"
                      + " found %s).",
                  ident, expectedLocation, row.metadataLocation());
            }

            // Layer 2: the conditional UPDATE; 0 rows means a concurrent writer won the race.
            metadataStore.commitTableLocation(
                connection, tableNamespace, ident.name(), expectedLocation, write.location());
            if (syncEntity) {
              updateTableEntity(ident, entity -> toTableEntity(entity, ident, updated));
            }
            return null;
          });
      persisted = true;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to commit the Iceberg table row for " + ident, e);
    } finally {
      if (!persisted && write.written()) {
        metadataStore.deleteMetadataFileQuietly(fileIO, write.location());
      }
    }

    return TableMetadataParser.read(fileIO, write.location());
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private boolean dropTableRowAndEntity(NameIdentifier ident, boolean purge)
      throws UnsupportedOperationException {
    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));

    if (purge) {
      Optional<ManagedIcebergMetadataStore.TableRow> row = selectTableRow(ident);
      if (row.isPresent()) {
        TableMetadata base = TableMetadataParser.read(fileIO, row.get().metadataLocation());
        CatalogUtil.dropTableData(fileIO, base);
      }
    }

    int deleted =
        runWriteOrFail(
            connection -> metadataStore.deleteTable(connection, tableNamespace, ident.name()));
    boolean entityDeleted = dropEntityQuietly(ident);
    return deleted > 0 || entityDeleted;
  }

  private void updateTableEntity(
      NameIdentifier ident, java.util.function.UnaryOperator<TableEntity> updater) {
    try {
      store().update(ident, TableEntity.class, Entity.EntityType.TABLE, updater);
    } catch (IOException e) {
      throw new RuntimeException("Failed to update the table entity of " + ident, e);
    }
  }

  private boolean dropEntityQuietly(NameIdentifier ident) {
    try {
      return store().delete(ident, Entity.EntityType.TABLE);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop the table entity of " + ident, e);
    }
  }

  private ManagedIcebergMetadataStore.TableRow selectTableRowOrThrow(NameIdentifier ident)
      throws NoSuchTableException {
    return selectTableRow(ident)
        .orElseThrow(() -> new NoSuchTableException("Table %s does not exist", ident));
  }

  private Optional<ManagedIcebergMetadataStore.TableRow> selectTableRow(NameIdentifier ident) {
    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));
    return runInTransaction(
        connection -> metadataStore.selectTable(connection, tableNamespace, ident.name()));
  }

  /**
   * Runs a write in one Gravitino transaction: the Iceberg row, the nested entity write and the
   * commit all happen on the same connection (design doc section 5.7).
   */
  private <T> T runInWriteTransaction(SqlFunction<T> action) throws SQLException {
    SessionUtils.beginTransaction();
    try {
      T result = action.apply(SessionUtils.currentConnection());
      // The transaction mixes raw JDBC statements with nested MyBatis entity writes, so the JDBC
      // transaction has to be resolved explicitly (see
      // SessionUtils#commitTransactionAndConnection).
      SessionUtils.commitTransactionAndConnection();
      return result;
    } catch (SQLException | RuntimeException | Error e) {
      SessionUtils.rollbackTransactionAndConnection();
      throw e;
    }
  }

  /** Runs a mutating action and rethrows a {@link SQLException} as a runtime failure. */
  private <T> T runWriteOrFail(SqlFunction<T> action) {
    try {
      return runInWriteTransaction(action);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run managed Iceberg SQL", e);
    }
  }

  /**
   * Runs SQL on the entity store connection. Reads borrow a pooled connection through a short
   * read-only transaction so they do not depend on a commit (design doc section 5.10).
   */
  private <T> T runInTransaction(SqlFunction<T> action) {
    SessionUtils.beginTransaction();
    try {
      return action.apply(SessionUtils.currentConnection());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run managed Iceberg SQL", e);
    } finally {
      SessionUtils.rollbackTransactionAndConnection();
    }
  }

  private static boolean isIntegrityConstraintViolation(SQLException e) {
    return e.getSQLState() != null && e.getSQLState().startsWith(SQL_STATE_INTEGRITY_CONSTRAINT);
  }

  private static NameIdentifier schemaIdentifierOf(NameIdentifier tableIdent) {
    return NameIdentifier.of(tableIdent.namespace().levels());
  }

  /**
   * Iceberg's JDBC backend stores the dotted Iceberg namespace in {@code iceberg_tables}, so a
   * Gravitino schema name has to be split with the configured hierarchical separator first (design
   * doc section 5.6, invariant 1).
   */
  private static String schemaNameToTableNamespace(String schemaName) {
    return String.join(".", IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName).levels());
  }

  String defaultTableLocation(NameIdentifier ident) {
    String tableNamespace =
        schemaNameToTableNamespace(ident.namespace().level(ident.namespace().length() - 1));
    return String.format(
        "%s/%s/%s", LocationUtil.stripTrailingSlash(warehouse), tableNamespace, ident.name());
  }

  /**
   * The entity id is the link between the Iceberg table and its {@code table_meta} row. The
   * Gravitino native entry has it injected by the dispatcher, the Iceberg REST entry does not, so
   * managed mode guarantees it here.
   */
  private Map<String, String> ensureStringIdentifier(Map<String, String> properties) {
    Map<String, String> result =
        properties == null ? new java.util.HashMap<>() : new java.util.HashMap<>(properties);
    if (StringIdentifier.fromProperties(result) == null) {
      result =
          StringIdentifier.newPropertiesWithId(
              StringIdentifier.fromId(idGenerator().nextId()), result);
    }
    return result;
  }

  private static IcebergColumn[] toIcebergColumns(Column[] columns) {
    return Arrays.stream(columns)
        .map(
            column ->
                IcebergColumn.builder()
                    .withName(column.name())
                    .withType(column.dataType())
                    .withComment(column.comment())
                    .withNullable(column.nullable())
                    .build())
        .toArray(IcebergColumn[]::new);
  }

  private static Distribution defaultDistribution(
      Transform[] partitioning, SortOrder[] sortOrders) {
    boolean partitioned = partitioning != null && partitioning.length > 0;
    boolean sorted = sortOrders != null && sortOrders.length > 0;
    if (sorted) {
      return Distributions.RANGE;
    }
    if (partitioned) {
      return Distributions.HASH;
    }
    return Distributions.NONE;
  }

  private static AuditInfo currentAuditInfo() {
    return AuditInfo.builder()
        .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
        .withCreateTime(Instant.now())
        .build();
  }

  private TableEntity toTableEntity(TableEntity old, NameIdentifier ident, TableMetadata metadata) {
    IcebergTable table = IcebergTable.fromIcebergTable(metadata, ident.name());
    List<ColumnEntity> columnEntities = new ArrayList<>();
    Column[] columns = table.columns();
    for (int i = 0; i < columns.length; i++) {
      columnEntities.add(
          ColumnEntity.toColumnEntity(columns[i], i, idGenerator().nextId(), old.auditInfo()));
    }
    return TableEntity.builder()
        .withId(old.id())
        .withName(ident.name())
        .withNamespace(ident.namespace())
        .withComment(table.comment())
        .withColumns(columnEntities)
        .withProperties(table.properties())
        .withAuditInfo(old.auditInfo())
        .withPartitioning(table.partitioning())
        .withDistribution(table.distribution())
        .withSortOrders(table.sortOrder())
        .withIndexes(Indexes.EMPTY_INDEXES)
        .build();
  }

  /** A SQL action that may throw {@link SQLException}. */
  @FunctionalInterface
  private interface SqlFunction<T> {
    T apply(Connection connection) throws SQLException;
  }
}
