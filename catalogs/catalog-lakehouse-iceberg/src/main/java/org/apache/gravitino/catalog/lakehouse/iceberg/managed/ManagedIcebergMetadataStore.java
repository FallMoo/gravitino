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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All SQL and metadata-file helpers managed mode needs (design doc sections 5.5, 5.6 and 5.11).
 *
 * <p>Every method takes a {@link Connection} explicitly. In production that connection is always
 * the one bound to the current Gravitino transaction ({@code SqlSessions}), so that the Iceberg
 * pointer and the Gravitino entity commit together. The class itself never opens or commits a
 * connection.
 *
 * <p>The DDL deliberately follows the Iceberg JDBC V1 schema, which is what a native Iceberg {@code
 * JdbcCatalog} pointed at the same database expects (design doc section 2, goal 5).
 */
public class ManagedIcebergMetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedIcebergMetadataStore.class);

  /** Iceberg's V1 {@code iceberg_tables} DDL. */
  public static final String CREATE_ICEBERG_TABLES_DDL =
      "CREATE TABLE IF NOT EXISTS iceberg_tables ("
          + "catalog_name VARCHAR(255) NOT NULL,"
          + "table_namespace VARCHAR(255) NOT NULL,"
          + "table_name VARCHAR(255) NOT NULL,"
          + "metadata_location VARCHAR(1000),"
          + "previous_metadata_location VARCHAR(1000),"
          + "iceberg_type VARCHAR(5),"
          + "PRIMARY KEY (catalog_name, table_namespace, table_name))";

  /** Iceberg's V1 {@code iceberg_namespace_properties} DDL. */
  public static final String CREATE_ICEBERG_NAMESPACE_PROPERTIES_DDL =
      "CREATE TABLE IF NOT EXISTS iceberg_namespace_properties ("
          + "catalog_name VARCHAR(255) NOT NULL,"
          + "namespace VARCHAR(255) NOT NULL,"
          + "property_key VARCHAR(255),"
          + "property_value VARCHAR(1000),"
          + "PRIMARY KEY (catalog_name, namespace, property_key))";

  /** The value managed mode writes into {@code iceberg_tables.iceberg_type}. */
  public static final String ICEBERG_TYPE_TABLE = "TABLE";

  private static final String TABLE_PREDICATE =
      " AND (iceberg_type = '" + ICEBERG_TYPE_TABLE + "' OR iceberg_type IS NULL)";

  private static final String SELECT_TABLE_ROW =
      "SELECT metadata_location, previous_metadata_location FROM iceberg_tables"
          + " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?"
          + TABLE_PREDICATE;

  private static final String SELECT_TABLE_NAMES =
      "SELECT table_name FROM iceberg_tables"
          + " WHERE catalog_name = ? AND table_namespace = ?"
          + TABLE_PREDICATE;

  private static final String SELECT_TABLE_EXISTS =
      "SELECT 1 FROM iceberg_tables"
          + " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?"
          + TABLE_PREDICATE;

  private static final String INSERT_TABLE =
      "INSERT INTO iceberg_tables"
          + " (catalog_name, table_namespace, table_name, metadata_location,"
          + " previous_metadata_location, iceberg_type) VALUES (?, ?, ?, ?, NULL, '"
          + ICEBERG_TYPE_TABLE
          + "')";

  private static final String UPDATE_TABLE_LOCATION =
      "UPDATE iceberg_tables SET metadata_location = ?, previous_metadata_location = ?"
          + " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?"
          + " AND metadata_location = ?"
          + TABLE_PREDICATE;

  private static final String RENAME_TABLE =
      "UPDATE iceberg_tables SET table_namespace = ?, table_name = ?"
          + " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?"
          + TABLE_PREDICATE;

  private static final String DELETE_TABLE =
      "DELETE FROM iceberg_tables"
          + " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?"
          + TABLE_PREDICATE;

  private static final String SELECT_NAMESPACE_PROPERTIES =
      "SELECT property_key, property_value FROM iceberg_namespace_properties"
          + " WHERE catalog_name = ? AND namespace = ?";

  private static final String SELECT_NAMESPACES =
      "SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE catalog_name = ?";

  private static final String SELECT_CHILD_TABLE_NAMESPACES =
      "SELECT DISTINCT table_namespace FROM iceberg_tables"
          + " WHERE catalog_name = ? AND (table_namespace = ? OR table_namespace LIKE ? ESCAPE '!')";

  private static final String SELECT_CHILD_NAMESPACES =
      "SELECT DISTINCT namespace FROM iceberg_namespace_properties"
          + " WHERE catalog_name = ? AND (namespace = ? OR namespace LIKE ? ESCAPE '!')";

  private static final String SELECT_ALL_NAMESPACES =
      "SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE catalog_name = ?"
          + " UNION SELECT DISTINCT table_namespace FROM iceberg_tables WHERE catalog_name = ?";

  private static final String COUNT_TABLES_IN_NAMESPACE =
      "SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?";

  private static final String UPDATE_NAMESPACE_PROPERTY =
      "UPDATE iceberg_namespace_properties SET property_value = ?"
          + " WHERE catalog_name = ? AND namespace = ? AND property_key = ?";

  private static final String INSERT_NAMESPACE_PROPERTY =
      "INSERT INTO iceberg_namespace_properties"
          + " (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)";

  private static final String DELETE_NAMESPACE_PROPERTIES =
      "DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ?";

  private static final String DELETE_NAMESPACE_PROPERTY =
      "DELETE FROM iceberg_namespace_properties"
          + " WHERE catalog_name = ? AND namespace = ? AND property_key = ?";

  private final String catalogName;

  /**
   * @param catalogName the value stored in {@code iceberg_tables.catalog_name}, i.e. the catalog's
   *     {@code catalog-backend-name} property
   */
  public ManagedIcebergMetadataStore(String catalogName) {
    this.catalogName = catalogName;
  }

  public String catalogName() {
    return catalogName;
  }

  // ---------------------------------------------------------------------------------------------
  // DDL
  // ---------------------------------------------------------------------------------------------

  /**
   * Creates the Iceberg V1 tables if they do not exist yet. Must run outside of any transaction
   * (MySQL commits implicitly on DDL).
   *
   * @param connection an autocommit connection to the entity store database
   * @throws SQLException if the DDL fails
   */
  public static void createIcebergTablesIfNotExists(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(CREATE_ICEBERG_TABLES_DDL);
      statement.execute(CREATE_ICEBERG_NAMESPACE_PROPERTIES_DDL);
    }
  }

  /**
   * Creates the Iceberg V1 tables on a short-lived entity-store transaction when they are missing.
   *
   * <p>They live in the same database as the Gravitino entity store, and this is called while a
   * catalog is initialized, never inside an existing transaction: MySQL commits DDL implicitly, so
   * running it on a bound transaction would silently break the "pointer row and entity row commit
   * together" guarantee (design doc section 5.7). When the database user is not allowed to run DDL
   * the failure is only logged - the probe in {@code IcebergManagedCatalogPrerequisites.validate}
   * then reports the actionable error that points at the init script.
   */
  public static void ensureIcebergTables() {
    if (SqlSessions.getBoundConnection() != null) {
      LOG.debug("A Gravitino transaction is already bound, skipping the Iceberg pointer table DDL");
      return;
    }
    SessionUtils.beginTransaction();
    try {
      createIcebergTablesIfNotExists(SessionUtils.currentConnection());
      SessionUtils.commitTransactionAndConnection();
    } catch (SQLException | RuntimeException e) {
      SessionUtils.rollbackTransactionAndConnection();
      LOG.warn(
          "Failed to create the Iceberg pointer tables; a managed catalog requires them in the"
              + " Gravitino entity store database",
          e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Metadata files (design doc section 5.5)
  // ---------------------------------------------------------------------------------------------

  /**
   * The outcome of writing (or reusing) a metadata file.
   *
   * <p>{@code location} is the metadata file location; {@code written} tells whether this call
   * actually created the file. {@code written == false} means the location was already in use by
   * someone else and must never be cleaned up on a later failure.
   */
  public record MetadataWriteResult(String location, boolean written) {}

  /**
   * Builds Iceberg's metadata file name, {@code %05d-<uuid><extension>} (Iceberg table spec).
   *
   * @param metadata the metadata about to be written (supplies the compression codec)
   * @param version the metadata version
   * @return the file name, not a full location
   */
  public static String newMetadataFileName(TableMetadata metadata, int version) {
    String codecName =
        metadata.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    return String.format(
        Locale.ROOT,
        "%05d-%s%s",
        version,
        UUID.randomUUID(),
        TableMetadataParser.getFileExtension(codecName));
  }

  /**
   * Derives the metadata file location for a file name, honouring {@code write.metadata.location}.
   *
   * @param metadata the table metadata
   * @param fileName the file name produced by {@link #newMetadataFileName(TableMetadata, int)}
   * @return the full metadata file location
   */
  public static String metadataFileLocation(TableMetadata metadata, String fileName) {
    String baseDir = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);
    if (baseDir != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(baseDir), fileName);
    }
    return String.format("%s/metadata/%s", metadata.location(), fileName);
  }

  /**
   * Writes a metadata file. This is the only {@code FileIO} write managed mode performs.
   *
   * @param io the FileIO to write with
   * @param metadata the metadata to persist
   * @param version the metadata version to encode in the file name
   * @return the write result, with {@code written = true}
   */
  public MetadataWriteResult writeMetadata(FileIO io, TableMetadata metadata, int version) {
    String location = metadataFileLocation(metadata, newMetadataFileName(metadata, version));
    OutputFile outputFile = io.newOutputFile(location);
    TableMetadataParser.overwrite(metadata, outputFile);
    return new MetadataWriteResult(outputFile.location(), true);
  }

  /**
   * Reuses an existing metadata file, e.g. for a register or for metadata that already carries a
   * location. No file is written, so the result must never be cleaned up on failure.
   *
   * @param metadata the metadata whose file location is reused
   * @return the write result, with {@code written = false}
   */
  public MetadataWriteResult reuseMetadataFile(TableMetadata metadata) {
    return new MetadataWriteResult(metadata.metadataFileLocation(), false);
  }

  /**
   * Best-effort deletion of a metadata file just written by a failed commit.
   *
   * @param io the FileIO to delete with
   * @param location the location to delete
   * @return {@code true} if the file was deleted
   */
  public boolean deleteMetadataFileQuietly(FileIO io, String location) {
    try {
      io.deleteFile(location);
      return true;
    } catch (RuntimeException e) {
      LOG.warn("Failed to delete the orphan metadata file {}", location, e);
      return false;
    }
  }

  /**
   * Parses the metadata version out of a metadata file name (design doc section 5.5).
   *
   * @param metadataLocation the metadata file location
   * @return the parsed version, or {@code -1} when the name does not follow {@code %05d-}
   */
  public static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1;
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      return -1;
    }
    return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
  }

  // ---------------------------------------------------------------------------------------------
  // iceberg_tables SQL (design doc section 5.6)
  // ---------------------------------------------------------------------------------------------

  /**
   * One row of {@code iceberg_tables}: the current metadata file location plus the previous one
   * (which may be {@code null}).
   */
  public record TableRow(String metadataLocation, String previousMetadataLocation) {}

  public Optional<TableRow> selectTable(Connection connection, String namespace, String tableName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE_ROW)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, tableName);
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        return Optional.of(new TableRow(rs.getString(1), rs.getString(2)));
      }
    }
  }

  public boolean tableExists(Connection connection, String namespace, String tableName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE_EXISTS)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, tableName);
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next();
      }
    }
  }

  public List<String> listTableNames(Connection connection, String namespace) throws SQLException {
    List<String> names = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE_NAMES)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          names.add(rs.getString(1));
        }
      }
    }
    return names;
  }

  /**
   * Inserts a table row. A duplicate key surfaces as a {@link SQLException} from the driver;
   * callers map it to {@code TableAlreadyExistsException}.
   *
   * @return the number of inserted rows (always 1 on success)
   */
  public int insertTable(
      Connection connection, String namespace, String tableName, String metadataLocation)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(INSERT_TABLE)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, tableName);
      statement.setString(4, metadataLocation);
      return statement.executeUpdate();
    }
  }

  /**
   * Optimistic-concurrency update of the metadata pointer. This is Iceberg's only protection
   * against concurrent writers (design doc section 5.6, invariant 2): when the current row no
   * longer points at {@code expectedMetadataLocation} the update affects 0 rows and a {@link
   * CommitFailedException} is thrown, which the commit path turns into a retry.
   *
   * @param connection the transaction-bound connection
   * @param namespace the table namespace
   * @param tableName the table name
   * @param expectedMetadataLocation the metadata location the caller based its update on
   * @param newMetadataLocation the new metadata location
   * @throws SQLException if the statement fails
   * @throws CommitFailedException if no row matched, i.e. a concurrent writer won the race
   */
  public void commitTableLocation(
      Connection connection,
      String namespace,
      String tableName,
      String expectedMetadataLocation,
      String newMetadataLocation)
      throws SQLException {
    int updated;
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_TABLE_LOCATION)) {
      statement.setString(1, newMetadataLocation);
      statement.setString(2, expectedMetadataLocation);
      statement.setString(3, catalogName);
      statement.setString(4, namespace);
      statement.setString(5, tableName);
      statement.setString(6, expectedMetadataLocation);
      updated = statement.executeUpdate();
    }

    if (updated == 0) {
      throw new CommitFailedException(
          "Failed to commit the metadata update of table %s.%s: the metadata location changed"
              + " concurrently (expected %s).",
          namespace, tableName, expectedMetadataLocation);
    }
  }

  /**
   * Renames a table row.
   *
   * @return the number of updated rows; {@code 0} means the source table disappeared
   */
  public int renameTable(
      Connection connection,
      String fromNamespace,
      String fromName,
      String toNamespace,
      String toName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(RENAME_TABLE)) {
      statement.setString(1, toNamespace);
      statement.setString(2, toName);
      statement.setString(3, catalogName);
      statement.setString(4, fromNamespace);
      statement.setString(5, fromName);
      return statement.executeUpdate();
    }
  }

  /**
   * Deletes a table row.
   *
   * @return the number of deleted rows; {@code 0} means the table did not exist
   */
  public int deleteTable(Connection connection, String namespace, String tableName)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(DELETE_TABLE)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, tableName);
      return statement.executeUpdate();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // iceberg_namespace_properties SQL (design doc section 5.11)
  // ---------------------------------------------------------------------------------------------

  public Map<String, String> loadNamespaceProperties(Connection connection, String namespace)
      throws SQLException {
    Map<String, String> properties = new LinkedHashMap<>();
    try (PreparedStatement statement = connection.prepareStatement(SELECT_NAMESPACE_PROPERTIES)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          properties.put(rs.getString(1), rs.getString(2));
        }
      }
    }
    return properties;
  }

  public List<String> listNamespaces(Connection connection) throws SQLException {
    List<String> namespaces = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SELECT_NAMESPACES)) {
      statement.setString(1, catalogName);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          namespaces.add(rs.getString(1));
        }
      }
    }
    return namespaces;
  }

  /**
   * Lists every namespace that has properties or tables. Namespace existence in Iceberg's JDBC
   * backend is derived from both tables, so a listing that only looked at the properties table
   * would hide namespaces created implicitly by a table.
   *
   * @param connection the transaction-bound connection
   * @return the dotted namespace strings
   * @throws SQLException if the statement fails
   */
  public List<String> listAllNamespaces(Connection connection) throws SQLException {
    List<String> namespaces = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(SELECT_ALL_NAMESPACES)) {
      statement.setString(1, catalogName);
      statement.setString(2, catalogName);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          namespaces.add(rs.getString(1));
        }
      }
    }
    return namespaces;
  }

  /**
   * @param connection the transaction-bound connection
   * @param namespace the dotted namespace string
   * @return the number of tables directly in the namespace
   * @throws SQLException if the statement fails
   */
  public int countTables(Connection connection, String namespace) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(COUNT_TABLES_IN_NAMESPACE)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
      }
    }
  }

  /**
   * Updates a single namespace property.
   *
   * @return the number of updated rows, {@code 0} when the key did not exist
   */
  public int updateNamespaceProperty(
      Connection connection, String namespace, String key, String value) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_NAMESPACE_PROPERTY)) {
      statement.setString(1, value);
      statement.setString(2, catalogName);
      statement.setString(3, namespace);
      statement.setString(4, key);
      return statement.executeUpdate();
    }
  }

  /**
   * Iceberg's JDBC backend derives namespace existence from {@code iceberg_namespace_properties}
   * together with {@code iceberg_tables}: a namespace exists when it has properties, has tables, or
   * is a parent of a deeper namespace.
   *
   * @param connection the transaction-bound connection
   * @param namespace the dotted namespace string
   * @return {@code true} if the namespace exists
   * @throws SQLException if a statement fails
   */
  public boolean namespaceExists(Connection connection, String namespace) throws SQLException {
    if (!loadNamespaceProperties(connection, namespace).isEmpty()) {
      return true;
    }

    String nestedPattern = escapeLike(namespace) + ".%";
    if (hasRow(connection, SELECT_CHILD_NAMESPACES, namespace, nestedPattern)) {
      return true;
    }
    return hasRow(connection, SELECT_CHILD_TABLE_NAMESPACES, namespace, nestedPattern);
  }

  private boolean hasRow(Connection connection, String sql, String namespace, String pattern)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, pattern);
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next();
      }
    }
  }

  public int insertNamespaceProperty(
      Connection connection, String namespace, String key, String value) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(INSERT_NAMESPACE_PROPERTY)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, key);
      statement.setString(4, value);
      return statement.executeUpdate();
    }
  }

  /**
   * Deletes all properties of a namespace.
   *
   * @return the number of deleted rows
   */
  public int deleteNamespaceProperties(Connection connection, String namespace)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(DELETE_NAMESPACE_PROPERTIES)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      return statement.executeUpdate();
    }
  }

  /**
   * Deletes a single namespace property.
   *
   * @return the number of deleted rows
   */
  public int deleteNamespaceProperty(Connection connection, String namespace, String key)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(DELETE_NAMESPACE_PROPERTY)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      statement.setString(3, key);
      return statement.executeUpdate();
    }
  }

  /** Escapes {@code %} and {@code _} using Iceberg's {@code !} escape character. */
  static String escapeLike(String value) {
    return value.replace("!", "!!").replace("%", "!%").replace("_", "!_");
  }
}
