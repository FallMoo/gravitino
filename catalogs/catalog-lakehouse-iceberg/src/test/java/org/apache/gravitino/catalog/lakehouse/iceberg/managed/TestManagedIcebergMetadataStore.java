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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ManagedIcebergMetadataStore} (design doc section 8, test 1): every SQL
 * statement against an embedded database, including "0 updated rows must raise {@code
 * CommitFailedException}".
 */
public class TestManagedIcebergMetadataStore {

  private static final String CATALOG_NAME = "catalog-jdbc-1";
  private static final String NAMESPACE = "db";
  private static final String TABLE = "t1";
  private static final String LOC_V1 = "file:///tmp/warehouse/t1/metadata/00001-a.metadata.json";
  private static final String LOC_V2 = "file:///tmp/warehouse/t1/metadata/00002-b.metadata.json";

  private static Path dbDir;
  private static Connection connection;

  private final ManagedIcebergMetadataStore store = new ManagedIcebergMetadataStore(CATALOG_NAME);

  @BeforeAll
  public static void setUp() throws Exception {
    dbDir = Files.createTempDirectory("gravitino-managed-store-");
    String url = String.format("jdbc:h2:file:%s/store;DB_CLOSE_DELAY=-1;MODE=MYSQL", dbDir);
    connection = DriverManager.getConnection(url, "root", "123");
    connection.setAutoCommit(true);
    ManagedIcebergMetadataStore.createIcebergTablesIfNotExists(connection);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
    }
    if (dbDir != null) {
      FileUtils.deleteDirectory(dbDir.toFile());
    }
  }

  @BeforeEach
  public void cleanTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM iceberg_tables");
      statement.execute("DELETE FROM iceberg_namespace_properties");
    }
  }

  @Test
  public void testTableRowLifecycle() throws SQLException {
    assertEquals(1, store.insertTable(connection, NAMESPACE, TABLE, LOC_V1));
    assertTrue(store.tableExists(connection, NAMESPACE, TABLE));
    assertEquals(List.of(TABLE), store.listTableNames(connection, NAMESPACE));

    Optional<ManagedIcebergMetadataStore.TableRow> row =
        store.selectTable(connection, NAMESPACE, TABLE);
    assertTrue(row.isPresent());
    assertEquals(LOC_V1, row.get().metadataLocation());
    assertNull(row.get().previousMetadataLocation());

    // Rename within the same namespace.
    assertEquals(1, store.renameTable(connection, NAMESPACE, TABLE, NAMESPACE, "t2"));
    assertFalse(store.tableExists(connection, NAMESPACE, TABLE));
    assertTrue(store.tableExists(connection, NAMESPACE, "t2"));

    // Rename to another namespace.
    assertEquals(1, store.renameTable(connection, NAMESPACE, "t2", "other", "t3"));
    assertTrue(store.tableExists(connection, "other", "t3"));
    assertFalse(store.tableExists(connection, NAMESPACE, "t2"));

    // Renaming a missing table is a no-op that the caller maps to NoSuchTableException.
    assertEquals(0, store.renameTable(connection, NAMESPACE, "missing", NAMESPACE, "x"));

    assertEquals(1, store.deleteTable(connection, "other", "t3"));
    assertEquals(0, store.deleteTable(connection, "other", "t3"));
  }

  @Test
  public void testOptimisticConcurrencyUpdate() throws SQLException {
    store.insertTable(connection, NAMESPACE, TABLE, LOC_V1);

    store.commitTableLocation(connection, NAMESPACE, TABLE, LOC_V1, LOC_V2);
    Optional<ManagedIcebergMetadataStore.TableRow> row =
        store.selectTable(connection, NAMESPACE, TABLE);
    assertTrue(row.isPresent());
    assertEquals(LOC_V2, row.get().metadataLocation());
    assertEquals(LOC_V1, row.get().previousMetadataLocation());

    // Committing against a stale base must fail: this is the only protection against concurrent
    // writers (design doc section 5.6, invariant 2).
    CommitFailedException e =
        assertThrows(
            CommitFailedException.class,
            () -> store.commitTableLocation(connection, NAMESPACE, TABLE, LOC_V1, LOC_V2));
    assertTrue(e.getMessage().contains("concurrently"), e.getMessage());

    // And the row is untouched.
    assertEquals(
        LOC_V2, store.selectTable(connection, NAMESPACE, TABLE).orElseThrow().metadataLocation());
  }

  @Test
  public void testInsertDuplicatePrimaryKeyFails() throws SQLException {
    store.insertTable(connection, NAMESPACE, TABLE, LOC_V1);
    assertThrows(SQLException.class, () -> store.insertTable(connection, NAMESPACE, TABLE, LOC_V2));
  }

  @Test
  public void testViewRowsAreNotTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name,"
                  + " metadata_location, previous_metadata_location, iceberg_type)"
                  + " VALUES ('%s', '%s', 'v1', '%s', NULL, 'VIEW')",
              CATALOG_NAME, NAMESPACE, LOC_V1));
    }

    // Views are out of scope for managed mode (design doc section 3) and must be invisible to the
    // table SQL.
    assertFalse(store.tableExists(connection, NAMESPACE, "v1"));
    assertTrue(store.listTableNames(connection, NAMESPACE).isEmpty());
  }

  @Test
  public void testNamespacePropertiesAndExistence() throws SQLException {
    assertFalse(store.namespaceExists(connection, NAMESPACE));

    store.insertNamespaceProperty(connection, NAMESPACE, "owner", "gravitino");
    store.insertNamespaceProperty(connection, NAMESPACE, "location", "/tmp/db");
    assertEquals(
        Map.of("owner", "gravitino", "location", "/tmp/db"),
        store.loadNamespaceProperties(connection, NAMESPACE));
    assertTrue(store.namespaceExists(connection, NAMESPACE));
    assertEquals(List.of(NAMESPACE), store.listNamespaces(connection));

    assertEquals(1, store.deleteNamespaceProperty(connection, NAMESPACE, "location"));
    assertEquals(
        Map.of("owner", "gravitino"), store.loadNamespaceProperties(connection, NAMESPACE));

    // A namespace that only holds tables still exists (Iceberg derives existence from both tables).
    assertFalse(store.namespaceExists(connection, "table_only"));
    store.insertTable(connection, "table_only", TABLE, LOC_V1);
    assertTrue(store.namespaceExists(connection, "table_only"));

    // A parent of a deeper namespace exists even without its own properties or tables.
    store.insertTable(connection, "parent.child", TABLE, LOC_V1);
    assertTrue(store.namespaceExists(connection, "parent"));

    assertEquals(1, store.deleteNamespaceProperties(connection, NAMESPACE));
    assertFalse(store.namespaceExists(connection, NAMESPACE));
  }

  @Test
  public void testEscapeLike() {
    assertEquals("a!%b!_c!!d", ManagedIcebergMetadataStore.escapeLike("a%b_c!d"));
  }
}
