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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the fail-fast preconditions (design doc section 8, test 16 and section 5.2). The
 * environment is faked so the checks can be exercised without a running Gravitino server.
 */
public class TestIcebergManagedCatalogPrerequisites {

  private static Connection probeConnection;
  private static boolean tablesCreated;

  private static final Map<String, String> MANAGED_BASE =
      ImmutableMap.of(
          IcebergConstants.CATALOG_BACKEND_MODE, "managed",
          IcebergConstants.CATALOG_BACKEND, "jdbc",
          IcebergConstants.WAREHOUSE, "file:///tmp/warehouse",
          IcebergConstants.CATALOG_BACKEND_NAME, "managed-catalog");

  @BeforeAll
  public static void setUp() throws Exception {
    probeConnection = DriverManager.getConnection("jdbc:h2:mem:prerequisites;DB_CLOSE_DELAY=-1");
  }

  @AfterAll
  public static void tearDown() throws SQLException {
    probeConnection.close();
  }

  private static IcebergManagedCatalogPrerequisites.Environment auxiliaryEnvironment() {
    return new IcebergManagedCatalogPrerequisites.Environment() {
      @Override
      public boolean isAuxiliaryMode() {
        return true;
      }

      @Override
      public void probeIcebergTables() throws SQLException {
        if (!tablesCreated) {
          throw new SQLException("Table \"ICEBERG_TABLES\" not found");
        }
        IcebergManagedCatalogPrerequisites.checkIcebergTablesAccessible(probeConnection);
      }
    };
  }

  private static IcebergManagedCatalogPrerequisites.Environment standaloneEnvironment() {
    return new IcebergManagedCatalogPrerequisites.Environment() {
      @Override
      public boolean isAuxiliaryMode() {
        return false;
      }

      @Override
      public void probeIcebergTables() {
        throw new AssertionError("the probe must not run in standalone mode");
      }
    };
  }

  @Test
  public void testFederatedRequiresUri() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergManagedCatalogPrerequisites.validate(
                    ImmutableMap.of(
                        IcebergConstants.CATALOG_BACKEND, "hive",
                        IcebergConstants.WAREHOUSE, "file:///tmp/warehouse"),
                    auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains("uri"), e.getMessage());
  }

  @Test
  public void testFederatedWithUriPassesWithoutTouchingTheEnvironment() {
    assertDoesNotThrow(
        () ->
            IcebergManagedCatalogPrerequisites.validate(
                ImmutableMap.of(
                    IcebergConstants.CATALOG_BACKEND, "hive",
                    IcebergConstants.URI, "thrift://127.0.0.1:9083",
                    IcebergConstants.WAREHOUSE, "file:///tmp/warehouse"),
                standaloneEnvironment()));
  }

  @Test
  public void testManagedRequiresJdbcBackend() {
    Map<String, String> properties = new HashMap<>(MANAGED_BASE);
    properties.put(IcebergConstants.CATALOG_BACKEND, "hive");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergManagedCatalogPrerequisites.validate(properties, auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains("catalog-backend = jdbc"), e.getMessage());
  }

  @Test
  public void testManagedRequiresWarehouse() {
    Map<String, String> properties = new HashMap<>(MANAGED_BASE);
    properties.remove(IcebergConstants.WAREHOUSE);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergManagedCatalogPrerequisites.validate(properties, auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains("warehouse"), e.getMessage());
  }

  @Test
  public void testManagedRequiresCatalogBackendName() {
    Map<String, String> properties = new HashMap<>(MANAGED_BASE);
    properties.remove(IcebergConstants.CATALOG_BACKEND_NAME);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergManagedCatalogPrerequisites.validate(properties, auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains(IcebergConstants.CATALOG_BACKEND_NAME), e.getMessage());
  }

  @Test
  public void testManagedIsRejectedInStandaloneMode() {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergManagedCatalogPrerequisites.validate(MANAGED_BASE, standaloneEnvironment()));
    assertTrue(e.getMessage().contains("auxiliary mode"), e.getMessage());
  }

  @Test
  public void testManagedIsRejectedWhenIcebergTablesAreMissing() {
    tablesCreated = false;
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergManagedCatalogPrerequisites.validate(MANAGED_BASE, auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains("init script"), e.getMessage());
  }

  @Test
  public void testManagedPassesWhenTheEnvironmentIsReady() throws SQLException {
    tablesCreated = true;
    ManagedIcebergMetadataStore.createIcebergTablesIfNotExists(probeConnection);

    assertDoesNotThrow(
        () -> IcebergManagedCatalogPrerequisites.validate(MANAGED_BASE, auxiliaryEnvironment()));
  }

  @Test
  public void testInvalidModeValueIsRejected() {
    Map<String, String> properties = new HashMap<>(MANAGED_BASE);
    properties.put(IcebergConstants.CATALOG_BACKEND_MODE, "semi-managed");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergManagedCatalogPrerequisites.validate(properties, auxiliaryEnvironment()));
    assertTrue(e.getMessage().contains("Invalid value [semi-managed]"), e.getMessage());
  }
}
