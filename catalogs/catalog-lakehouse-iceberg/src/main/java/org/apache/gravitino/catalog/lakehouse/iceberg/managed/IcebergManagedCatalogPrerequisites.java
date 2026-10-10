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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackendMode;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Fail-fast preconditions for {@code catalog-backend-mode = managed} (design doc section 5.2).
 *
 * <p>Managed mode puts the Iceberg metadata pointer ({@code iceberg_tables}) and the Gravitino
 * entity ({@code table_meta}) in one database and one transaction. That is only possible when the
 * catalog is a JDBC Iceberg catalog backed by the very same database Gravitino uses, and when the
 * Iceberg REST service runs in the same process as the entity store. Every violation is rejected
 * here instead of silently degrading into a catalog that "looks created but never works".
 */
public final class IcebergManagedCatalogPrerequisites {

  /** Columns probed to make sure the Iceberg V1 schema is really usable. */
  private static final String PROBE_ICEBERG_TABLES =
      "SELECT catalog_name, table_namespace, table_name, metadata_location,"
          + " previous_metadata_location, iceberg_type FROM iceberg_tables WHERE 1 = 0";

  private static final String PROBE_ICEBERG_NAMESPACE_PROPERTIES =
      "SELECT catalog_name, namespace, property_key, property_value"
          + " FROM iceberg_namespace_properties WHERE 1 = 0";

  private IcebergManagedCatalogPrerequisites() {}

  /**
   * The part of the runtime a managed catalog depends on. Abstracted so that the checks can be unit
   * tested without an initialized {@link GravitinoEnv}.
   */
  interface Environment {
    /**
     * @return {@code true} when Gravitino runs with its full components (auxiliary mode).
     */
    boolean isAuxiliaryMode();

    /**
     * Probes {@code iceberg_tables} / {@code iceberg_namespace_properties} on a connection to the
     * Gravitino entity store database.
     *
     * @throws SQLException if either table is missing or not readable
     */
    void probeIcebergTables() throws SQLException;
  }

  /**
   * Validates every managed-mode precondition that can be checked from the catalog properties plus
   * the surrounding Gravitino runtime. Federated catalogs only get the {@code uri} check, so their
   * pre-existing behaviour is preserved.
   *
   * @param properties the resolved catalog properties
   * @throws IllegalArgumentException if the property combination is illegal
   * @throws IllegalStateException if the runtime cannot support managed mode
   */
  public static void validate(Map<String, String> properties) {
    validate(properties, gravitinoEnvironment());
  }

  static void validate(Map<String, String> properties, Environment environment) {
    IcebergCatalogBackendMode mode = IcebergCatalogBackendMode.fromProperties(properties);
    if (mode == IcebergCatalogBackendMode.FEDERATED) {
      // `uri` used to be a static required property; keep the requirement for federated catalogs.
      if (StringUtils.isBlank(properties.get(IcebergConstants.URI))) {
        throw new IllegalArgumentException(
            "Property 'uri' is required when '"
                + IcebergConstants.CATALOG_BACKEND_MODE
                + "' is 'federated' (or unset).");
      }
      return;
    }

    String backend = properties.get(IcebergConstants.CATALOG_BACKEND);
    if (!IcebergCatalogBackend.JDBC.name().equalsIgnoreCase(backend)) {
      throw new IllegalArgumentException(
          String.format(
              "'%s = managed' requires '%s = jdbc', but got '%s'. Iceberg backends such as hive,"
                  + " rest, memory or custom are external metastores; Gravitino cannot commit"
                  + " them together with its entity store.",
              IcebergConstants.CATALOG_BACKEND_MODE, IcebergConstants.CATALOG_BACKEND, backend));
    }

    if (StringUtils.isBlank(properties.get(IcebergConstants.WAREHOUSE))) {
      throw new IllegalArgumentException(
          String.format(
              "Property '%s' is required in managed mode: Gravitino still writes Iceberg metadata"
                  + " JSON files into the warehouse.",
              IcebergConstants.WAREHOUSE));
    }

    if (StringUtils.isBlank(properties.get(IcebergConstants.CATALOG_BACKEND_NAME))) {
      throw new IllegalArgumentException(
          String.format(
              "Property '%s' is required in managed mode: it is the 'catalog_name' column used to"
                  + " address rows in iceberg_tables.",
              IcebergConstants.CATALOG_BACKEND_NAME));
    }

    if (!environment.isAuxiliaryMode()) {
      throw new IllegalStateException(
          "Managed Iceberg catalogs require the Iceberg REST service to run in auxiliary mode, in"
              + " the same process as the Gravitino entity store. This deployment has no entity"
              + " store, so iceberg_tables and table_meta could never share a transaction.");
    }

    try {
      environment.probeIcebergTables();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Managed Iceberg catalogs require the Iceberg V1 tables 'iceberg_tables' and"
              + " 'iceberg_namespace_properties' to exist in the Gravitino entity store database."
              + " Create them with the Gravitino init script before creating the catalog.",
          e);
    }
  }

  /**
   * Verifies that the Iceberg V1 tables are present and readable on the given (entity store)
   * connection.
   *
   * @param connection a connection to the Gravitino entity store database
   * @throws SQLException if either table is missing or not readable
   */
  public static void checkIcebergTablesAccessible(Connection connection) throws SQLException {
    probe(connection, PROBE_ICEBERG_TABLES);
    probe(connection, PROBE_ICEBERG_NAMESPACE_PROPERTIES);
  }

  private static void probe(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet ignored = statement.executeQuery(sql)) {
      // Only the existence of the table and of its columns matters here.
    }
  }

  private static Environment gravitinoEnvironment() {
    return new Environment() {
      @Override
      public boolean isAuxiliaryMode() {
        GravitinoEnv env = GravitinoEnv.getInstance();
        return env.entityStore() != null && env.catalogManager() != null;
      }

      @Override
      public void probeIcebergTables() throws SQLException {
        // A short read-only transaction: the probe must observe the same database the entity store
        // uses, and it must not leave a session behind.
        SessionUtils.beginTransaction();
        try {
          checkIcebergTablesAccessible(SessionUtils.currentConnection());
        } finally {
          SessionUtils.rollbackTransaction();
        }
      }
    };
  }
}
