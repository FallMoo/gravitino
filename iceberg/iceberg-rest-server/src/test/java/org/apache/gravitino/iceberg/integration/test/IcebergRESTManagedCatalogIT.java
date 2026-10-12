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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end test of the managed Iceberg mode: Spark talks the Iceberg REST protocol to Gravitino's
 * auxiliary Iceberg REST service, which serves a catalog whose metadata Gravitino owns ({@code
 * catalog-backend-mode = managed}).
 *
 * <p>The test runs without Docker: the entity store is the H2 database BaseIT configures, the
 * warehouse is a local directory, and the server is MiniGravitino with the auxiliary Iceberg REST
 * service enabled in the same process. This is what managed mode requires - the pointer tables live
 * in the entity store database and the REST service resolves the catalog through the in-process
 * {@code CatalogManager}.
 *
 * <p>Beyond the Spark SQL results, the test asserts the Gravitino side: the table entity exists
 * after Spark creates the table even though authorization is disabled, which is the behaviour
 * federated mode cannot provide.
 */
public class IcebergRESTManagedCatalogIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTManagedCatalogIT.class);

  private static final String METALAKE_NAME = "managed_rest_metalake";
  private static final String CATALOG_NAME = "managed_rest_catalog";
  private static final String SPARK_CATALOG = "rest";
  private static final String NAMESPACE = "managed_db";
  private static final String TABLE = "t_managed";
  private static final String RENAMED_TABLE = "t_managed_renamed";
  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";

  private Path warehouseDir;
  private SparkSession spark;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    warehouseDir = Files.createTempDirectory("gravitino-managed-iceberg-rest");
    // MiniGravitino starts the full Gravitino server, so the auxiliary Iceberg REST service shares
    // the entity store and the CatalogManager with it.
    ignoreIcebergAuxRestService = false;
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE, METALAKE_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
        CATALOG_NAME);

    super.startIntegrationTest();

    GravitinoMetalake metalake = client.createMetalake(METALAKE_NAME, "", new HashMap<>());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    properties.put(IcebergConstants.CATALOG_BACKEND_MODE, "managed");
    // The catalog name must match, because the REST prefix, the entity rows and the Iceberg pointer
    // rows are all resolved through it.
    properties.put(IcebergConstants.CATALOG_BACKEND_NAME, CATALOG_NAME);
    properties.put(IcebergConstants.WAREHOUSE, warehouseDir.toUri().toString());
    metalake.createCatalog(
        CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "managed e2e", properties);

    initSparkEnv();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (spark != null) {
      spark.close();
      spark = null;
    }
    if (client != null) {
      try {
        client.dropMetalake(METALAKE_NAME, true);
      } catch (Exception e) {
        LOG.warn("Failed to drop metalake {}", METALAKE_NAME, e);
      }
    }
    super.stopIntegrationTest();
  }

  @Test
  public void testSparkCrudAgainstManagedCatalog() {
    spark.sql("USE " + SPARK_CATALOG);
    spark.sql(String.format("CREATE DATABASE %s", NAMESPACE));
    spark.sql(
        String.format("CREATE TABLE %s (id INT, name STRING) USING iceberg", fullTable(TABLE)));

    // Spark went through the Iceberg REST entry into the managed implementation layer, which wrote
    // both the Iceberg pointer row and the Gravitino entity even though authorization is disabled.
    assertTableEntityExists(TABLE, true);

    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", fullTable(TABLE)));
    Assertions.assertEquals(
        2, spark.sql(String.format("SELECT * FROM %s", fullTable(TABLE))).count());

    // Alter: the native Iceberg commit path through the REST entry.
    spark.sql(String.format("ALTER TABLE %s ADD COLUMN extra STRING", fullTable(TABLE)));
    spark.sql(String.format("INSERT INTO %s VALUES (3, 'c', 'x')", fullTable(TABLE)));
    Assertions.assertEquals(
        3, spark.sql(String.format("SELECT * FROM %s", fullTable(TABLE))).count());
    Assertions.assertEquals(
        1,
        spark.sql(String.format("SELECT * FROM %s WHERE extra = 'x'", fullTable(TABLE))).count());

    // Rename moves the pointer row and the entity together.
    spark.sql(
        String.format("ALTER TABLE %s RENAME TO %s", fullTable(TABLE), fullTable(RENAMED_TABLE)));
    assertTableEntityExists(TABLE, false);
    assertTableEntityExists(RENAMED_TABLE, true);

    // Drop removes the pointer row and the entity.
    spark.sql(String.format("DROP TABLE %s PURGE", fullTable(RENAMED_TABLE)));
    assertTableEntityExists(RENAMED_TABLE, false);
    spark.sql(String.format("DROP DATABASE %s", NAMESPACE));
  }

  private void initSparkEnv() {
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog." + SPARK_CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog." + SPARK_CATALOG + ".type", "rest")
            .set("spark.sql.catalog." + SPARK_CATALOG + ".uri", getIcebergRestServiceUri())
            // drop Iceberg table purge may hang in spark local mode
            .set("spark.locality.wait.node", "0");
    spark = SparkSession.builder().master("local[1]").config(sparkConf).getOrCreate();
  }

  private static String fullTable(String table) {
    return String.format("%s.%s", NAMESPACE, table);
  }

  private void assertTableEntityExists(String table, boolean expected) {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE_NAME);
    boolean exists =
        metalake
            .loadCatalog(CATALOG_NAME)
            .asTableCatalog()
            .tableExists(NameIdentifier.of(NAMESPACE, table));
    Assertions.assertEquals(
        expected,
        exists,
        String.format(
            "Gravitino entity for table %s should %s exist", table, expected ? "" : "not"));
  }
}
