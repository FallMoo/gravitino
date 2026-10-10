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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackendMode;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogCapability;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the managed-storage capability (design doc section 8, test 4): {@code
 * managedStorage(TABLE/SCHEMA)} is {@code SUPPORTED} only when {@code catalog-backend-mode =
 * managed} and {@code catalog-backend = jdbc}.
 */
public class TestManagedCapability {

  private static Map<String, String> managedProperties() {
    return ImmutableMap.of(
        IcebergConstants.CATALOG_BACKEND_MODE, "managed",
        IcebergConstants.CATALOG_BACKEND, "jdbc",
        IcebergConstants.WAREHOUSE, "file:///tmp/warehouse");
  }

  @Test
  public void testManagedModeDeclaresManagedStorage() {
    IcebergCatalogCapability capability =
        new IcebergCatalogCapability(":", IcebergCatalogBackendMode.MANAGED);

    assertTrue(capability.managedStorage(Capability.Scope.TABLE).supported());
    assertTrue(capability.managedStorage(Capability.Scope.SCHEMA).supported());
    assertFalse(capability.managedStorage(Capability.Scope.VIEW).supported());
    assertFalse(capability.managedStorage(Capability.Scope.FILESET).supported());
  }

  @Test
  public void testFederatedModeKeepsTheDefaultBehaviour() {
    IcebergCatalogCapability capability =
        new IcebergCatalogCapability(":", IcebergCatalogBackendMode.FEDERATED);

    assertFalse(capability.managedStorage(Capability.Scope.TABLE).supported());
    assertFalse(capability.managedStorage(Capability.Scope.SCHEMA).supported());

    // The single-argument constructor stays federated, so existing behaviour is untouched.
    IcebergCatalogCapability defaultCapability = new IcebergCatalogCapability(":");
    assertFalse(defaultCapability.managedStorage(Capability.Scope.TABLE).supported());
  }

  @Test
  public void testCatalogReadsTheModeFromTheEntity() {
    IcebergCatalog managed = new IcebergCatalog();
    managed.withCatalogEntity(
        entityWithProperties(
            ImmutableMap.<String, String>builder()
                .putAll(managedProperties())
                .put(IcebergConstants.CATALOG_BACKEND_NAME, "managed-catalog")
                .build()));
    assertTrue(managed.newCapability().managedStorage(Capability.Scope.TABLE).supported());

    IcebergCatalog federated = new IcebergCatalog();
    federated.withCatalogEntity(
        entityWithProperties(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND, "hive",
                IcebergConstants.URI, "thrift://127.0.0.1:9083",
                IcebergConstants.WAREHOUSE, "file:///tmp/warehouse")));
    assertFalse(federated.newCapability().managedStorage(Capability.Scope.TABLE).supported());
  }

  private static CatalogEntity entityWithProperties(Map<String, String> properties) {
    return CatalogEntity.builder()
        .withId(1L)
        .withName("catalog")
        .withNamespace(Namespace.of("metalake"))
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("lakehouse-iceberg")
        .withComment("")
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
