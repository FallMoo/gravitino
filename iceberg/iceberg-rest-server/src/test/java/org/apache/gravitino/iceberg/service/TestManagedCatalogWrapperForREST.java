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
package org.apache.gravitino.iceberg.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.ManagedIcebergCatalogOps;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Test;

/** Verifies that the managed REST wrapper delegates to the managed operations and nothing else. */
public class TestManagedCatalogWrapperForREST {

  private static final String CATALOG_NAME = "managed_catalog";

  private static IcebergConfig managedConfig() {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND,
            "jdbc",
            IcebergConstants.CATALOG_BACKEND_MODE,
            "managed",
            IcebergConstants.WAREHOUSE,
            "/tmp/managed-wrapper-test"));
  }

  private static ManagedCatalogWrapperForREST wrapperFor(ManagedIcebergCatalogOps ops) {
    return new ManagedCatalogWrapperForREST(CATALOG_NAME, managedConfig(), () -> ops);
  }

  @Test
  public void testTableOperationsDelegateToManagedOps() {
    ManagedIcebergCatalogOps ops = mock(ManagedIcebergCatalogOps.class);
    ManagedCatalogWrapperForREST wrapper = wrapperFor(ops);
    Namespace namespace = Namespace.of("db");
    TableIdentifier identifier = TableIdentifier.of(namespace, "t1");

    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
    LoadTableResponse expected =
        LoadTableResponse.builder()
            .withTableMetadata(
                org.apache.iceberg.TableMetadata.newTableMetadata(
                    schema,
                    org.apache.iceberg.PartitionSpec.unpartitioned(),
                    org.apache.iceberg.SortOrder.unsorted(),
                    "/tmp/t1",
                    java.util.Map.of()))
            .build();
    when(ops.createTable(eq(namespace), any(CreateTableRequest.class))).thenReturn(expected);
    when(ops.loadTable(identifier)).thenReturn(expected);
    when(ops.updateTable(eq(identifier), any(UpdateTableRequest.class))).thenReturn(expected);
    when(ops.registerTable(eq(namespace), any(RegisterTableRequest.class))).thenReturn(expected);
    when(ops.tableExists(identifier)).thenReturn(true);
    ListTablesResponse listed = ListTablesResponse.builder().build();
    when(ops.listTables(namespace)).thenReturn(listed);
    when(ops.metadataLocation(identifier)).thenReturn(Optional.of("s3://bucket/meta.json"));

    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withName("t1")
            .withSchema(schema)
            .withPartitionSpec(org.apache.iceberg.PartitionSpec.unpartitioned())
            .withWriteOrder(org.apache.iceberg.SortOrder.unsorted())
            .build();
    assertEquals(expected, wrapper.createTable(namespace, createRequest));
    assertEquals(expected, wrapper.createTable(namespace, createRequest, true /* credential */));
    assertEquals(expected, wrapper.loadTable(identifier));
    assertEquals(expected, wrapper.loadTable(identifier, true, CredentialPrivilege.READ));
    assertEquals(expected, wrapper.updateTable(identifier, mock(UpdateTableRequest.class)));
    assertEquals(expected, wrapper.registerTable(namespace, mock(RegisterTableRequest.class)));
    assertTrue(wrapper.tableExists(identifier));
    assertSame(listed, wrapper.listTable(namespace));
    assertEquals(
        Optional.of("s3://bucket/meta.json"), wrapper.getTableMetadataLocation(identifier));

    RenameTableRequest renameRequest =
        RenameTableRequest.builder()
            .withSource(identifier)
            .withDestination(TableIdentifier.of(namespace, "t2"))
            .build();
    wrapper.renameTable(renameRequest);
    verify(ops).renameTable(identifier, TableIdentifier.of(namespace, "t2"));

    wrapper.dropTable(identifier);
    verify(ops).dropTable(identifier, false);
    wrapper.purgeTable(identifier);
    verify(ops).dropTable(identifier, true);
  }

  @Test
  public void testNamespaceOperationsDelegateToManagedOps() {
    ManagedIcebergCatalogOps ops = mock(ManagedIcebergCatalogOps.class);
    ManagedCatalogWrapperForREST wrapper = wrapperFor(ops);
    Namespace namespace = Namespace.of("db");

    org.apache.iceberg.rest.requests.CreateNamespaceRequest createRequest =
        org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
            .withNamespace(namespace)
            .build();
    org.apache.iceberg.rest.responses.CreateNamespaceResponse created =
        org.apache.iceberg.rest.responses.CreateNamespaceResponse.builder()
            .withNamespace(namespace)
            .build();
    org.apache.iceberg.rest.responses.GetNamespaceResponse loaded =
        org.apache.iceberg.rest.responses.GetNamespaceResponse.builder()
            .withNamespace(namespace)
            .build();
    org.apache.iceberg.rest.responses.ListNamespacesResponse listed =
        org.apache.iceberg.rest.responses.ListNamespacesResponse.builder().build();
    org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse updated =
        org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse.builder().build();

    when(ops.createNamespace(createRequest)).thenReturn(created);
    when(ops.namespaceExists(namespace)).thenReturn(true);
    when(ops.loadNamespace(namespace)).thenReturn(loaded);
    when(ops.listNamespaces(namespace)).thenReturn(listed);
    when(ops.updateNamespaceProperties(eq(namespace), any())).thenReturn(updated);

    assertSame(created, wrapper.createNamespace(createRequest));
    assertTrue(wrapper.namespaceExists(namespace));
    assertSame(loaded, wrapper.loadNamespace(namespace));
    assertSame(listed, wrapper.listNamespace(namespace));
    assertSame(
        updated,
        wrapper.updateNamespaceProperties(
            namespace,
            mock(org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest.class)));

    wrapper.dropNamespace(namespace);
    verify(ops).dropNamespace(namespace);
  }

  @Test
  public void testSdkCatalogIsNeverInitialized() {
    ManagedCatalogWrapperForREST wrapper = wrapperFor(mock(ManagedIcebergCatalogOps.class));
    assertThrows(UnsupportedOperationException.class, wrapper::getCatalog);
  }

  @Test
  public void testViewsAreNotSupportedAndCredentialsAreEmpty() {
    ManagedCatalogWrapperForREST wrapper = wrapperFor(mock(ManagedIcebergCatalogOps.class));
    assertFalse(wrapper.supportsViewOperations());
    assertTrue(
        wrapper
            .getTableCredentials(
                TableIdentifier.of(Namespace.of("db"), "t1"), CredentialPrivilege.READ)
            .credentials()
            .isEmpty());
    assertEquals(CATALOG_NAME, wrapper.catalogName());
  }
}
