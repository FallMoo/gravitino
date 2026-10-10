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

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Supplier;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.ManagedIcebergCatalogOps;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Iceberg REST wrapper of a Gravitino-managed Iceberg catalog (design doc sections 5.3 and
 * 5.9.1).
 *
 * <p>It inherits the credential and scan-plan plumbing of {@link CatalogWrapperForREST} but never
 * touches the Iceberg SDK catalog: every table operation is delegated to the managed implementation
 * layer through {@link ManagedIcebergCatalogOps}. The SDK catalog of the parent is therefore never
 * initialized, which is exactly the point of managed mode - Gravitino, not the Iceberg JDBC
 * catalog, owns the metadata.
 */
public class ManagedCatalogWrapperForREST extends CatalogWrapperForREST {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedCatalogWrapperForREST.class);

  private final String catalogName;
  private final Supplier<ManagedIcebergCatalogOps> opsSupplier;
  private volatile ManagedIcebergCatalogOps ops;

  /**
   * @param catalogName the Gravitino catalog name (it must match the REST catalog name)
   * @param config the Iceberg REST catalog configuration
   */
  public ManagedCatalogWrapperForREST(String catalogName, IcebergConfig config) {
    this(catalogName, config, () -> loadManagedOps(catalogName));
  }

  @VisibleForTesting
  ManagedCatalogWrapperForREST(
      String catalogName, IcebergConfig config, Supplier<ManagedIcebergCatalogOps> opsSupplier) {
    super(catalogName, config);
    this.catalogName = catalogName;
    this.opsSupplier = opsSupplier;
    LOG.info("Created a managed Iceberg REST wrapper for catalog {}", catalogName);
  }

  /**
   * Resolves the managed operations of the Gravitino catalog. The catalog instance is loaded
   * through {@link CatalogManager}, so the wrapper shares the entity store and the configuration of
   * the catalog the REST prefix addresses.
   */
  private static ManagedIcebergCatalogOps loadManagedOps(String catalogName) {
    String metalakeName = IcebergRESTServerContext.getInstance().metalakeName();
    BaseCatalog<?> catalog =
        GravitinoEnv.getInstance()
            .catalogManager()
            .loadCatalogAndWrap(NameIdentifier.of(metalakeName, catalogName))
            .catalog();
    CatalogOperations catalogOps = catalog.ops();
    if (!(catalogOps instanceof ManagedIcebergCatalogOps)) {
      throw new IllegalStateException(
          String.format(
              "Catalog %s is configured with catalog-backend-mode=managed but its operations (%s) do"
                  + " not implement ManagedIcebergCatalogOps",
              catalogName, catalogOps.getClass().getName()));
    }
    return (ManagedIcebergCatalogOps) catalogOps;
  }

  private ManagedIcebergCatalogOps managedOps() {
    ManagedIcebergCatalogOps loaded = ops;
    if (loaded == null) {
      synchronized (this) {
        if (ops == null) {
          ops = opsSupplier.get();
        }
        loaded = ops;
      }
    }
    return loaded;
  }

  @Override
  public Catalog getCatalog() {
    throw new UnsupportedOperationException(
        "The Iceberg REST endpoint does not expose the Iceberg SDK catalog of a managed catalog");
  }

  @Override
  public LoadTableResponse createTable(Namespace namespace, CreateTableRequest request) {
    return managedOps().createTable(namespace, request);
  }

  @Override
  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    // Managed catalogs do not vend credentials: Gravitino is the only writer of the metadata.
    return createTable(namespace, request);
  }

  @Override
  public LoadTableResponse loadTable(TableIdentifier identifier) {
    return managedOps().loadTable(identifier);
  }

  @Override
  public LoadTableResponse loadTable(
      TableIdentifier identifier, boolean requestCredential, CredentialPrivilege privilege) {
    return loadTable(identifier);
  }

  @Override
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    return managedOps().registerTable(namespace, request);
  }

  @Override
  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    return registerTable(namespace, request);
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier identifier, UpdateTableRequest updateTableRequest) {
    return managedOps().updateTable(identifier, updateTableRequest);
  }

  @Override
  public void dropTable(TableIdentifier tableIdentifier) {
    managedOps().dropTable(tableIdentifier, false);
  }

  @Override
  public void purgeTable(TableIdentifier tableIdentifier) {
    managedOps().dropTable(tableIdentifier, true);
  }

  @Override
  public boolean tableExists(TableIdentifier tableIdentifier) {
    return managedOps().tableExists(tableIdentifier);
  }

  @Override
  public ListTablesResponse listTable(Namespace namespace) {
    return managedOps().listTables(namespace);
  }

  @Override
  public void renameTable(RenameTableRequest renameTableRequest) {
    managedOps().renameTable(renameTableRequest.source(), renameTableRequest.destination());
  }

  @Override
  public java.util.Optional<String> getTableMetadataLocation(TableIdentifier tableIdentifier) {
    return managedOps().metadataLocation(tableIdentifier);
  }

  @Override
  public TableMetadata loadTableMetadata(TableIdentifier tableIdentifier) {
    return managedOps().loadTable(tableIdentifier).tableMetadata();
  }

  @Override
  public LoadCredentialsResponse getTableCredentials(
      TableIdentifier identifier, CredentialPrivilege privilege) {
    // Managed catalogs store no JDBC/S3 credentials to hand out; clients keep using their own.
    return ImmutableLoadCredentialsResponse.builder().build();
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    return managedOps().createNamespace(request);
  }

  @Override
  public void dropNamespace(Namespace namespace) {
    managedOps().dropNamespace(namespace);
  }

  @Override
  public GetNamespaceResponse loadNamespace(Namespace namespace) {
    return managedOps().loadNamespace(namespace);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return managedOps().namespaceExists(namespace);
  }

  @Override
  public ListNamespacesResponse listNamespace(Namespace parent) {
    return managedOps().listNamespaces(parent);
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    return managedOps().updateNamespaceProperties(namespace, request);
  }

  @Override
  public boolean supportsViewOperations() {
    return false;
  }

  /**
   * @return the catalog name this wrapper was created for
   */
  public String catalogName() {
    return catalogName;
  }
}
