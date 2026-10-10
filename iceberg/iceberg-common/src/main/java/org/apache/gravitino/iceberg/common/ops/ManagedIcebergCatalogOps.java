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
package org.apache.gravitino.iceberg.common.ops;

import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

/**
 * The Iceberg REST entry's view of a Gravitino-managed Iceberg catalog (design doc section 5.3).
 *
 * <p>It is deliberately expressed in Iceberg types only: the REST layer must not see Gravitino
 * identifiers, and the implementation lives in the {@code catalog-lakehouse-iceberg} module while
 * the REST entry lives in {@code iceberg-rest-server}. This interface sits in {@code
 * iceberg-common} because that module is a dependency of both, and because {@code
 * IsolatedClassLoader} treats classes outside {@code org.apache.gravitino.catalog.*} as shared, so
 * the interface is loaded by the server class loader and an {@code instanceof} check across the
 * catalog isolation boundary works.
 *
 * <p>Implementations are expected to be stateless with respect to a single request; the catalog
 * name and metalake are bound when the catalog is initialized.
 */
public interface ManagedIcebergCatalogOps {

  /**
   * Creates a table, or, when the request is a staged create, only builds the metadata without
   * persisting anything (design doc section 5.9.1).
   *
   * @param namespace the Iceberg namespace that holds the table
   * @param request the REST create request
   * @return the metadata of the created (or staged) table
   */
  LoadTableResponse createTable(Namespace namespace, CreateTableRequest request);

  /**
   * Loads a table.
   *
   * @param identifier the table identifier
   * @return the current table metadata
   */
  LoadTableResponse loadTable(TableIdentifier identifier);

  /**
   * Applies a REST commit request.
   *
   * @param identifier the table identifier
   * @param request the requirements and updates
   * @return the committed table metadata
   */
  LoadTableResponse updateTable(TableIdentifier identifier, UpdateTableRequest request);

  /**
   * Registers an existing metadata file as a table.
   *
   * @param namespace the Iceberg namespace that holds the table
   * @param request the REST register request
   * @return the registered table metadata
   */
  LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request);

  /**
   * Lists the tables of a namespace.
   *
   * @param namespace the Iceberg namespace
   * @return the table identifiers in that namespace
   */
  ListTablesResponse listTables(Namespace namespace);

  /**
   * @param identifier the table identifier
   * @return whether the table exists
   */
  boolean tableExists(TableIdentifier identifier);

  /**
   * Drops a table.
   *
   * @param identifier the table identifier
   * @param purge whether the table data should be deleted as well
   */
  void dropTable(TableIdentifier identifier, boolean purge);

  /**
   * Renames a table.
   *
   * @param from the current identifier
   * @param to the new identifier
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * @param identifier the table identifier
   * @return the current metadata file location, or empty when the table does not exist
   */
  Optional<String> metadataLocation(TableIdentifier identifier);

  /**
   * Creates a namespace and its Gravitino schema entity in one transaction (design doc section
   * 5.11).
   *
   * @param request the REST create request
   * @return the created namespace with its stored properties
   */
  CreateNamespaceResponse createNamespace(CreateNamespaceRequest request);

  /**
   * Drops a namespace. The namespace must be empty.
   *
   * @param namespace the namespace to drop
   */
  void dropNamespace(Namespace namespace);

  /**
   * Loads a namespace.
   *
   * @param namespace the namespace
   * @return the namespace with its properties
   */
  GetNamespaceResponse loadNamespace(Namespace namespace);

  /**
   * @param namespace the namespace
   * @return whether the namespace exists
   */
  boolean namespaceExists(Namespace namespace);

  /**
   * Lists the namespaces directly under a parent namespace.
   *
   * @param parent the parent namespace, or the empty namespace for the top level
   * @return the child namespaces
   */
  ListNamespacesResponse listNamespaces(Namespace parent);

  /**
   * Applies a REST namespace property update.
   *
   * @param namespace the namespace
   * @param request the removals and updates
   * @return the applied removals and the resulting properties
   */
  UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request);
}
