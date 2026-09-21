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
package org.apache.gravitino.catalog;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.storage.IdGenerator;

/**
 * {@code AgentOperationDispatcher} is responsible for dispatching agent-related operations. Agents
 * are registered under schemas managed by the underlying catalog (e.g., Hive, Iceberg), so this
 * dispatcher validates schema existence by calling the underlying catalog's schema operations, then
 * delegates actual storage operations to {@link ManagedAgentOperations}. Mirrors {@code
 * FunctionOperationDispatcher}.
 *
 * <p>Note: unlike Function, Agent has no per-catalog-provider reserved schema (e.g. Iceberg's
 * "system" schema is a Function-specific concern). The {@code validateNotIcebergReservedSchema}
 * check is intentionally omitted.
 */
public class AgentOperationDispatcher extends OperationDispatcher implements AgentDispatcher {

  private final SchemaOperationDispatcher schemaOps;
  private final ManagedAgentOperations managedAgentOps;

  public AgentOperationDispatcher(
      CatalogManager catalogManager,
      SchemaOperationDispatcher schemaOps,
      EntityStore store,
      IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
    this.schemaOps = schemaOps;
    this.managedAgentOps = new ManagedAgentOperations(store, idGenerator);
  }

  @Override
  public NameIdentifier[] listAgents(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    // Validate schema exists in the underlying catalog
    schemaOps.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        schemaIdent, LockType.READ, () -> managedAgentOps.listAgents(namespace));
  }

  @Override
  public Agent getAgent(NameIdentifier ident) throws NoSuchAgentException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaOps.schemaExists(schemaIdent)) {
      throw new NoSuchAgentException("Schema does not exist: %s", schemaIdent);
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.READ, () -> managedAgentOps.getAgent(ident));
  }

  @Override
  public Agent registerAgent(
      NameIdentifier ident,
      String endpoint,
      @Nullable String[] capabilities,
      @Nullable AgentProtocol[] protocols,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      String owner)
      throws NoSuchSchemaException, AgentAlreadyExistsException {

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    // Validate schema exists in the underlying catalog
    schemaOps.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            managedAgentOps.registerAgent(
                ident, endpoint, capabilities, protocols, comment, properties, owner));
  }

  @Override
  public Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException {

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaOps.schemaExists(schemaIdent)) {
      throw new NoSuchAgentException("Schema does not exist: %s", schemaIdent);
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.WRITE, () -> managedAgentOps.alterAgent(ident, changes));
  }

  @Override
  public boolean dropAgent(NameIdentifier ident) {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaOps.schemaExists(schemaIdent)) {
      return false;
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.WRITE, () -> managedAgentOps.dropAgent(ident));
  }
}
