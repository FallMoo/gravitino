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

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/**
 * {@code AgentNormalizeDispatcher} normalizes agent identifiers and namespaces by applying
 * case-sensitivity and other naming capabilities before delegating to the underlying dispatcher.
 * Mirrors {@code FunctionNormalizeDispatcher} but uses {@link Capability.Scope#AGENT}.
 */
public class AgentNormalizeDispatcher implements AgentDispatcher {
  private final CatalogManager catalogManager;
  private final AgentDispatcher dispatcher;

  public AgentNormalizeDispatcher(AgentDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listAgents(Namespace namespace) throws NoSuchSchemaException {
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listAgents(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public Agent getAgent(NameIdentifier ident) throws NoSuchAgentException {
    return dispatcher.getAgent(normalizeCaseSensitive(ident));
  }

  @Override
  public boolean agentExists(NameIdentifier ident) {
    return dispatcher.agentExists(normalizeCaseSensitive(ident));
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
    return dispatcher.registerAgent(
        normalizeNameIdentifier(ident),
        endpoint,
        capabilities,
        protocols,
        comment,
        properties,
        owner);
  }

  @Override
  public Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException {
    return dispatcher.alterAgent(normalizeCaseSensitive(ident), changes);
  }

  @Override
  public boolean dropAgent(NameIdentifier ident) {
    return dispatcher.dropAgent(normalizeCaseSensitive(ident));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.AGENT, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier agentIdent) {
    Capability capabilities = getCapability(agentIdent, catalogManager);
    return applyCaseSensitive(agentIdent, Capability.Scope.AGENT, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] agentIdents) {
    if (agentIdents == null || agentIdents.length == 0) {
      return agentIdents;
    }

    Capability capabilities = getCapability(agentIdents[0], catalogManager);
    return applyCaseSensitive(agentIdents, Capability.Scope.AGENT, capabilities);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier agentIdent) {
    Capability capability = getCapability(agentIdent, catalogManager);
    return applyCapabilities(agentIdent, Capability.Scope.AGENT, capability);
  }
}
