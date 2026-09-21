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
package org.apache.gravitino.hook;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.AgentDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code AgentHookDispatcher} is a decorator for {@link AgentDispatcher} that delegates agent
 * operations to the underlying dispatcher and executes hook operations around them. Currently the
 * only hook is to attach the creator as the owner of the agent on register; future hooks (e.g. tag
 * / policy auto-attach) can land here. Mirrors {@code FunctionHookDispatcher}.
 */
public class AgentHookDispatcher implements AgentDispatcher {
  private final AgentDispatcher dispatcher;

  public AgentHookDispatcher(AgentDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listAgents(Namespace namespace) {
    return dispatcher.listAgents(namespace);
  }

  @Override
  public Agent getAgent(NameIdentifier ident) throws NoSuchAgentException {
    return dispatcher.getAgent(ident);
  }

  @Override
  public boolean agentExists(NameIdentifier ident) {
    return dispatcher.agentExists(ident);
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
      throws AgentAlreadyExistsException {

    Agent agent =
        dispatcher.registerAgent(
            ident, endpoint, capabilities, protocols, comment, properties, owner);

    // Set the creator as the owner of the agent.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      // The inner NormalizeDispatcher case-folds the agent name (and its schema namespace)
      // based on catalog capabilities, so the entity is stored under the normalized identifier.
      // Apply the same normalization here so the owner is attached to the same identifier the
      // manager sees.
      NameIdentifier normalizedIdent =
          CapabilityHelpers.applyCapabilities(
              ident, Capability.Scope.AGENT, GravitinoEnv.getInstance().catalogManager());
      ownerManager.setOwner(
          normalizedIdent.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(normalizedIdent, Entity.EntityType.AGENT),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return agent;
  }

  @Override
  public Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException {
    return dispatcher.alterAgent(ident, changes);
  }

  @Override
  public boolean dropAgent(NameIdentifier ident) {
    return dispatcher.dropAgent(ident);
  }
}
