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
package org.apache.gravitino.listener;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.catalog.AgentDispatcher;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.listener.api.event.agent.AlterAgentEvent;
import org.apache.gravitino.listener.api.event.agent.AlterAgentFailureEvent;
import org.apache.gravitino.listener.api.event.agent.AlterAgentPreEvent;
import org.apache.gravitino.listener.api.event.agent.DropAgentEvent;
import org.apache.gravitino.listener.api.event.agent.DropAgentFailureEvent;
import org.apache.gravitino.listener.api.event.agent.DropAgentPreEvent;
import org.apache.gravitino.listener.api.event.agent.GetAgentEvent;
import org.apache.gravitino.listener.api.event.agent.GetAgentFailureEvent;
import org.apache.gravitino.listener.api.event.agent.GetAgentPreEvent;
import org.apache.gravitino.listener.api.event.agent.ListAgentsEvent;
import org.apache.gravitino.listener.api.event.agent.ListAgentsFailureEvent;
import org.apache.gravitino.listener.api.event.agent.ListAgentsPreEvent;
import org.apache.gravitino.listener.api.event.agent.RegisterAgentEvent;
import org.apache.gravitino.listener.api.event.agent.RegisterAgentFailureEvent;
import org.apache.gravitino.listener.api.event.agent.RegisterAgentPreEvent;
import org.apache.gravitino.listener.api.info.AgentInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code AgentEventDispatcher} dispatches Pre/Success/Failure events around every Agent operation.
 * Mirrors {@code FunctionEventDispatcher} but does NOT fire any event for {@code agentExists}
 * (consistent with Function's {@code functionExists} passthrough behavior).
 */
public class AgentEventDispatcher implements AgentDispatcher {

  private final EventBus eventBus;
  private final AgentDispatcher dispatcher;

  public AgentEventDispatcher(EventBus eventBus, AgentDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listAgents(Namespace namespace) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListAgentsPreEvent(user, namespace));
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listAgents(namespace);
      eventBus.dispatchEvent(
          new ListAgentsEvent(
              user, namespace, nameIdentifiers != null ? nameIdentifiers.length : -1));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListAgentsFailureEvent(user, namespace, e));
      throw e;
    }
  }

  @Override
  public Agent getAgent(NameIdentifier ident) throws NoSuchAgentException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new GetAgentPreEvent(user, ident));
    try {
      Agent agent = dispatcher.getAgent(ident);
      eventBus.dispatchEvent(new GetAgentEvent(user, ident, new AgentInfo(agent)));
      return agent;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetAgentFailureEvent(user, ident, e));
      throw e;
    }
  }

  @Override
  public boolean agentExists(NameIdentifier ident) {
    // Match FunctionEventDispatcher: functionExists does not fire any event.
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

    String user = PrincipalUtils.getCurrentUserName();
    AgentInfo registerRequest =
        new AgentInfo(
            ident.name(),
            endpoint,
            capabilities,
            protocols,
            owner,
            comment,
            properties,
            null /* no audit yet */);
    eventBus.dispatchEvent(new RegisterAgentPreEvent(user, ident, registerRequest));
    try {
      Agent agent =
          dispatcher.registerAgent(
              ident, endpoint, capabilities, protocols, comment, properties, owner);
      eventBus.dispatchEvent(new RegisterAgentEvent(user, ident, new AgentInfo(agent)));
      return agent;
    } catch (Exception e) {
      eventBus.dispatchEvent(new RegisterAgentFailureEvent(user, ident, e, registerRequest));
      throw e;
    }
  }

  @Override
  public Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new AlterAgentPreEvent(user, ident, changes));
    try {
      Agent agent = dispatcher.alterAgent(ident, changes);
      eventBus.dispatchEvent(new AlterAgentEvent(user, ident, changes, new AgentInfo(agent)));
      return agent;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AlterAgentFailureEvent(user, ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropAgent(NameIdentifier ident) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new DropAgentPreEvent(user, ident));
    try {
      boolean isExists = dispatcher.dropAgent(ident);
      eventBus.dispatchEvent(new DropAgentEvent(user, ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DropAgentFailureEvent(user, ident, e));
      throw e;
    }
  }
}
