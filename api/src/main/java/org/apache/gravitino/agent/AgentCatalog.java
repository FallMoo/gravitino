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
package org.apache.gravitino.agent;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/**
 * The AgentCatalog interface defines the public API for managing {@link Agent agents} in a schema.
 * The shape mirrors {@link org.apache.gravitino.model.ModelCatalog}: list / get / exists / register
 * / alter / drop, with no separate "list with details" method (callers fetch each agent
 * individually if they want details). Agents have no versioning, so there is no equivalent to
 * ModelCatalog's version methods.
 *
 * <p>This interface is the canonical capability surfaced via {@code Catalog.asAgentCatalog()}; the
 * server-side {@code ManagedAgentOperations} implements it directly, while the Java client side
 * wraps it as a thin REST proxy.
 */
@Evolving
public interface AgentCatalog {

  /**
   * List the agents in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of agent identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listAgents(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Get an agent metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident An agent identifier.
   * @return The agent metadata.
   * @throws NoSuchAgentException If the agent does not exist.
   */
  Agent getAgent(NameIdentifier ident) throws NoSuchAgentException;

  /**
   * Check if an agent exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident An agent identifier.
   * @return true If the agent exists, false if the agent does not exist.
   */
  default boolean agentExists(NameIdentifier ident) {
    try {
      getAgent(ident);
      return true;
    } catch (NoSuchAgentException e) {
      return false;
    }
  }

  /**
   * Register an agent in the catalog.
   *
   * @param ident The name identifier of the agent.
   * @param endpoint The agent endpoint URL (opaque string). Gravitino does not validate or invoke
   *     it.
   * @param capabilities Optional capability tags. {@code null} is normalized to an empty array (v1
   *     does not distinguish 'unset' from 'empty').
   * @param protocols Optional ordered protocol descriptors. Within the resulting agent, {@link
   *     AgentProtocol#type()} values must be unique.
   * @param comment The comment of the agent. May be {@code null}.
   * @param properties The properties of the agent. May be {@code null} or empty.
   * @param owner The logical owner of the agent. Must be non-blank.
   * @return The registered agent object.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws AgentAlreadyExistsException If the agent already exists.
   */
  Agent registerAgent(
      NameIdentifier ident,
      String endpoint,
      @Nullable String[] capabilities,
      @Nullable AgentProtocol[] protocols,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      String owner)
      throws NoSuchSchemaException, AgentAlreadyExistsException;

  /**
   * Delete the agent from the catalog. If the agent does not exist, return false. Otherwise, return
   * true.
   *
   * @param ident The name identifier of the agent.
   * @return True if the agent is deleted, false if the agent does not exist.
   */
  boolean dropAgent(NameIdentifier ident);

  /**
   * Applies the {@link AgentChange changes} to an agent in the catalog.
   *
   * <p>Implementations may reject the changes. If any change is rejected, no changes should be
   * applied to the agent.
   *
   * @param ident the {@link NameIdentifier} instance of the agent to alter.
   * @param changes the several {@link AgentChange} instances to apply to the agent.
   * @return the updated {@link Agent} instance.
   * @throws NoSuchAgentException If the agent does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException;
}
