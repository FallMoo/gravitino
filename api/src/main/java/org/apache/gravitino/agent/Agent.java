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

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing an AI agent under a schema. An agent is a metadata record of an
 * autonomous or semi-autonomous software service (typically backed by an LLM) that Gravitino tracks
 * for governance purposes. Gravitino does not invoke or proxy agent requests; it only stores agent
 * metadata (endpoint, capabilities, protocols, owner, etc.) so that the rest of the governance
 * stack (tags, policies, roles, audit) applies uniformly.
 *
 * <p>An agent is registered in Gravitino by registering it under a schema within a catalog, and can
 * then be discovered, tagged, governed, and audited using the standard Gravitino metadata object
 * lifecycle.
 *
 * <p>The shape of the interface mirrors {@link org.apache.gravitino.model.Model}: the optional
 * mixins are exposed via {@code default} accessors that throw {@link UnsupportedOperationException}
 * when the underlying implementation does not support that capability. This keeps the interface
 * usable in environments where one or more of the mixins are disabled.
 */
@Evolving
public interface Agent extends Auditable {

  /**
   * @return The name of the agent.
   */
  String name();

  /**
   * @return The endpoint URL of the agent (opaque string). Gravitino does not validate or invoke
   *     this URL.
   */
  String endpoint();

  /**
   * @return The optional capability tags (free-form strings such as {@code text2sql}). Returns an
   *     empty array if none are declared; callers should not depend on a {@code null} return.
   */
  default String[] capabilities() {
    return new String[0];
  }

  /**
   * @return The optional ordered list of protocol descriptors. Within a single agent, {@link
   *     AgentProtocol#type()} values must be unique.
   */
  default AgentProtocol[] protocols() {
    return new AgentProtocol[0];
  }

  /**
   * @return The optional logical owner (typically a username).
   */
  String owner();

  /**
   * @return The optional comment of the agent.
   */
  default String comment() {
    return null;
  }

  /**
   * @return The user-defined properties of the agent. Returns an empty map if none are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * @return The {@link SupportsTags} if the agent supports tag operations.
   * @throws UnsupportedOperationException If the agent does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Agent does not support tag operations.");
  }

  /**
   * @return The {@link SupportsPolicies} if the agent supports policy operations.
   * @throws UnsupportedOperationException If the agent does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Agent does not support policy operations.");
  }

  /**
   * @return The {@link SupportsRoles} if the agent supports role operations.
   * @throws UnsupportedOperationException If the agent does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Agent does not support role operations.");
  }
}
