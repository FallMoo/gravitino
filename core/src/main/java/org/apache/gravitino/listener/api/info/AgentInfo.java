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
package org.apache.gravitino.listener.api.info;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * AgentInfo exposes agent information for event listeners. It is read-only. Mirrors FunctionInfo
 * but carries the Agent-specific shape (endpoint / capabilities / protocols / owner).
 */
@DeveloperApi
public final class AgentInfo {
  private final String name;
  private final String endpoint;
  private final String[] capabilities;
  private final AgentProtocol[] protocols;
  private final String owner;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs an {@code AgentInfo} from an {@link Agent}.
   *
   * @param agent The source Agent.
   */
  public AgentInfo(Agent agent) {
    this(
        agent.name(),
        agent.endpoint(),
        agent.capabilities(),
        agent.protocols(),
        agent.owner(),
        agent.comment(),
        agent.properties(),
        agent.auditInfo());
  }

  /**
   * Constructs an {@code AgentInfo} with all fields explicit.
   *
   * @param name Name of the agent.
   * @param endpoint Endpoint URL.
   * @param capabilities Capability tags.
   * @param protocols Protocol descriptors.
   * @param owner Logical owner.
   * @param comment Optional comment.
   * @param properties User-defined properties.
   * @param auditInfo Optional audit info.
   */
  public AgentInfo(
      String name,
      String endpoint,
      String[] capabilities,
      AgentProtocol[] protocols,
      String owner,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      @Nullable Audit auditInfo) {
    this.name = name;
    this.endpoint = endpoint;
    this.capabilities =
        capabilities == null ? new String[0] : Arrays.copyOf(capabilities, capabilities.length);
    this.protocols =
        protocols == null ? new AgentProtocol[0] : Arrays.copyOf(protocols, protocols.length);
    this.owner = owner;
    this.comment = comment;
    this.properties = properties == null ? Collections.emptyMap() : new HashMap<>(properties);
    this.auditInfo = auditInfo;
  }

  /**
   * @return Agent name.
   */
  public String name() {
    return name;
  }

  /**
   * @return Endpoint URL.
   */
  public String endpoint() {
    return endpoint;
  }

  /**
   * @return Capability tags (defensive copy).
   */
  public String[] capabilities() {
    return Arrays.copyOf(capabilities, capabilities.length);
  }

  /**
   * @return Protocol descriptors (defensive copy).
   */
  public AgentProtocol[] protocols() {
    return Arrays.copyOf(protocols, protocols.length);
  }

  /**
   * @return Logical owner.
   */
  public String owner() {
    return owner;
  }

  /**
   * @return Comment, or {@code null} if absent.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * @return User-defined properties (unmodifiable view).
   */
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(properties);
  }

  /**
   * @return Audit info, or {@code null} if absent.
   */
  @Nullable
  public Audit auditInfo() {
    return auditInfo;
  }
}
