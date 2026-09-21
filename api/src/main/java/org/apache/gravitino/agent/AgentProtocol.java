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
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * Describes a protocol that an agent speaks. The {@code type} is a free-form string identifier of
 * the protocol family (for example {@code A2A} or {@code MCP}); Gravitino does not maintain an
 * enumeration of protocol types and does not validate the value. The optional {@code agentCardUrl}
 * points at a protocol descriptor (for example an A2A agent card) but Gravitino does not fetch or
 * interpret it.
 *
 * <p>{@link #properties()} is an optional per-protocol extension slot (e.g. {@code auth}, {@code
 * preferredTransport}, {@code version}). Gravitino does not interpret these keys; they are stored
 * verbatim and surfaced to consumers as-is. The scope is intentionally narrower than the
 * agent-level {@link Agent#properties()} map: per-agent keys cover whole-agent operations (e.g.
 * rate limits), while per-protocol keys cover protocol-connection details.
 */
@Evolving
public interface AgentProtocol {

  /**
   * @return The protocol family identifier (free-form string).
   */
  String type();

  /**
   * @return The optional URL of the protocol descriptor (e.g. an A2A agent card).
   */
  @Nullable
  String agentCardUrl();

  /**
   * @return Optional per-protocol metadata. Returns an empty map if no protocol-level properties
   *     are set; callers should not depend on a {@code null} return. The scope is intentionally
   *     narrower than {@link Agent#properties()}: this map covers protocol-connection details such
   *     as authentication scheme or transport hints, while the agent-level map covers whole-agent
   *     operations.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
