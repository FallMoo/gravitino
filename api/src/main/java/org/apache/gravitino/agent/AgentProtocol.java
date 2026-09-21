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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * Describes a protocol that an agent speaks. The {@code type} is a free-form string identifier of
 * the protocol family (for example {@code A2A} or {@code MCP}); Gravitino does not maintain an
 * enumeration of protocol types and does not validate the value. The optional {@code agentCardUrl}
 * points at a protocol descriptor (for example an A2A agent card) but Gravitino does not fetch or
 * interpret it.
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
}
