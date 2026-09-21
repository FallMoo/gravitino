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
package org.apache.gravitino.dto.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.gravitino.Audit;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.dto.AuditDTO;

/** Represents an Agent DTO (Data Transfer Object). */
@NoArgsConstructor
@AllArgsConstructor
@Builder(setterPrefix = "with")
@EqualsAndHashCode
public class AgentDTO implements Agent {

  @JsonProperty("name")
  private String name;

  @JsonProperty("endpoint")
  private String endpoint;

  @JsonProperty("capabilities")
  private String[] capabilities;

  @JsonProperty("protocols")
  private AgentProtocolDTO[] protocols;

  @JsonProperty("owner")
  private String owner;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Override
  public String name() {
    return name;
  }

  @Override
  public String endpoint() {
    return endpoint;
  }

  @Override
  public String[] capabilities() {
    return capabilities == null ? new String[0] : capabilities;
  }

  @Override
  public AgentProtocol[] protocols() {
    return protocols == null ? new AgentProtocol[0] : protocols;
  }

  @Override
  public String owner() {
    return owner;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  @Override
  public String toString() {
    return "AgentDTO{"
        + "name='"
        + name
        + '\''
        + ", endpoint='"
        + endpoint
        + '\''
        + ", capabilities="
        + Arrays.toString(capabilities)
        + ", protocols="
        + Arrays.toString(protocols)
        + ", owner='"
        + owner
        + '\''
        + ", comment='"
        + comment
        + '\''
        + ", properties="
        + properties
        + ", audit="
        + audit
        + '}';
  }
}
