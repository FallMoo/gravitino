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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Persistent object for one row in the {@code agent_meta} table. The JSON-array fields
 * (capabilities, protocols, properties) are serialized as strings; the full PO ↔ Entity conversion
 * methods will be added in Phase 1 Subtask #10 / #11 (AgentProtocolDTO + DTOConverters), since they
 * require the Jackson-annotated DTO layer to round-trip {@code AgentProtocol} (an interface).
 *
 * <p>The shape mirrors {@link FunctionPO} — Lombok {@code @Builder} with {@code with}-prefixed
 * setters, immutable fields, hand-written constructor that validates required arguments.
 */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class AgentPO {

  private static final Integer INITIAL_VERSION = 1;

  private Long agentId;

  private String agentName;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private String endpoint;

  private String capabilities;

  private String protocols;

  private String owner;

  private String agentLatestVersion;

  private String agentCurrentVersion;

  private String auditInfo;

  private Long deletedAt;

  private AgentVersionPO agentVersionPO;

  public AgentPO() {}

  @lombok.Builder(setterPrefix = "with")
  private AgentPO(
      Long agentId,
      String agentName,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String endpoint,
      String capabilities,
      String protocols,
      String owner,
      String agentLatestVersion,
      String agentCurrentVersion,
      String auditInfo,
      Long deletedAt,
      AgentVersionPO agentVersionPO) {
    Preconditions.checkArgument(agentId != null, "Agent id is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(agentName), "Agent name cannot be empty");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(endpoint), "Endpoint cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(owner), "Owner cannot be empty");
    Preconditions.checkArgument(agentLatestVersion != null, "Agent latest version is required");
    Preconditions.checkArgument(agentCurrentVersion != null, "Agent current version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.agentId = agentId;
    this.agentName = agentName;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.endpoint = endpoint;
    this.capabilities = capabilities;
    this.protocols = protocols;
    this.owner = owner;
    this.agentLatestVersion = agentLatestVersion;
    this.agentCurrentVersion = agentCurrentVersion;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
    this.agentVersionPO = agentVersionPO;
  }

  public static Integer initialVersion() {
    return INITIAL_VERSION;
  }
}
