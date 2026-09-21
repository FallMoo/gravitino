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
 * Persistent object for one row in the {@code agent_version_info} table. Each agent alter writes a
 * new version row carrying the comment, properties snapshot, and audit info at that point in time.
 */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class AgentVersionPO {

  private Long id;

  private Long agentId;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private Integer agentVersion;

  private String agentComment;

  private String agentProperties;

  private String auditInfo;

  private Long deletedAt;

  public AgentVersionPO() {}

  @lombok.Builder(setterPrefix = "with")
  private AgentVersionPO(
      Long id,
      Long agentId,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Integer agentVersion,
      String agentComment,
      String agentProperties,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(agentId != null, "Agent id is required");
    Preconditions.checkArgument(agentVersion != null, "Agent version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.agentId = agentId;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.agentVersion = agentVersion;
    this.agentComment = agentComment;
    this.agentProperties = agentProperties;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }
}
