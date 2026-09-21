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
package org.apache.gravitino.storage.relational.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AgentEntity;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.mapper.AgentMetaMapper;
import org.apache.gravitino.storage.relational.mapper.AgentVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.po.AgentPO;
import org.apache.gravitino.storage.relational.po.AgentVersionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * Service class for {@link AgentEntity} persistence. Mirrors {@code FunctionMetaService} in shape
 * but inlines the version-row construction because the Agent PO carries {@code capabilities} and
 * {@code protocols} on the main row, with the version row only tracking {@code comment} and {@code
 * properties} (the only mutable fields on alter).
 */
public class AgentMetaService {
  private static final AgentMetaService INSTANCE = new AgentMetaService();

  public static AgentMetaService getInstance() {
    return INSTANCE;
  }

  private AgentMetaService() {}

  // ---------- Public read API ----------

  public List<AgentEntity> listAgentsByNamespace(Namespace ns) {
    NamespaceUtil.checkAgent(ns);

    List<AgentPO> agentPOs =
        SessionUtils.getWithoutCommit(
            AgentMetaMapper.class, mapper -> mapper.listAgentPOsBySchemaId(getSchemaId(ns)));
    return agentPOs.stream()
        .map(po -> fromAgentPO(po, ns))
        .collect(ImmutableList.toImmutableList());
  }

  public AgentEntity getAgentByIdentifier(NameIdentifier ident) {
    AgentPO agentPO = getAgentPOByIdentifier(ident);
    return fromAgentPO(agentPO, ident.namespace());
  }

  // ---------- Public write API ----------

  public void insertAgent(AgentEntity agentEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkAgent(agentEntity.nameIdentifier());

    AgentPO.AgentPOBuilder builder = AgentPO.builder();
    fillAgentPOBuilderParentEntityId(builder, agentEntity.namespace());
    AgentPO po = initializeAgentPO(agentEntity, builder);

    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  AgentMetaMapper.class, mapper -> insertAgentRow(mapper, po, overwrite)),
          () ->
              SessionUtils.doWithoutCommit(
                  AgentVersionMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertAgentVersionMeta(po.agentVersionPO());
                    } else {
                      mapper.insertAgentVersionMeta(po.agentVersionPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.AGENT, agentEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public boolean deleteAgent(NameIdentifier ident) {
    AgentPO agentPO = getAgentPOByIdentifier(ident);
    Long agentId = agentPO.agentId();

    AtomicInteger agentDeletedCount = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        () ->
            agentDeletedCount.set(
                SessionUtils.getWithoutCommit(
                    AgentMetaMapper.class, mapper -> mapper.softDeleteAgentMetaByAgentId(agentId))),
        () -> {
          if (agentDeletedCount.get() > 0) {
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        agentId, MetadataObject.Type.AGENT.name()));
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        agentId, MetadataObject.Type.AGENT.name()));
          }
        });

    return agentDeletedCount.get() > 0;
  }

  public <E extends Entity & HasIdentifier> AgentEntity updateAgent(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    AgentPO oldAgentPO = getAgentPOByIdentifier(identifier);
    AgentEntity oldAgentEntity = fromAgentPO(oldAgentPO, identifier.namespace());
    AgentEntity newEntity = (AgentEntity) updater.apply((E) oldAgentEntity);
    Objects.requireNonNull(
        newEntity.id(),
        "Updater must preserve id (was "
            + oldAgentEntity.id()
            + ", got "
            + (newEntity.id() == null ? "null" : newEntity.id())
            + ")");

    try {
      AgentPO newAgentPO = buildUpdatedAgentPO(oldAgentPO, newEntity);
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  AgentVersionMetaMapper.class,
                  mapper -> mapper.insertAgentVersionMeta(newAgentPO.agentVersionPO())),
          () ->
              SessionUtils.doWithoutCommit(
                  AgentMetaMapper.class, mapper -> mapper.updateAgentMeta(newAgentPO, oldAgentPO)));
      return newEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.AGENT, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public int deleteAgentMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        AgentMetaMapper.class,
        mapper -> mapper.deleteAgentMetasByLegacyTimeline(legacyTimeline, limit));
  }

  // ---------- Internal helpers ----------

  AgentPO getAgentPOByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkAgent(ident);
    AgentPO agentPO =
        SessionUtils.getWithoutCommit(
            AgentMetaMapper.class,
            mapper ->
                mapper.selectAgentMetaBySchemaIdAndName(
                    getSchemaId(ident.namespace()), ident.name()));
    if (agentPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.AGENT.name().toLowerCase(Locale.ROOT),
          ident.name());
    }
    return agentPO;
  }

  private long getSchemaId(Namespace ns) {
    NamespaceUtil.checkAgent(ns);
    return EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA)
        .entityId();
  }

  private void fillAgentPOBuilderParentEntityId(AgentPO.AgentPOBuilder builder, Namespace ns) {
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  // ---------- PO ↔ Entity conversion (filled in here, NOT in AgentPO) ----------

  static AgentEntity fromAgentPO(AgentPO agentPO, Namespace namespace) {
    try {
      AgentVersionPO versionPO = agentPO.agentVersionPO();
      String[] capabilities =
          agentPO.capabilities() == null
              ? new String[0]
              : ((List<String>)
                      JsonUtils.anyFieldMapper()
                          .readValue(
                              agentPO.capabilities(),
                              JsonUtils.anyFieldMapper()
                                  .getTypeFactory()
                                  .constructCollectionType(List.class, String.class)))
                  .toArray(new String[0]);
      AgentProtocol[] protocols =
          agentPO.protocols() == null
              ? new AgentProtocol[0]
              : JsonUtils.anyFieldMapper()
                  .readValue(
                      agentPO.protocols(),
                      JsonUtils.anyFieldMapper()
                          .getTypeFactory()
                          .constructArrayType(AgentProtocol.class));
      Map<String, String> properties =
          versionPO.agentProperties() == null
              ? Collections.emptyMap()
              : JsonUtils.anyFieldMapper().readValue(versionPO.agentProperties(), Map.class);
      AuditInfo auditInfo =
          JsonUtils.anyFieldMapper().readValue(agentPO.auditInfo(), AuditInfo.class);
      return AgentEntity.builder()
          .withId(agentPO.agentId())
          .withName(agentPO.agentName())
          .withNamespace(namespace)
          .withEndpoint(agentPO.endpoint())
          .withCapabilities(capabilities)
          .withProtocols(protocols)
          .withOwner(agentPO.owner())
          .withComment(versionPO.agentComment())
          .withProperties(properties)
          .withAuditInfo(auditInfo)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  static AgentPO initializeAgentPO(AgentEntity agentEntity, AgentPO.AgentPOBuilder builder) {
    Integer initial = AgentPO.initialVersion();
    builder
        .withAgentLatestVersion(String.valueOf(initial))
        .withAgentCurrentVersion(String.valueOf(initial));
    return buildAgentPO(agentEntity, builder, initial);
  }

  private static AgentPO buildAgentPO(
      AgentEntity agentEntity, AgentPO.AgentPOBuilder builder, Integer version) {
    try {
      NamespacedEntityId namespacedEntityId =
          EntityIdService.getEntityIds(
              NameIdentifier.of(agentEntity.namespace().levels()), Entity.EntityType.SCHEMA);
      AgentVersionPO versionPO =
          AgentVersionPO.builder()
              .withAgentId(agentEntity.id())
              .withMetalakeId(namespacedEntityId.namespaceIds()[0])
              .withCatalogId(namespacedEntityId.namespaceIds()[1])
              .withSchemaId(namespacedEntityId.entityId())
              .withAgentVersion(version)
              .withAgentComment(agentEntity.comment())
              .withAgentProperties(
                  agentEntity.properties() == null || agentEntity.properties().isEmpty()
                      ? null
                      : JsonUtils.anyFieldMapper().writeValueAsString(agentEntity.properties()))
              .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(agentEntity.auditInfo()))
              .withDeletedAt(0L)
              .build();
      return builder
          .withAgentId(agentEntity.id())
          .withAgentName(agentEntity.name())
          .withEndpoint(agentEntity.endpoint())
          .withCapabilities(
              agentEntity.capabilities() == null
                  ? null
                  : JsonUtils.anyFieldMapper().writeValueAsString(agentEntity.capabilities()))
          .withProtocols(
              agentEntity.protocols() == null
                  ? null
                  : JsonUtils.anyFieldMapper().writeValueAsString(agentEntity.protocols()))
          .withOwner(agentEntity.owner())
          .withAgentVersionPO(versionPO)
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(agentEntity.auditInfo()))
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  private AgentPO buildUpdatedAgentPO(AgentPO oldAgentPO, AgentEntity newAgentEntity) {
    Integer newVersion = Integer.parseInt(oldAgentPO.agentLatestVersion()) + 1;
    AgentPO.AgentPOBuilder builder =
        AgentPO.builder()
            .withMetalakeId(oldAgentPO.metalakeId())
            .withCatalogId(oldAgentPO.catalogId())
            .withSchemaId(oldAgentPO.schemaId())
            .withAgentLatestVersion(String.valueOf(newVersion))
            .withAgentCurrentVersion(String.valueOf(newVersion));
    return buildAgentPO(newAgentEntity, builder, newVersion);
  }

  private void insertAgentRow(AgentMetaMapper mapper, AgentPO po, boolean overwrite) {
    if (overwrite) {
      mapper.insertAgentMetaOnDuplicateKeyUpdate(po);
    } else {
      mapper.insertAgentMeta(po);
    }
  }
}
