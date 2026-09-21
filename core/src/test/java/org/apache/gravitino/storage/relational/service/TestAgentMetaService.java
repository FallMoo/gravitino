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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.catalog.ManagedAgentOperations;
import org.apache.gravitino.dto.agent.AgentProtocolDTO;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AgentEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestAgentMetaService extends TestJDBCBackend {
  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_agent_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_agent_schema");

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "sql_assistant", AUDIT_INFO);
    AgentEntity agentCopy =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "sql_assistant", AUDIT_INFO);

    AgentMetaService.getInstance().insertAgent(agent, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> AgentMetaService.getInstance().insertAgent(agentCopy, false));
  }

  @TestTemplate
  public void testListAgentsByNamespace() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentMetaService.getInstance()
        .insertAgent(
            createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "agent_a", AUDIT_INFO),
            false);
    AgentMetaService.getInstance()
        .insertAgent(
            createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "agent_b", AUDIT_INFO),
            false);
    AgentMetaService.getInstance()
        .insertAgent(
            createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "agent_c", AUDIT_INFO),
            false);

    List<AgentEntity> agents = AgentMetaService.getInstance().listAgentsByNamespace(ns);
    assertEquals(3, agents.size());
    assertEquals(
        ImmutableList.of("agent_a", "agent_b", "agent_c"),
        agents.stream().map(AgentEntity::name).sorted().collect(ImmutableList.toImmutableList()));
  }

  @TestTemplate
  public void testGetAndDeleteAgent() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "to_delete", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentEntity fetched =
        AgentMetaService.getInstance().getAgentByIdentifier(NameIdentifier.of(ns, "to_delete"));
    assertEquals(agent.id(), fetched.id());
    assertEquals(agent.endpoint(), fetched.endpoint());
    assertEquals(agent.owner(), fetched.owner());

    boolean deleted =
        AgentMetaService.getInstance().deleteAgent(NameIdentifier.of(ns, "to_delete"));
    assertTrue(deleted);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            AgentMetaService.getInstance()
                .getAgentByIdentifier(NameIdentifier.of(ns, "to_delete")));
  }

  @TestTemplate
  public void testDeleteAgentReturnsFalseWhenMissing() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    boolean deleted =
        AgentMetaService.getInstance().deleteAgent(NameIdentifier.of(ns, "never_existed"));
    assertFalse(deleted);
  }

  @TestTemplate
  public void testAlterAgentSetCommentAndProperty() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "alter_me", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentEntity updated =
        AgentMetaService.getInstance()
            .updateAgent(
                NameIdentifier.of(ns, "alter_me"),
                e ->
                    ManagedAgentOperations.applyChanges(
                        (AgentEntity) e,
                        AgentChange.updateComment("updated comment"),
                        AgentChange.setProperty("k1", "v1")));

    assertEquals("updated comment", updated.comment());
    assertEquals("v1", updated.properties().get("k1"));
    // endpoint / owner / capabilities / protocols unchanged
    assertEquals(agent.endpoint(), updated.endpoint());
    assertEquals(agent.owner(), updated.owner());
    assertNotNull(updated.auditInfo().lastModifiedTime());
  }

  @TestTemplate
  public void testAlterAgentAddAndRemoveCapability() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "cap_test", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentEntity updated =
        AgentMetaService.getInstance()
            .updateAgent(
                NameIdentifier.of(ns, "cap_test"),
                e ->
                    ManagedAgentOperations.applyChanges(
                        (AgentEntity) e,
                        AgentChange.addCapability("reranking"),
                        AgentChange.removeCapability("text2sql")));

    // The helper created with [text2sql, schema-explain]; after remove text2sql -> [schema-explain,
    // reranking]
    assertEquals(2, updated.capabilities().length);
    assertTrue(ImmutableList.copyOf(updated.capabilities()).contains("schema-explain"));
    assertTrue(ImmutableList.copyOf(updated.capabilities()).contains("reranking"));
  }

  @TestTemplate
  public void testAlterAgentAddProtocolRejectsDuplicateType() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "proto_dup", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentProtocol mcp =
        AgentProtocolDTO.builder()
            .withType("MCP")
            .withAgentCardUrl("https://example.com/mcp.json")
            .build();
    // First addProtocol MCP succeeds
    AgentMetaService.getInstance()
        .updateAgent(
            NameIdentifier.of(ns, "proto_dup"),
            e ->
                ManagedAgentOperations.applyChanges((AgentEntity) e, AgentChange.addProtocol(mcp)));

    // Second addProtocol with same type throws (Q1 in design doc)
    AgentProtocol mcpAgain =
        AgentProtocolDTO.builder()
            .withType("MCP")
            .withAgentCardUrl("https://example.com/mcp2.json")
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AgentMetaService.getInstance()
                .updateAgent(
                    NameIdentifier.of(ns, "proto_dup"),
                    e ->
                        ManagedAgentOperations.applyChanges(
                            (AgentEntity) e, AgentChange.addProtocol(mcpAgain))));
  }

  @TestTemplate
  public void testAlterAgentRemoveProtocol() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "proto_rm", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentEntity updated =
        AgentMetaService.getInstance()
            .updateAgent(
                NameIdentifier.of(ns, "proto_rm"),
                e ->
                    ManagedAgentOperations.applyChanges(
                        (AgentEntity) e, AgentChange.removeProtocol("A2A")));

    assertEquals(0, updated.protocols().length);
  }

  @TestTemplate
  public void testAlterAgentRenameAndSetOwner() throws IOException {
    Namespace ns = NamespaceUtil.ofAgent(metalakeName, catalogName, schemaName);
    AgentEntity agent =
        createAgentEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "renameme", AUDIT_INFO);
    AgentMetaService.getInstance().insertAgent(agent, false);

    AgentEntity updated =
        AgentMetaService.getInstance()
            .updateAgent(
                NameIdentifier.of(ns, "renameme"),
                e ->
                    ManagedAgentOperations.applyChanges(
                        (AgentEntity) e,
                        AgentChange.renameAgent("renamed"),
                        AgentChange.setOwner("bob")));

    assertEquals("renamed", updated.name());
    assertEquals("bob", updated.owner());
    // The old name should no longer exist
    assertThrows(
        NoSuchEntityException.class,
        () ->
            AgentMetaService.getInstance().getAgentByIdentifier(NameIdentifier.of(ns, "renameme")));
  }
}
