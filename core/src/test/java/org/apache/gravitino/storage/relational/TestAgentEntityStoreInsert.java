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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AgentEntity;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.TestTemplate;

/**
 * End-to-end insert test that drives Agent through the real {@code RelationalBackend.insert} method
 * (the production path ManagedAgentOperations.registerAgent → EntityStore.put →
 * JDBCBackend.insert).
 *
 * <p>{@code TestAgentMetaService} only calls {@code
 * AgentMetaService.getInstance().insertAgent(...)} directly, bypassing the dispatcher's EntityStore
 * path, so it would never have caught this bug.
 *
 * <p>Without the {@code AgentEntity} branch in {@code JDBCBackend.insert}, this test reproduces the
 * production bug where {@code registerAgent} throws {@code UnsupportedEntityTypeException:
 * Unsupported entity type: ...AgentEntity for insert operation}.
 */
public class TestAgentEntityStoreInsert extends TestJDBCBackend {

  @TestTemplate
  public void testRegisterAgentEndToEnd() throws Exception {
    AuditInfo audit =
        AuditInfo.builder().withCreator("tester").withCreateTime(java.time.Instant.now()).build();

    String metalakeName = "ml_agent_bug_repro";
    String catalogName = "c_agent_bug_repro";
    String schemaName = "s_agent_bug_repro";
    String agentName = "sql_assistant";

    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    AgentEntity agent =
        AgentEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(agentName)
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName))
            .withEndpoint("https://example.com/a")
            .withCapabilities(new String[] {"text2sql"})
            .withProtocols(new org.apache.gravitino.agent.AgentProtocol[0])
            .withOwner("alice")
            .withComment("test")
            .withProperties(ImmutableMap.of())
            .withAuditInfo(audit)
            .build();

    // The actual production path: ManagedAgentOperations.registerAgent -> EntityStore.put ->
    // JDBCBackend.insert. Without the AgentEntity branch in JDBCBackend.insert, this throws
    // UnsupportedEntityTypeException("Unsupported entity type: ...AgentEntity for insert
    // operation").
    assertDoesNotThrow(
        () -> backend.insert(agent, false),
        "RelationalBackend.insert for AgentEntity must reach AgentMetaService.insertAgent. "
            + "If this fails with UnsupportedEntityTypeException, JDBCBackend.insert is missing the "
            + "instanceof AgentEntity branch (see design doc §6 'real lessons learned' note).");
  }
}
