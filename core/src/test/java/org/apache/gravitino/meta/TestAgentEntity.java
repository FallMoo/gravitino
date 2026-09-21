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
package org.apache.gravitino.meta;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.AgentProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAgentEntity {

  @Test
  public void testBuildShouldFailWhenNamespaceIsMissing() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            AgentEntity.builder()
                .withId(1L)
                .withName("a")
                .withEndpoint("https://example.com/a")
                .withCapabilities(new String[] {"text2sql"})
                .withProtocols(new AgentProtocol[0])
                .withOwner("alice")
                .withComment("test")
                .withProperties(ImmutableMap.of())
                .withAuditInfo(auditInfo)
                .build());
  }

  @Test
  public void testBuildShouldSucceedWhenNamespaceIsPresent() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build();

    AgentEntity entity =
        AgentEntity.builder()
            .withId(1L)
            .withName("a")
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withEndpoint("https://example.com/a")
            .withCapabilities(new String[] {"text2sql", "schema-explain"})
            .withProtocols(new AgentProtocol[0])
            .withOwner("alice")
            .withComment("test agent")
            .withProperties(ImmutableMap.of("routing.priority", "high"))
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals("a", entity.name());
    Assertions.assertEquals("https://example.com/a", entity.endpoint());
    Assertions.assertEquals("alice", entity.owner());
    Assertions.assertArrayEquals(
        new String[] {"text2sql", "schema-explain"}, entity.capabilities());
    Assertions.assertEquals(Namespace.of("m1", "c1", "s1"), entity.namespace());
  }
}
