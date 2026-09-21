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
package org.apache.gravitino.hook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.AgentDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestAgentHookDispatcher {

  @Test
  public void testRegisterAgentSetOwnerAfterRegister() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);
    Object originalCatalogManager = FieldUtils.readField(gravitinoEnv, "catalogManager", true);

    NameIdentifier agentIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "agent1");
    AgentDispatcher dispatcher = Mockito.mock(AgentDispatcher.class);
    Agent registeredAgent = Mockito.mock(Agent.class);
    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);

    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(catalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);
    Mockito.when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);

    Mockito.when(
            dispatcher.registerAgent(
                Mockito.eq(agentIdentifier),
                Mockito.eq("https://example.com/a"),
                Mockito.eq(new String[] {"text2sql"}),
                Mockito.eq(null),
                Mockito.eq("comment"),
                Mockito.eq(null),
                Mockito.eq("alice")))
        .thenReturn(registeredAgent);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(gravitinoEnv, "catalogManager", catalogManager, true);
    try {
      AgentHookDispatcher hookDispatcher = new AgentHookDispatcher(dispatcher);
      Agent result =
          hookDispatcher.registerAgent(
              agentIdentifier,
              "https://example.com/a",
              new String[] {"text2sql"},
              null,
              "comment",
              null,
              "alice");

      assertSame(registeredAgent, result);

      ArgumentCaptor<MetadataObject> metadataObjectCaptor =
          ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(ownerDispatcher)
          .setOwner(
              Mockito.eq("metalake1"),
              metadataObjectCaptor.capture(),
              Mockito.eq(PrincipalUtils.getCurrentUserName()),
              Mockito.eq(Owner.Type.USER));
      // The metadata object should be the un-normalized identifier the caller passed in.
      MetadataObject captured = metadataObjectCaptor.getValue();
      assertEquals(MetadataObject.Type.AGENT, captured.type());
      assertEquals("catalog1.schema1.agent1", captured.fullName());
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "catalogManager", originalCatalogManager, true);
    }
  }
}
