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
package org.apache.gravitino.catalog;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentCatalog;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.exceptions.AgentAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchAgentException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AgentEntity;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code ManagedAgentOperations} provides the storage-level operations for managing AI agents in
 * Gravitino's {@link EntityStore}. Mirrors {@code ManagedFunctionOperations} but tailored to the
 * flatter Agent schema: agents have no overload / arity concerns, only ordered protocols and a set
 * of capability tags.
 */
public class ManagedAgentOperations implements AgentCatalog {

  private final EntityStore store;
  private final IdGenerator idGenerator;

  public ManagedAgentOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public NameIdentifier[] listAgents(Namespace namespace) throws NoSuchSchemaException {
    try {
      List<AgentEntity> agents = store.list(namespace, AgentEntity.class, Entity.EntityType.AGENT);
      return agents.stream()
          .map(a -> NameIdentifier.of(namespace, a.name()))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list agents in namespace " + namespace, e);
    }
  }

  @Override
  public Agent getAgent(NameIdentifier ident) throws NoSuchAgentException {
    try {
      return store.get(ident, Entity.EntityType.AGENT, AgentEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchAgentException(e, "Agent %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get agent " + ident, e);
    }
  }

  @Override
  public Agent registerAgent(
      NameIdentifier ident,
      String endpoint,
      String[] capabilities,
      AgentProtocol[] protocols,
      String comment,
      Map<String, String> properties,
      String owner)
      throws NoSuchSchemaException, AgentAlreadyExistsException {

    Preconditions.checkArgument(
        StringUtils.isNotBlank(endpoint), "Agent endpoint is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(owner), "Agent owner is required and cannot be empty");

    if (capabilities != null) {
      for (String capability : capabilities) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(capability), "Agent capabilities entries must not be blank");
      }
    }
    if (protocols != null) {
      for (AgentProtocol protocol : protocols) {
        Preconditions.checkArgument(protocol != null, "Agent protocols entries must not be null");
        Preconditions.checkArgument(
            StringUtils.isNotBlank(protocol.type()),
            "Agent protocol entries must have a non-blank type");
      }
      // Enforce protocol type uniqueness (Q1 in the design doc).
      java.util.HashSet<String> seenTypes = new java.util.HashSet<>();
      for (AgentProtocol protocol : protocols) {
        if (!seenTypes.add(protocol.type())) {
          throw new IllegalArgumentException("Duplicate protocol type: " + protocol.type());
        }
      }
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    Instant now = Instant.now();
    AuditInfo auditInfo = AuditInfo.builder().withCreator(currentUser).withCreateTime(now).build();

    AgentEntity agentEntity =
        AgentEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withEndpoint(endpoint)
            .withCapabilities(capabilities == null ? new String[0] : capabilities)
            .withProtocols(protocols == null ? new AgentProtocol[0] : protocols)
            .withOwner(owner)
            .withComment(comment)
            .withProperties(properties == null ? new HashMap<>() : properties)
            .withAuditInfo(auditInfo)
            .build();

    try {
      store.put(agentEntity, false /* overwrite */);
      return agentEntity;
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
    } catch (EntityAlreadyExistsException e) {
      throw new AgentAlreadyExistsException(e, "Agent %s already exists", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register agent " + ident, e);
    }
  }

  @Override
  public boolean dropAgent(NameIdentifier ident) {
    try {
      return store.delete(ident, Entity.EntityType.AGENT);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop agent " + ident, e);
    }
  }

  @Override
  public Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException {

    try {
      return store.update(
          ident,
          AgentEntity.class,
          Entity.EntityType.AGENT,
          oldEntity -> applyChanges(oldEntity, changes));

    } catch (NoSuchEntityException e) {
      throw new NoSuchAgentException(e, "Agent %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalArgumentException("Failed to alter agent " + ident, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to alter agent " + ident, e);
    }
  }

  /**
   * Apply an ordered list of {@link AgentChange}s to the existing entity and return a new entity.
   * The change set is closed: unknown change types raise {@link IllegalArgumentException}.
   *
   * <p>Public static so tests and other dispatchers can reuse the same change-application logic
   * without instantiating a {@link ManagedAgentOperations}.
   */
  public static AgentEntity applyChanges(AgentEntity oldEntity, AgentChange... changes) {
    String newName = oldEntity.name();
    String newComment = oldEntity.comment();
    String newEndpoint = oldEntity.endpoint();
    String newOwner = oldEntity.owner();
    Map<String, String> newProperties =
        oldEntity.properties() == null ? new HashMap<>() : new HashMap<>(oldEntity.properties());
    List<String> newCapabilities =
        oldEntity.capabilities() == null
            ? new ArrayList<>()
            : new ArrayList<>(Arrays.asList(oldEntity.capabilities()));
    List<AgentProtocol> newProtocols =
        oldEntity.protocols() == null
            ? new ArrayList<>()
            : new ArrayList<>(Arrays.asList(oldEntity.protocols()));

    for (AgentChange change : changes) {
      if (change instanceof AgentChange.RenameAgent) {
        newName = ((AgentChange.RenameAgent) change).newName();

      } else if (change instanceof AgentChange.UpdateComment) {
        newComment = ((AgentChange.UpdateComment) change).newComment();

      } else if (change instanceof AgentChange.SetOwner) {
        newOwner = ((AgentChange.SetOwner) change).newOwner();

      } else if (change instanceof AgentChange.SetProperty) {
        AgentChange.SetProperty set = (AgentChange.SetProperty) change;
        newProperties.put(set.property(), set.value());

      } else if (change instanceof AgentChange.RemoveProperty) {
        newProperties.remove(((AgentChange.RemoveProperty) change).property());

      } else if (change instanceof AgentChange.AddProtocol) {
        AgentProtocol protocol = ((AgentChange.AddProtocol) change).protocol();
        for (AgentProtocol existing : newProtocols) {
          if (existing.type().equals(protocol.type())) {
            throw new IllegalArgumentException("Duplicate protocol type: " + protocol.type());
          }
        }
        newProtocols.add(protocol);

      } else if (change instanceof AgentChange.RemoveProtocol) {
        String typeToRemove = ((AgentChange.RemoveProtocol) change).type();
        boolean removed = newProtocols.removeIf(p -> typeToRemove.equals(p.type()));
        if (!removed) {
          throw new IllegalArgumentException("No protocol found with type: " + typeToRemove);
        }

      } else if (change instanceof AgentChange.AddCapability) {
        newCapabilities.add(((AgentChange.AddCapability) change).capability());

      } else if (change instanceof AgentChange.RemoveCapability) {
        String capabilityToRemove = ((AgentChange.RemoveCapability) change).capability();
        boolean removed = newCapabilities.removeIf(capabilityToRemove::equals);
        if (!removed) {
          throw new IllegalArgumentException("No capability found: " + capabilityToRemove);
        }

      } else {
        throw new IllegalArgumentException(
            "Unknown agent change: " + change.getClass().getSimpleName());
      }
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    Instant now = Instant.now();
    AuditInfo newAuditInfo =
        AuditInfo.builder()
            .withCreator(oldEntity.auditInfo().creator())
            .withCreateTime(oldEntity.auditInfo().createTime())
            .withLastModifier(currentUser)
            .withLastModifiedTime(now)
            .build();

    return AgentEntity.builder()
        .withId(oldEntity.id())
        .withName(newName)
        .withNamespace(oldEntity.namespace())
        .withEndpoint(newEndpoint)
        .withCapabilities(newCapabilities.toArray(new String[0]))
        .withProtocols(newProtocols.toArray(new AgentProtocol[0]))
        .withOwner(newOwner)
        .withComment(newComment)
        .withProperties(newProperties)
        .withAuditInfo(newAuditInfo)
        .build();
  }
}
