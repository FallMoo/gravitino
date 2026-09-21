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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentProtocol;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/**
 * A class representing an agent entity in the metadata store. The entity mirrors {@link
 * FunctionEntity} in shape: it implements {@link Entity}, {@link Auditable}, {@link HasIdentifier}
 * and the asset interface ({@link Agent}), and uses a Lombok-style builder for construction.
 *
 * <p>{@link AgentProtocol} is stored on the entity as an array reference; the unique-type
 * constraint is enforced by {@code AgentMetaService}, not by the entity.
 */
@ToString
public class AgentEntity implements Entity, Auditable, HasIdentifier, Agent {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the agent entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the agent entity.");
  public static final Field NAMESPACE =
      Field.required("namespace", Namespace.class, "The namespace of the agent entity.");
  public static final Field ENDPOINT =
      Field.required("endpoint", String.class, "The endpoint URL of the agent.");
  public static final Field CAPABILITIES =
      Field.required("capabilities", String[].class, "The capability tags of the agent.");
  public static final Field PROTOCOLS =
      Field.required("protocols", AgentProtocol[].class, "The protocol descriptors of the agent.");
  public static final Field OWNER =
      Field.required("owner", String.class, "The logical owner of the agent.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the agent entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The user-defined properties of the agent entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the agent entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private String endpoint;
  private String[] capabilities;
  private AgentProtocol[] protocols;
  private String owner;
  private String comment;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private AgentEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(NAMESPACE, namespace);
    fields.put(ENDPOINT, endpoint);
    fields.put(CAPABILITIES, capabilities);
    fields.put(PROTOCOLS, protocols);
    fields.put(OWNER, owner);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public String endpoint() {
    return endpoint;
  }

  @Override
  public String[] capabilities() {
    return capabilities;
  }

  @Override
  public AgentProtocol[] protocols() {
    return protocols;
  }

  @Override
  public String owner() {
    return owner;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.AGENT;
  }

  @Override
  public SupportsTags supportsTags() {
    throw new UnsupportedOperationException(
        "Agent.supportsTags() is not bound at the entity level; use "
            + "AgentCatalog.asAgentCatalog().getAgent(...).supportsTags()");
  }

  @Override
  public SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException(
        "Agent.supportsPolicies() is not bound at the entity level; use "
            + "AgentCatalog.asAgentCatalog().getAgent(...).supportsPolicies()");
  }

  @Override
  public SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException(
        "Agent.supportsRoles() is not bound at the entity level; use "
            + "AgentCatalog.asAgentCatalog().getAgent(...).supportsRoles()");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AgentEntity)) {
      return false;
    }
    AgentEntity that = (AgentEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(endpoint, that.endpoint)
        && Objects.equals(owner, that.owner)
        && Objects.equals(comment, that.comment)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, namespace, endpoint, owner, comment, auditInfo);
  }

  /** Creates a new builder for constructing an AgentEntity. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for creating instances of {@link AgentEntity}. */
  public static class Builder {
    private final AgentEntity agentEntity;

    private Builder() {
      agentEntity = new AgentEntity();
    }

    public Builder withId(Long id) {
      agentEntity.id = id;
      return this;
    }

    public Builder withName(String name) {
      agentEntity.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      agentEntity.namespace = namespace;
      return this;
    }

    public Builder withEndpoint(String endpoint) {
      agentEntity.endpoint = endpoint;
      return this;
    }

    public Builder withCapabilities(String[] capabilities) {
      agentEntity.capabilities = capabilities;
      return this;
    }

    public Builder withProtocols(AgentProtocol[] protocols) {
      agentEntity.protocols = protocols;
      return this;
    }

    public Builder withOwner(String owner) {
      agentEntity.owner = owner;
      return this;
    }

    public Builder withComment(String comment) {
      agentEntity.comment = comment;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      agentEntity.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      agentEntity.auditInfo = auditInfo;
      return this;
    }

    /** Builds the AgentEntity instance. */
    public AgentEntity build() {
      agentEntity.validate();
      return agentEntity;
    }
  }
}
