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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.dto.agent.AgentProtocolDTO;
import org.apache.gravitino.rest.RESTRequest;

/**
 * Request to update an agent. Mirrors {@code FunctionUpdateRequest}: the wire protocol uses a
 * discriminator field (e.g. {@code "@type": "addProtocol"}) via Jackson's {@link JsonTypeInfo} +
 * {@link JsonSubTypes}. Each inner class corresponds to one entry in {@link AgentChange}'s static
 * factory set.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AgentUpdateRequest.RenameAgentRequest.class, name = "renameAgent"),
  @JsonSubTypes.Type(value = AgentUpdateRequest.SetPropertyRequest.class, name = "setProperty"),
  @JsonSubTypes.Type(
      value = AgentUpdateRequest.RemovePropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(value = AgentUpdateRequest.AddProtocolRequest.class, name = "addProtocol"),
  @JsonSubTypes.Type(
      value = AgentUpdateRequest.RemoveProtocolRequest.class,
      name = "removeProtocol"),
  @JsonSubTypes.Type(value = AgentUpdateRequest.AddCapabilityRequest.class, name = "addCapability"),
  @JsonSubTypes.Type(
      value = AgentUpdateRequest.RemoveCapabilityRequest.class,
      name = "removeCapability"),
  @JsonSubTypes.Type(value = AgentUpdateRequest.UpdateCommentRequest.class, name = "updateComment"),
  @JsonSubTypes.Type(value = AgentUpdateRequest.SetOwnerRequest.class, name = "setOwner")
})
public interface AgentUpdateRequest extends RESTRequest {

  /** Convert this DTO into the corresponding {@link AgentChange}. */
  AgentChange agentChange();

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RenameAgentRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("newName")
    private final String newName;

    @Override
    public AgentChange agentChange() {
      return AgentChange.renameAgent(newName);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SetPropertyRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    @Override
    public AgentChange agentChange() {
      return AgentChange.setProperty(property, value);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemovePropertyRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("property")
    private final String property;

    @Override
    public AgentChange agentChange() {
      return AgentChange.removeProperty(property);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class AddProtocolRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("protocol")
    private final AgentProtocolDTO protocol;

    @Override
    public AgentChange agentChange() {
      return AgentChange.addProtocol(protocol);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(protocol != null, "\"protocol\" field is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(protocol.type()), "\"protocol.type\" field is required");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveProtocolRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("type")
    private final String type;

    @Override
    public AgentChange agentChange() {
      return AgentChange.removeProtocol(type);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(type), "\"type\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class AddCapabilityRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("capability")
    private final String capability;

    @Override
    public AgentChange agentChange() {
      return AgentChange.addCapability(capability);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(capability),
          "\"capability\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveCapabilityRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("capability")
    private final String capability;

    @Override
    public AgentChange agentChange() {
      return AgentChange.removeCapability(capability);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(capability),
          "\"capability\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateCommentRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    @Override
    public AgentChange agentChange() {
      return AgentChange.updateComment(newComment);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }
  }

  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class SetOwnerRequest implements AgentUpdateRequest {
    @Getter
    @JsonProperty("newOwner")
    private final String newOwner;

    @Override
    public AgentChange agentChange() {
      return AgentChange.setOwner(newOwner);
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newOwner), "\"newOwner\" field is required and cannot be empty");
    }
  }
}
