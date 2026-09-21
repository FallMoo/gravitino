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
package org.apache.gravitino.agent;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * Represents a change that can be applied to an {@link Agent}. The shape mirrors {@link
 * org.apache.gravitino.function.FunctionChange}: each kind of change is an immutable nested {@code
 * final class} whose constructor validates its arguments. Use the static factory methods to
 * construct changes; the public {@link AgentChange} interface is intentionally empty so that all
 * change values are sealed in this file.
 */
@Evolving
public interface AgentChange {

  /**
   * Create a change that renames the agent within its schema.
   *
   * @param newName The new name; must be non-blank.
   * @return The change instance.
   */
  static AgentChange renameAgent(String newName) {
    return new RenameAgent(newName);
  }

  /**
   * Create a change that sets or replaces a user-defined property.
   *
   * @param property The property key; must be non-blank.
   * @param value The new property value; must be non-null.
   * @return The change instance.
   */
  static AgentChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a change that removes a user-defined property.
   *
   * @param property The property key; must be non-blank.
   * @return The change instance.
   */
  static AgentChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a change that appends a protocol descriptor to the agent.
   *
   * @param protocol The protocol to add; must be non-null.
   * @return The change instance.
   */
  static AgentChange addProtocol(AgentProtocol protocol) {
    return new AddProtocol(protocol);
  }

  /**
   * Create a change that removes the protocol descriptor with the given type.
   *
   * @param type The protocol type to remove; must be non-blank.
   * @return The change instance.
   */
  static AgentChange removeProtocol(String type) {
    return new RemoveProtocol(type);
  }

  /**
   * Create a change that appends a capability tag to the agent.
   *
   * @param capability The capability to add; must be non-blank.
   * @return The change instance.
   */
  static AgentChange addCapability(String capability) {
    return new AddCapability(capability);
  }

  /**
   * Create a change that removes a capability tag (first match) from the agent.
   *
   * @param capability The capability to remove; must be non-blank.
   * @return The change instance.
   */
  static AgentChange removeCapability(String capability) {
    return new RemoveCapability(capability);
  }

  /**
   * Create a change that updates the agent comment.
   *
   * @param newComment The new comment; must be non-blank.
   * @return The change instance.
   */
  static AgentChange updateComment(String newComment) {
    return new UpdateComment(newComment);
  }

  /**
   * Create a change that replaces the logical owner of the agent.
   *
   * @param newOwner The new owner; must be non-blank.
   * @return The change instance.
   */
  static AgentChange setOwner(String newOwner) {
    return new SetOwner(newOwner);
  }

  /** A {@link AgentChange} to rename an agent. */
  final class RenameAgent implements AgentChange {
    private final String newName;

    RenameAgent(String newName) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), '"' + "newName" + '"' + " cannot be null or empty");
      this.newName = newName;
    }

    /**
     * @return The new agent name.
     */
    public String newName() {
      return newName;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RenameAgent)) {
        return false;
      }
      return Objects.equals(newName, ((RenameAgent) obj).newName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    @Override
    public String toString() {
      return "RenameAgent{newName='" + newName + "'}";
    }
  }

  /** A {@link AgentChange} to set or replace a property. */
  final class SetProperty implements AgentChange {
    private final String property;
    private final String value;

    SetProperty(String property, String value) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), '"' + "property" + '"' + " cannot be null or empty");
      Preconditions.checkArgument(value != null, '"' + "value" + '"' + " cannot be null");
      this.property = property;
      this.value = value;
    }

    /**
     * @return The property name being set or removed.
     */
    public String property() {
      return property;
    }

    /**
     * @return The property value being set.
     */
    public String value() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof SetProperty)) {
        return false;
      }
      SetProperty that = (SetProperty) obj;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    @Override
    public String toString() {
      return "SetProperty{property='" + property + "', value='" + value + "'}";
    }
  }

  /** A {@link AgentChange} to remove a property. */
  final class RemoveProperty implements AgentChange {
    private final String property;

    RemoveProperty(String property) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), '"' + "property" + '"' + " cannot be null or empty");
      this.property = property;
    }

    /**
     * @return The property name being set or removed.
     */
    public String property() {
      return property;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveProperty)) {
        return false;
      }
      return Objects.equals(property, ((RemoveProperty) obj).property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    @Override
    public String toString() {
      return "RemoveProperty{property='" + property + "'}";
    }
  }

  /** A {@link AgentChange} to append a protocol descriptor. */
  final class AddProtocol implements AgentChange {
    private final AgentProtocol protocol;

    AddProtocol(AgentProtocol protocol) {
      Preconditions.checkArgument(protocol != null, '"' + "protocol" + '"' + " cannot be null");
      this.protocol = protocol;
    }

    /**
     * @return The protocol descriptor being added.
     */
    public AgentProtocol protocol() {
      return protocol;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof AddProtocol)) {
        return false;
      }
      return Objects.equals(protocol, ((AddProtocol) obj).protocol);
    }

    @Override
    public int hashCode() {
      return Objects.hash(protocol);
    }

    @Override
    public String toString() {
      return "AddProtocol{protocol=" + protocol + '}';
    }
  }

  /** A {@link AgentChange} to remove a protocol descriptor by {@code type}. */
  final class RemoveProtocol implements AgentChange {
    private final String type;

    RemoveProtocol(String type) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(type), '"' + "type" + '"' + " cannot be null or empty");
      this.type = type;
    }

    /**
     * @return The protocol type identifying the protocol to remove.
     */
    public String type() {
      return type;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveProtocol)) {
        return false;
      }
      return Objects.equals(type, ((RemoveProtocol) obj).type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type);
    }

    @Override
    public String toString() {
      return "RemoveProtocol{type='" + type + "'}";
    }
  }

  /** A {@link AgentChange} to append a capability tag. */
  final class AddCapability implements AgentChange {
    private final String capability;

    AddCapability(String capability) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(capability),
          '"' + "capability" + '"' + " cannot be null or empty");
      this.capability = capability;
    }

    /**
     * @return The capability tag being added or removed.
     */
    public String capability() {
      return capability;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof AddCapability)) {
        return false;
      }
      return Objects.equals(capability, ((AddCapability) obj).capability);
    }

    @Override
    public int hashCode() {
      return Objects.hash(capability);
    }

    @Override
    public String toString() {
      return "AddCapability{capability='" + capability + "'}";
    }
  }

  /** A {@link AgentChange} to remove a capability tag (first match). */
  final class RemoveCapability implements AgentChange {
    private final String capability;

    RemoveCapability(String capability) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(capability),
          '"' + "capability" + '"' + " cannot be null or empty");
      this.capability = capability;
    }

    /**
     * @return The capability tag being added or removed.
     */
    public String capability() {
      return capability;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveCapability)) {
        return false;
      }
      return Objects.equals(capability, ((RemoveCapability) obj).capability);
    }

    @Override
    public int hashCode() {
      return Objects.hash(capability);
    }

    @Override
    public String toString() {
      return "RemoveCapability{capability='" + capability + "'}";
    }
  }

  /** A {@link AgentChange} to update the comment of an agent. */
  final class UpdateComment implements AgentChange {
    private final String newComment;

    UpdateComment(String newComment) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          '"' + "newComment" + '"' + " cannot be null or empty");
      this.newComment = newComment;
    }

    /**
     * @return The new agent comment.
     */
    public String newComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof UpdateComment)) {
        return false;
      }
      return Objects.equals(newComment, ((UpdateComment) obj).newComment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    @Override
    public String toString() {
      return "UpdateComment{newComment='" + newComment + "'}";
    }
  }

  /** A {@link AgentChange} to replace the logical owner of an agent. */
  final class SetOwner implements AgentChange {
    private final String newOwner;

    SetOwner(String newOwner) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newOwner), '"' + "newOwner" + '"' + " cannot be null or empty");
      this.newOwner = newOwner;
    }

    /**
     * @return The new agent owner.
     */
    public String newOwner() {
      return newOwner;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof SetOwner)) {
        return false;
      }
      return Objects.equals(newOwner, ((SetOwner) obj).newOwner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newOwner);
    }

    @Override
    public String toString() {
      return "SetOwner{newOwner='" + newOwner + "'}";
    }
  }
}
