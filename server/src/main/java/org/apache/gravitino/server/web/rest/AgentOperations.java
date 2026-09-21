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
package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.agent.Agent;
import org.apache.gravitino.agent.AgentChange;
import org.apache.gravitino.catalog.AgentDispatcher;
import org.apache.gravitino.dto.requests.AgentRegisterRequest;
import org.apache.gravitino.dto.requests.AgentUpdateRequest;
import org.apache.gravitino.dto.requests.AgentUpdatesRequest;
import org.apache.gravitino.dto.responses.AgentResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST operations for agent management under a schema. Mirrors {@code FunctionOperations} but the
 * list endpoint always returns identifiers (Agent has no {@code listInfos} variant) and the
 * alter/update path goes through the new {@link AgentUpdateRequest} {@code @JsonTypeInfo}
 * discriminated payload.
 */
@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/agents")
public class AgentOperations {

  private static final Logger LOG = LoggerFactory.getLogger(AgentOperations.class);

  private final AgentDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public AgentOperations(AgentDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-agent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-agent", absolute = true)
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response listAgents(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema) {
    LOG.info("Received list agents request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace namespace = NamespaceUtil.ofAgent(metalake, catalog, schema);
            NameIdentifier[] identifiers = dispatcher.listAgents(namespace);
            identifiers = identifiers == null ? new NameIdentifier[0] : identifiers;
            identifiers =
                MetadataAuthzHelper.filterByExpression(
                    metalake,
                    AuthorizationExpressionConstants.FILTER_AGENT_AUTHORIZATION_EXPRESSION,
                    Entity.EntityType.AGENT,
                    identifiers);
            LOG.info(
                "List {} agent names under schema: {}.{}.{}",
                identifiers.length,
                metalake,
                catalog,
                schema);
            return Utils.ok(new EntityListResponse(identifiers));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleAgentException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "register-agent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-agent", absolute = true)
  @AuthorizationExpression(
      expression =
          """
                      ANY(OWNER, METALAKE, CATALOG) ||
                      SCHEMA_OWNER_WITH_USE_CATALOG ||
                      ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_REGISTER_AGENT
                      """,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response registerAgent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      AgentRegisterRequest request) {
    LOG.info(
        "Received register agent request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifier.of(
                    NamespaceUtil.ofAgent(metalake, catalog, schema), request.getName());

            Agent agent =
                dispatcher.registerAgent(
                    ident,
                    request.getEndpoint(),
                    request.getCapabilities(),
                    request.getProtocols(),
                    request.getComment(),
                    request.getProperties(),
                    request.getOwner());

            Response response = Utils.ok(new AgentResponse(DTOConverters.toDTO(agent)));
            LOG.info("Agent registered: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleAgentException(
          OperationType.REGISTER, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{agent}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-agent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-agent", absolute = true)
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_AGENT_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.AGENT)
  public Response getAgent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("agent") @AuthorizationMetadata(type = Entity.EntityType.AGENT) String agent) {
    LOG.info("Received get agent request: {}.{}.{}.{}", metalake, catalog, schema, agent);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifier.of(NamespaceUtil.ofAgent(metalake, catalog, schema), agent);
            Agent a = dispatcher.getAgent(ident);
            Response response = Utils.ok(new AgentResponse(DTOConverters.toDTO(a)));
            LOG.info("Agent loaded: {}.{}.{}.{}", metalake, catalog, schema, agent);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleAgentException(OperationType.LOAD, agent, schema, e);
    }
  }

  @PUT
  @Path("{agent}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-agent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-agent", absolute = true)
  @AuthorizationExpression(
      expression =
          """
                      ANY(OWNER, METALAKE, CATALOG) ||
                      SCHEMA_OWNER_WITH_USE_CATALOG ||
                      ANY_USE_CATALOG && ANY_USE_SCHEMA && (AGENT::OWNER || ANY_MODIFY_AGENT)
                      """,
      accessMetadataType = MetadataObject.Type.AGENT)
  public Response alterAgent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("agent") @AuthorizationMetadata(type = Entity.EntityType.AGENT) String agent,
      AgentUpdatesRequest request) {
    LOG.info("Received alter agent request: {}.{}.{}.{}", metalake, catalog, schema, agent);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifier.of(NamespaceUtil.ofAgent(metalake, catalog, schema), agent);
            AgentChange[] changes =
                request.getUpdates().stream()
                    .map(AgentUpdateRequest::agentChange)
                    .toArray(AgentChange[]::new);
            Agent a = dispatcher.alterAgent(ident, changes);
            Response response = Utils.ok(new AgentResponse(DTOConverters.toDTO(a)));
            LOG.info("Agent altered: {}.{}.{}.{}", metalake, catalog, schema, a.name());
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleAgentException(OperationType.ALTER, agent, schema, e);
    }
  }

  @DELETE
  @Path("{agent}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-agent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-agent", absolute = true)
  @AuthorizationExpression(
      expression =
          """
                      ANY(OWNER, METALAKE, CATALOG) ||
                      SCHEMA_OWNER_WITH_USE_CATALOG ||
                      ANY_USE_CATALOG && ANY_USE_SCHEMA && AGENT::OWNER
                      """,
      accessMetadataType = MetadataObject.Type.AGENT)
  public Response dropAgent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("agent") @AuthorizationMetadata(type = Entity.EntityType.AGENT) String agent) {
    LOG.info("Received drop agent request: {}.{}.{}.{}", metalake, catalog, schema, agent);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifier.of(NamespaceUtil.ofAgent(metalake, catalog, schema), agent);
            boolean dropped = dispatcher.dropAgent(ident);
            if (!dropped) {
              LOG.warn("Cannot find to be dropped agent {} under schema {}", agent, schema);
            } else {
              LOG.info("Agent dropped: {}.{}.{}.{}", metalake, catalog, schema, agent);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleAgentException(OperationType.DROP, agent, schema, e);
    }
  }
}
