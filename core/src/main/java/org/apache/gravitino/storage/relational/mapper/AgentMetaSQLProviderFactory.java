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
package org.apache.gravitino.storage.relational.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.AgentMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.AgentMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.AgentPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class AgentMetaSQLProviderFactory {

  static class AgentMetaMySQLProvider extends AgentMetaBaseSQLProvider {}

  static class AgentMetaH2Provider extends AgentMetaBaseSQLProvider {}

  private static final Map<JDBCBackendType, AgentMetaBaseSQLProvider> AGENT_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new AgentMetaMySQLProvider(),
          JDBCBackendType.H2, new AgentMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new AgentMetaPostgreSQLProvider());

  public static AgentMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return AGENT_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertAgentMeta(@Param("agentMeta") AgentPO agentPO) {
    return getProvider().insertAgentMeta(agentPO);
  }

  public static String insertAgentMetaOnDuplicateKeyUpdate(@Param("agentMeta") AgentPO agentPO) {
    return getProvider().insertAgentMetaOnDuplicateKeyUpdate(agentPO);
  }

  public static String listAgentPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listAgentPOsBySchemaId(schemaId);
  }

  public static String listAgentPOsByAgentIds(@Param("agentIds") List<Long> agentIds) {
    return getProvider().listAgentPOsByAgentIds(agentIds);
  }

  public static String selectAgentMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("agentName") String agentName) {
    return getProvider().selectAgentMetaBySchemaIdAndName(schemaId, agentName);
  }

  public static String selectAgentIdBySchemaIdAndAgentName(
      @Param("schemaId") Long schemaId, @Param("agentName") String agentName) {
    return getProvider().selectAgentIdBySchemaIdAndAgentName(schemaId, agentName);
  }

  public static String softDeleteAgentMetaByAgentId(@Param("agentId") Long agentId) {
    return getProvider().softDeleteAgentMetaByAgentId(agentId);
  }

  public static String softDeleteAgentMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteAgentMetasByCatalogId(catalogId);
  }

  public static String softDeleteAgentMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteAgentMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteAgentMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return getProvider().softDeleteAgentMetasBySchemaIds(schemaIds);
  }

  public static String deleteAgentMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteAgentMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String updateAgentMeta(
      @Param("newAgentMeta") AgentPO newAgentPO, @Param("oldAgentMeta") AgentPO oldAgentPO) {
    return getProvider().updateAgentMeta(newAgentPO, oldAgentPO);
  }
}
