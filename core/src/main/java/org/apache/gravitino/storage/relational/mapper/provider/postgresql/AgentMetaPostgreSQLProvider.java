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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.AgentMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.AgentMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.AgentPO;
import org.apache.ibatis.annotations.Param;

public class AgentMetaPostgreSQLProvider extends AgentMetaBaseSQLProvider {

  @Override
  public String insertAgentMeta(@Param("agentMeta") AgentPO agentPO) {
    return "INSERT INTO "
        + AgentMetaMapper.TABLE_NAME
        + " (agent_id, agent_name, metalake_id, catalog_id, schema_id,"
        + " endpoint, capabilities, protocols, owner,"
        + " agent_current_version, agent_latest_version, audit_info, deleted_at)"
        + " VALUES (#{agentMeta.agentId}, #{agentMeta.agentName},"
        + " #{agentMeta.metalakeId}, #{agentMeta.catalogId}, #{agentMeta.schemaId},"
        + " #{agentMeta.endpoint}, #{agentMeta.capabilities}, #{agentMeta.protocols},"
        + " #{agentMeta.owner},"
        + " #{agentMeta.agentCurrentVersion}, #{agentMeta.agentLatestVersion},"
        + " #{agentMeta.auditInfo}, #{agentMeta.deletedAt})";
  }

  @Override
  public String insertAgentMetaOnDuplicateKeyUpdate(@Param("agentMeta") AgentPO agentPO) {
    return "INSERT INTO "
        + AgentMetaMapper.TABLE_NAME
        + " (agent_id, agent_name, metalake_id, catalog_id, schema_id,"
        + " endpoint, capabilities, protocols, owner,"
        + " agent_current_version, agent_latest_version, audit_info, deleted_at)"
        + " VALUES (#{agentMeta.agentId}, #{agentMeta.agentName},"
        + " #{agentMeta.metalakeId}, #{agentMeta.catalogId}, #{agentMeta.schemaId},"
        + " #{agentMeta.endpoint}, #{agentMeta.capabilities}, #{agentMeta.protocols},"
        + " #{agentMeta.owner},"
        + " #{agentMeta.agentCurrentVersion}, #{agentMeta.agentLatestVersion},"
        + " #{agentMeta.auditInfo}, #{agentMeta.deletedAt})"
        + " ON CONFLICT (agent_id) DO UPDATE SET"
        + " agent_name = #{agentMeta.agentName},"
        + " metalake_id = #{agentMeta.metalakeId},"
        + " catalog_id = #{agentMeta.catalogId},"
        + " schema_id = #{agentMeta.schemaId},"
        + " endpoint = #{agentMeta.endpoint},"
        + " capabilities = #{agentMeta.capabilities},"
        + " protocols = #{agentMeta.protocols},"
        + " owner = #{agentMeta.owner},"
        + " agent_current_version = #{agentMeta.agentCurrentVersion},"
        + " agent_latest_version = #{agentMeta.agentLatestVersion},"
        + " audit_info = #{agentMeta.auditInfo},"
        + " deleted_at = #{agentMeta.deletedAt}";
  }

  @Override
  public String listAgentPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT am.agent_id, am.agent_name, am.metalake_id, am.catalog_id, am.schema_id,"
        + " am.endpoint, am.capabilities, am.protocols, am.owner,"
        + " am.agent_current_version, am.agent_latest_version,"
        + " am.audit_info, am.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.agent_id as version_agent_id,"
        + " vi.version, vi.agent_comment, vi.agent_properties,"
        + " vi.audit_info as version_audit_info, vi.deleted_at as version_deleted_at"
        + " FROM "
        + AgentMetaMapper.TABLE_NAME
        + " am INNER JOIN "
        + AgentMetaMapper.VERSION_TABLE_NAME
        + " vi ON am.agent_id = vi.agent_id AND am.agent_current_version = vi.version"
        + " WHERE am.schema_id = #{schemaId} AND am.deleted_at = 0 AND vi.deleted_at = 0";
  }

  @Override
  public String selectAgentMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("agentName") String agentName) {
    return "SELECT am.agent_id, am.agent_name, am.metalake_id, am.catalog_id, am.schema_id,"
        + " am.endpoint, am.capabilities, am.protocols, am.owner,"
        + " am.agent_current_version, am.agent_latest_version,"
        + " am.audit_info, am.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.agent_id as version_agent_id,"
        + " vi.version, vi.agent_comment, vi.agent_properties,"
        + " vi.audit_info as version_audit_info, vi.deleted_at as version_deleted_at"
        + " FROM "
        + AgentMetaMapper.TABLE_NAME
        + " am INNER JOIN "
        + AgentMetaMapper.VERSION_TABLE_NAME
        + " vi ON am.agent_id = vi.agent_id AND am.agent_current_version = vi.version"
        + " WHERE am.schema_id = #{schemaId} AND am.agent_name = #{agentName}"
        + " AND am.deleted_at = 0 AND vi.deleted_at = 0";
  }

  @Override
  public String softDeleteAgentMetaByAgentId(@Param("agentId") Long agentId) {
    return "UPDATE "
        + AgentMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE agent_id = #{agentId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteAgentMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + AgentMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteAgentMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + AgentMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteAgentMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + AgentMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String deleteAgentMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + AgentMetaMapper.TABLE_NAME
        + " WHERE agent_id IN (SELECT agent_id FROM "
        + AgentMetaMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
