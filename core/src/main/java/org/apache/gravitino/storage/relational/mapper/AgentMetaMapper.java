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

import java.util.List;
import org.apache.gravitino.storage.relational.po.AgentPO;
import org.apache.gravitino.storage.relational.po.AgentVersionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis Mapper for agent metadata operation SQLs. */
public interface AgentMetaMapper {
  String TABLE_NAME = "agent_meta";
  String VERSION_TABLE_NAME = "agent_version_info";

  @Results(
      id = "mapToAgentVersionPO",
      value = {
        @Result(property = "id", column = "id", id = true),
        @Result(property = "metalakeId", column = "version_metalake_id"),
        @Result(property = "catalogId", column = "version_catalog_id"),
        @Result(property = "schemaId", column = "version_schema_id"),
        @Result(property = "agentId", column = "version_agent_id"),
        @Result(property = "agentVersion", column = "version"),
        @Result(property = "agentComment", column = "agent_comment"),
        @Result(property = "agentProperties", column = "agent_properties"),
        @Result(property = "auditInfo", column = "version_audit_info"),
        @Result(property = "deletedAt", column = "version_deleted_at")
      })
  @Select("SELECT 1") // Dummy SQL to avoid MyBatis error, never be executed
  AgentVersionPO mapToAgentVersionPO();

  @InsertProvider(type = AgentMetaSQLProviderFactory.class, method = "insertAgentMeta")
  void insertAgentMeta(@Param("agentMeta") AgentPO agentPO);

  @InsertProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "insertAgentMetaOnDuplicateKeyUpdate")
  void insertAgentMetaOnDuplicateKeyUpdate(@Param("agentMeta") AgentPO agentPO);

  @Results({
    @Result(property = "agentId", column = "agent_id", id = true),
    @Result(property = "agentName", column = "agent_name"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "catalogId", column = "catalog_id"),
    @Result(property = "schemaId", column = "schema_id"),
    @Result(property = "endpoint", column = "endpoint"),
    @Result(property = "capabilities", column = "capabilities"),
    @Result(property = "protocols", column = "protocols"),
    @Result(property = "owner", column = "owner"),
    @Result(property = "agentCurrentVersion", column = "agent_current_version"),
    @Result(property = "agentLatestVersion", column = "agent_latest_version"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(
        property = "agentVersionPO",
        javaType = AgentVersionPO.class,
        column =
            "{id,version_metalake_id,version_catalog_id,version_schema_id,version_agent_id,"
                + "version,agent_comment,agent_properties,version_audit_info,version_deleted_at}",
        one = @One(resultMap = "mapToAgentVersionPO"))
  })
  @SelectProvider(type = AgentMetaSQLProviderFactory.class, method = "listAgentPOsBySchemaId")
  List<AgentPO> listAgentPOsBySchemaId(@Param("schemaId") Long schemaId);

  @Results({
    @Result(property = "agentId", column = "agent_id", id = true),
    @Result(property = "agentName", column = "agent_name"),
    @Result(property = "schemaId", column = "schema_id"),
  })
  @SelectProvider(type = AgentMetaSQLProviderFactory.class, method = "listAgentPOsByAgentIds")
  List<AgentPO> listAgentPOsByAgentIds(@Param("agentIds") List<Long> agentIds);

  @ResultMap("agentPOResultMap")
  @SelectProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "selectAgentMetaBySchemaIdAndName")
  AgentPO selectAgentMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("agentName") String agentName);

  @SelectProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "selectAgentIdBySchemaIdAndAgentName")
  Long selectAgentIdBySchemaIdAndAgentName(
      @Param("schemaId") Long schemaId, @Param("agentName") String agentName);

  @UpdateProvider(type = AgentMetaSQLProviderFactory.class, method = "softDeleteAgentMetaByAgentId")
  Integer softDeleteAgentMetaByAgentId(@Param("agentId") Long agentId);

  @UpdateProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "softDeleteAgentMetasByCatalogId")
  Integer softDeleteAgentMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "softDeleteAgentMetasByMetalakeId")
  Integer softDeleteAgentMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "softDeleteAgentMetasBySchemaIds")
  Integer softDeleteAgentMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  @DeleteProvider(
      type = AgentMetaSQLProviderFactory.class,
      method = "deleteAgentMetasByLegacyTimeline")
  Integer deleteAgentMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(type = AgentMetaSQLProviderFactory.class, method = "updateAgentMeta")
  Integer updateAgentMeta(
      @Param("newAgentMeta") AgentPO newAgentPO, @Param("oldAgentMeta") AgentPO oldAgentPO);

  // ResultMap alias so the @ResultMap annotation can reference it by name.
  @Results(
      id = "agentPOResultMap",
      value = {
        @Result(property = "agentId", column = "agent_id", id = true),
        @Result(property = "agentName", column = "agent_name"),
        @Result(property = "metalakeId", column = "metalake_id"),
        @Result(property = "catalogId", column = "catalog_id"),
        @Result(property = "schemaId", column = "schema_id"),
        @Result(property = "endpoint", column = "endpoint"),
        @Result(property = "capabilities", column = "capabilities"),
        @Result(property = "protocols", column = "protocols"),
        @Result(property = "owner", column = "owner"),
        @Result(property = "agentCurrentVersion", column = "agent_current_version"),
        @Result(property = "agentLatestVersion", column = "agent_latest_version"),
        @Result(property = "auditInfo", column = "audit_info"),
        @Result(property = "deletedAt", column = "deleted_at"),
        @Result(
            property = "agentVersionPO",
            javaType = AgentVersionPO.class,
            column =
                "{id,version_metalake_id,version_catalog_id,version_schema_id,version_agent_id,"
                    + "version,agent_comment,agent_properties,version_audit_info,version_deleted_at}",
            one = @One(resultMap = "mapToAgentVersionPO"))
      })
  @Select("SELECT 1")
  AgentPO agentPOResultMap();
}
