<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Gravitino 中 Agent 资产的设计

| Field   | Value                                                            |
| ------- | ---------------------------------------------------------------- |
| Status  | Draft → Under Review → Accepted → Implemented                    |
| Authors | @weixl <weixl@apache.org>                                        |
| Created | 2026-09-18                                                       |
| Module  | `api`, `core`, `server`, `common`, `clients/*`                   |
| Mirrors | `org.apache.gravitino.function.*` (Function asset, schema-scoped) |

---

## 1. 背景

Gravitino 当前管理多种元数据资产：关系型的 **Tables** 和 **Views**、对象存储上的 **Filesets**、消息队列上的 **Topics**、带版本的 **Models**、schema 下的 **Functions**，以及可以挂到任意节点的辅助对象（**Tags**、**Policies**、**Roles**、**Statistics**）。所有这些都挂载在同一棵四级资源树（`metalake → catalog → schema → asset`）上，并继承同一套治理栈（审计、事件、tag/policy/role 绑定、统计）。

Gravitino **目前没有**对 **AI Agent** 的管理能力——这里的 Agent 指的是通过 A2A（Agent-to-Agent）、MCP（Model Context Protocol）等协议对外暴露 LLM 驱动能力面的自治或半自治软件服务。组织在落地 Agent 时面临的痛点：

- **没有中央注册中心。** 团队在 wiki、电子表格或散落的配置文件里记录 Agent，发现方式只有"问负责的团队"。
- **没有治理钩子。** Tag（如 `pii-handling`、`production-approved`）、Policy（如 `rate-limit`、`cost-cap`）、Role 无法挂到 Agent 上，因为 Agent 不是一等公民。
- **没有审计轨迹。** 谁注册了一个 Agent、谁更新过它的协议集合、谁删了它，都没有记录。
- **没有协议感知的元数据。** 能力描述和协议描述符（如 A2A 的 `agent_card_url`）与 Agent 的运行时配置分离，导致下游 dispatcher 无法按协议路由到正确的 endpoint。

结果是：企业内 AI Agent 的落地是碎片化、不可治理的——和 Gravitino 已经治理好的其他数据资产形成强烈反差。

本文档提议在 Gravitino 中增加 **Agent** 作为新的顶层资产类型，沿用和 `Function`、`Model` 一致的架构模式，让 Agent 立即继承整套统一治理栈。

---

## 2. 目标

1. **Schema 级生命周期**：Agent 在 `(metalake, catalog, schema)` 下管理，通过标准 Gravitino REST API 完成完整 CRUD。其完整标识为 `metalake.catalog.schema.agent`。
2. **增量变更语义**：所有更新都通过 `AlterAgent` 携带 `AgentChange[]`，与已有的 `TableChange[]` / `ModelChange[]` / `PolicyChange[]` 模式一致，使每一次变更都可独立审计。
3. **开箱即用的完整治理 mixin**：Agent 接口暴露 `default supportsTags()` / `default supportsPolicies()` / `default supportsRoles()`（与 `Model` 同构），返回的对象实现了对应的 `SupportsTags` / `SupportsPolicies` / `SupportsRoles` mixin；不支持时抛 `UnsupportedOperationException`。`SupportsStatistics` 不在资产接口上落地（它由 `MetadataObjectStatisticsOperations` 在 metadata-object 维度提供，详见 §3 非目标 #12）；v1 不提供 Agent 级的 `grantRole` / `revokeRole` dispatcher（见 §3 非目标 #8）。
4. **协议感知的元数据**：Agent 记录携带一份有序的 `AgentProtocol` 描述符列表（`type` + 可选 `agent_card_url`），让下游 dispatcher 可按协议族选取正确的 endpoint。
5. **自管元数据**：Agent 记录存在 Gravitino 的关系型 `EntityStore` 里，运行不依赖任何外部系统——和没有自带后端存储的 `Function`、`Model` 一致。
6. **OpenAPI 与客户端齐头并进**：REST API 之外提供完整的 OpenAPI 规范、Java 客户端、Python 客户端。
7. **软删除与版本保留**：Agent 沿用 Gravitino 现有的软删除 + 版本保留策略（默认 7 天、最多 10 个版本），误删可恢复。

---

## 3. 非目标

1. **Agent 调用 / 路由**：Gravitino **不**执行也不代理对 Agent `endpoint` 的请求。注册 Agent 只是登记元数据，调用 Agent 是消费者的责任。
2. **Agent Card 的 Schema 校验**：Gravitino 把 `agent_card_url` 作为不透明字符串存储；既不抓取也不解析描述符，更不会做协议 schema 的版本校验。
3. **新 `Catalog.Type.AGENT` 或 `agent` `CatalogProvider`**：Agent 不需要专门的 catalog type，也不需要新的 CatalogProvider。它挂在任意已有 catalog 的 schema 下，与 `Function` 完全一致——`Function` 也没有 `Catalog.Type.FUNCTION`，没有 `function` CatalogProvider。Agent 的全部内容（name、endpoint、capabilities、protocols）都是 Gravitino 内部的元数据，没有外部系统需要 provider 去包装。
4. **运行时遥测 / 指标**：Gravitino **不**记录调用次数、延迟、token 用量或任何运行时遥测。只捕获"谁对 Agent 记录做了什么"这条审计 / 事件轨迹。
5. **跨 Metalake 联邦**：一个 Agent 严格属于唯一的 `(metalake, catalog, schema)` 三元组。不做联邦、不跨 metalake 引用。
6. **策略下发到 Endpoint**：把 Policy 挂到 Agent 上，含义是"这个 user/role 能对这个 Agent 的元数据做这个动作"。**不**意味着策略被推送到 Agent 运行时。
7. **发现搜索 / 模糊匹配**：`GET .../agents?name=foo` 做 **精确**名字匹配。模糊搜索和全文索引不在 v1 范围。
8. **Role 在 Agent 上的独立绑定**：所谓"Role 绑定"指在某个具体的资源上把一个 Role（例如 `data_engineer`）关联给某个用户 / 用户组，使该 Role 的权限（例如 `USE_TABLE`、`MODIFY_TABLE`）对该资源生效。Gravitino 现有的 `Role` 机制（`MetadataObject.Type.ROLE` + `SupportsRoles` mixin）允许 Resource 维度（如 catalog、schema、table）上挂 Role；但在本设计中，`SupportsRoles` 仅作为 mixin 占位，v1 **不**暴露 `grantRole` / `revokeRole` 在 Agent 上；Schema 级 `BIND_ROLE` 即可覆盖 Agent 子树。
9. **协议 type 重复**：同一 Agent 内 `protocols[].type` 必须唯一，`addProtocol` 遇重复抛 `IllegalArgumentException`。
10. **Agent 上 Capabilities 区分"未声明 / 声明为空"**：`null` 与空数组统一落库为 `[]`。
11. **A2A/MCP 协议协议层验证**：`agent_card_url` 是不透明字符串，v1 不抓取、不解析、不做 schema 校验，也不区分 A2A/MCP 是否必填 URL。
12. **`SupportsStatistics` 在 Agent 接口层落地**：与 `Model` 一致——`SupportsStatistics` 只由 `MetadataObjectStatisticsOperations` 在 metadata-object 维度实现，不在资产接口（`Agent`、`Model`）上 `extends`。
13. **`AgentChange` 上 per-protocol 细粒度 setter**：v1 不提供 `SetProtocolProperty` / `RemoveProtocolProperty`；修改某条既有 protocol 的 `properties` 走 `RemoveProtocol(type)` + `AddProtocol(newProtocol)` 组合实现（走法 B-1，详见 §5.1 AgentProtocol 表的"作用域区分"）。`AgentChange` 类总数保持 9 个。

---

## 4. 方案调研

### 方案 A：Schema 级 + 增量 `AgentChange[]`（已选）

**是什么**：Agent 挂在 `(metalake, catalog, schema)` 下，REST 端点前缀为 `/api/metalakes/{m}/catalogs/{c}/schemas/{s}/agents`。所有更新走 `POST /agents/{name}/alter`，body 是 `AgentChange[]`。**不**引入新的 `Catalog.Type` 或 `CatalogProvider`——完全沿用 Function 的"挂在任意 catalog 的 schema 下"模式。

**优点**：
- 完全对齐 `Function` 既有模式（REST 路径在 schema 下、自管元数据、相同的 dispatcher 链）。
- `AgentChange[]` 事件给出一份干净的逐变更审计轨迹，将来增加新的变更类型也很容易。
- Schema 级作用域让 Agent 自然归属于某个逻辑域（schema），避免扁平命名空间导致的混淆。

**缺点**：
- 用户必须先创建 schema，才能注册 Agent（多一步）。
- 对于简单更新，比整体 PUT 多一些样板代码。

**决策**：**已选** —— 直接对齐 Function/Model/Table 既定惯例。

### 方案 B：Metalake 级 + 整体 PUT

**是什么**：Agent 直接挂在 metalake 下，路径为 `/api/metalakes/{m}/agents`。更新用整体 `PUT` 整个对象。

**优点**：
- URL 结构更简单（路径里没有 catalog/schema）。
- 整体 PUT 贴近用户最初草稿，对变更语义的思考更少。

**缺点**：
- 与其他所有 Gravitino 资产（Function、Model、Table、View、Fileset、Topic）的风格不一致。
- 整体 PUT 丢失细粒度的变更审计；事件载荷变成"新对象"，而不是"改了什么"。
- 没有自然的命名空间边界（没有 schema），命名冲突概率更高。

**决策**：**拒绝** —— 与 Gravitino 既有资产惯例不一致，且削弱审计粒度。

### 方案 C：新 `Catalog.Type.AGENT` + 新 `CatalogProvider`，Catalog 级

**是什么**：新增一个 `Catalog.Type.AGENT` 枚举值，再写一个 `AgentCatalogImpl extends BaseCatalog` 并通过 `META-INF/services/org.apache.gravitino.CatalogProvider` 注册；Agent 直接挂在 catalog 下。

**优点**：
- 隔离度最高；组织可以专门建一个"AI Catalog"。

**缺点**：
- 强迫用户专门安装 / 配置一个 catalog 才能注册 Agent。
- Agent 元数据完全由 Gravitino 内部持有，没有外部系统让 provider 去包装——provider 层毫无价值。
- 把 Agent 当作"另一种存储技术"，但它本质上是治理概念。
- **违反 Function 的既有模式**：`Catalog.Type` 枚举当前只有 `RELATIONAL / FILESET / MESSAGING / MODEL` 四种，没有 `FUNCTION`——Function 也是挂在任意 catalog 的 schema 下，没有自己的 provider。Agent 应该遵循同一模式。

**决策**：**拒绝** —— 对一个自管、仅元数据的资产来说过度设计；且与 Function 的成熟模式相违背。

---

## 5. 设计方案

### 5.1 数据模型

#### Agent 对象

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | string | 是 | schema 内唯一。由 Gravitino 标准命名校验器校验。 |
| `endpoint` | string (URI) | 是 | Agent 接收请求的 URL。存储为不透明字符串，不强制协议。在 Java 接口上 `String endpoint()` 不带 default（对齐 `Model.name()`、`Model.latestVersion()` 的非 default 形态）。 |
| `capabilities` | string[] | 否 | 自由形式的能力标签（如 `text2sql`、`schema-explain`）。`null` 与空数组等价，统一落库为 `[]`（v1 不区分"未声明"与"声明为空"）。Java 接口 `default` 返回 `new String[0]`。 |
| `protocols` | `AgentProtocol[]` | 否 | 有序的协议描述符列表，见下表。同一 Agent 内 `protocols[].type` **不可重复**；重复时 `addProtocol` 抛 `IllegalArgumentException`。Java 接口 `default` 返回 `new AgentProtocol[0]`。 |
| `owner` | string | 是 | 逻辑所有者（通常是用户名）。与 RBAC owner 独立。Java 接口 `String owner()` 不带 default，与 `Model` 一致。 |
| `comment` | string | 否 | 人类可读的描述。 |
| `properties` | string→string map | 否 | 预留扩展位，v1 核心实现不读。Java 接口 `default` 返回 `Collections.emptyMap()`。 |
| `audit_info` | object | 自动 | `creator`、`create_time`、`last_modifier`、`last_modified_time`（继承自 `Auditable`）。 |

> **与 `Model` 对齐细节**：Agent 不 `extends HasIdentifier`（api 模块里 `HasIdentifier` 仅在 core Entity 层使用）；Agent 不 `extends SupportsStatistics`（`SupportsStatistics` 仅由 `MetadataObjectStatisticsOperations` 在 metadata-object 维度实现，不下沉到资产接口，参见 §3 非目标 #12）；Tag/Policy/Role 通过 `default supportsTags() / supportsPolicies() / supportsRoles()` 暴露（对齐 `Model.java:84-102`）。

#### AgentProtocol 对象

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `type` | string | 是 | 协议族标识（如 `"A2A"`、`"MCP"`）。自由字符串，Gravitino 不维护枚举。同 Agent 内 `type` 唯一，参见上表约束。 |
| `agent_card_url` | string (URI) | 否 | 协议描述符（如 A2A agent card）的获取 URL。v1 不强制非空、不抓取、不解析；保留为可选字段。 |
| `properties` | string→string map | 否 | 协议级扩展位（`auth`、`preferredTransport`、`version` 等）。Gravitino 不解释键值，原样存取。作用域窄于 Agent 级 `properties`：per-agent keys 覆盖整 agent 的运维（如 `rate-limit.rpm`），per-protocol keys 覆盖协议连接细节。Java 接口 `default` 返回 `Collections.emptyMap()`。 |

> **作用域区分**：`Agent.properties` 是整个 Agent 的运维元数据（如 `rate-limit.rpm`、`routing.priority`）；`AgentProtocol.properties` 是该协议连接时的细节（如 A2A 的 `auth=bearer`、`preferredTransport=jsonrpc+http`）。两者命名相同但作用域不同，文档全文一致使用此约定。

### 5.2 Java API

Agent 接口族沿用 Function 的包路径与命名约定，放在 `org.apache.gravitino.agent`，与 `org.apache.gravitino.function` 并列。命名风格、注解（`@Evolving`）、异常命名、`Change` 模式均与 Function 严格对齐。

#### `org.apache.gravitino.agent.Agent`

```java
package org.apache.gravitino.agent;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

@Evolving
public interface Agent extends Auditable {

  String name();
  String endpoint();
  String owner();

  default String[] capabilities() { return new String[0]; }
  default AgentProtocol[] protocols() { return new AgentProtocol[0]; }
  @Nullable default String comment() { return null; }
  default Map<String, String> properties() { return Collections.emptyMap(); }

  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Agent does not support tag operations.");
  }
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Agent does not support policy operations.");
  }
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Agent does not support role operations.");
  }
}
```

#### `org.apache.gravitino.agent.AgentProtocol`

```java
@Evolving
public interface AgentProtocol {
  String type();
  @Nullable String agentCardUrl();
}
```

#### `org.apache.gravitino.agent.AgentChange`

采用与 `FunctionChange` 完全相同的"密封式静态工厂 + 嵌套 final class"模式：每种变更是一个不可变 final class，构造器内做参数校验；`AgentChange` 接口本身不暴露任何 `@type` discriminator，序列化由 REST 层 `DTOConverters` 完成。v1 包含的具体类型：

| 静态工厂 | 嵌套 final class | 载荷 | 作用 |
| --- | --- | --- | --- |
| `AgentChange.renameAgent(String newName)` | `RenameAgent` | `newName` | 在 schema 内重命名 Agent。 |
| `AgentChange.setProperty(String property, String value)` | `SetProperty` | `property`, `value` | 新增或覆盖一个属性。 |
| `AgentChange.removeProperty(String property)` | `RemoveProperty` | `property` | 删除一个属性。 |
| `AgentChange.addProtocol(AgentProtocol protocol)` | `AddProtocol` | `protocol` | 追加一个协议描述符；若已存在同 `type` 抛 `IllegalArgumentException("duplicate protocol type: " + protocol.type())`。 |
| `AgentChange.removeProtocol(String type)` | `RemoveProtocol` | `type` | 按 `type` 严格匹配删除（因禁止重复，最多匹配一条）；不存在时抛 `NoSuchAgentException`。 |
| `AgentChange.addCapability(String capability)` | `AddCapability` | `capability` | 追加一个能力标签。 |
| `AgentChange.removeCapability(String capability)` | `RemoveCapability` | `capability` | 删除一个能力标签（首个匹配；capabilities 允许重复）。 |
| `AgentChange.updateComment(String newComment)` | `UpdateComment` | `newComment` | 替换描述。 |
| `AgentChange.setOwner(String newOwner)` | `SetOwner` | `newOwner` | 替换所有者。 |

#### `org.apache.gravitino.agent.AgentCatalog`（能力接口，必备）

`AgentCatalog` 是挂在 `Catalog` 上的 capability 接口，与 `FunctionCatalog` 同构。客户端通过 `Catalog.asAgentCatalog()` 拿到它；服务端不通过 `CatalogOperations` 暴露（因为 Agent 没有 `Catalog.Type.AGENT` 也没有 provider），由 `AgentOperationDispatcher` 链直接处理，`ManagedAgentOperations` 对接 `AgentCatalog` 接口，与 `ManagedFunctionOperations` 同构。

```java
@Evolving
public interface AgentCatalog {

  /**
   * List the agents in a schema.
   *
   * @param namespace A namespace. Must be a function/agent namespace of level 3.
   * @return An array of agent identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listAgents(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Get an agent by {@link NameIdentifier}.
   *
   * @param ident An agent identifier. Must be a function/agent identifier.
   * @return The agent with the given name.
   * @throws NoSuchAgentException If the agent does not exist.
   */
  Agent getAgent(NameIdentifier ident) throws NoSuchAgentException;

  /**
   * Check if an agent with the given name exists.
   *
   * @param ident The agent identifier.
   * @return True if the agent exists, false otherwise.
   */
  default boolean agentExists(NameIdentifier ident) {
    try {
      getAgent(ident);
      return true;
    } catch (NoSuchAgentException e) {
      return false;
    }
  }

  /**
   * Register an agent.
   *
   * @param ident The agent identifier.
   * @param endpoint The agent endpoint URL (opaque string).
   * @param capabilities Optional capability tags (free-form, e.g. "text2sql").
   * @param protocols Optional ordered protocol descriptors.
   * @param comment The optional agent comment.
   * @param properties Optional user-defined properties (may be null).
   * @param owner The logical owner of the agent. Required, must be non-blank; aligned with
   *     Model.owner / Model.name non-default shape.
   * @return The registered agent.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws AgentAlreadyExistsException If the agent already exists.
   */
  Agent registerAgent(
      NameIdentifier ident,
      String endpoint,
      @Nullable String[] capabilities,
      @Nullable AgentProtocol[] protocols,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      String owner)
      throws NoSuchSchemaException, AgentAlreadyExistsException;

  /**
   * Applies {@link AgentChange changes} to an agent.
   *
   * @param ident the {@link NameIdentifier} of the agent to alter.
   * @param changes the several {@link AgentChange} instances to apply.
   * @return the updated {@link Agent} instance.
   * @throws NoSuchAgentException If the agent does not exist.
   * @throws IllegalArgumentException If the change is rejected.
   */
  Agent alterAgent(NameIdentifier ident, AgentChange... changes)
      throws NoSuchAgentException, IllegalArgumentException;

  /**
   * Drop an agent by name.
   *
   * @param ident The name identifier of the agent.
   * @return True if the agent is deleted, false if the agent does not exist.
   */
  boolean dropAgent(NameIdentifier ident);
}
```

> 与 Function 的对齐要点：`listAgents(Namespace)` 返回 `NameIdentifier[]`，**没有** `listAgentInfos`（Model 也没有），需要详情时 caller 直接循环 `getAgent`；`agentExists` 是 `default` 实现；`alterAgent` 用可变参数 `AgentChange...` 而不是数组；`dropAgent` 不抛 `NoSuchAgentException`（不存在时返回 `false`）。

### 5.3 REST API

所有端点共享前缀：

```
/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/agents
```

> 集合名 `agents` 与 schema 名无关；本文档示例为避免双重 `agents` 把 schema 名取作 `ai_agents`。实际项目里建议将承载 Agent 的 schema 命名为 `ai_agents`、`agent_registry` 之类。

| Method | Path | 用途 | 鉴权表达式 |
| --- | --- | --- | --- |
| `GET` | `/` | 列出 schema 下所有 Agent。可选 `?details=true` 切换 `NameIdentifier[]` / `Agent[]` 响应。 | `LOAD_SCHEMA_AUTHORIZATION_EXPRESSION`（同 Function） |
| `POST` | `/` | 注册一个新 Agent。 | `OWNER \|\| METALAKE \|\| CATALOG \|\| SCHEMA_OWNER_WITH_USE_CATALOG \|\| ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_REGISTER_AGENT` |
| `GET` | `/{agent}` | 按名取单个 Agent。不存在返 404。 | `LOAD_SCHEMA_AUTHORIZATION_EXPRESSION` |
| `POST` | `/{agent}/alter` | 应用 `AgentChange[]`。不存在返 404；参数校验失败返 400。 | `OWNER \|\| METALAKE \|\| CATALOG \|\| SCHEMA_OWNER_WITH_USE_CATALOG \|\| ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_MODIFY_AGENT` |
| `DELETE` | `/{agent}` | 软删除 Agent。 | `OWNER \|\| METALAKE \|\| CATALOG \|\| SCHEMA_OWNER_WITH_USE_CATALOG \|\| ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_MODIFY_AGENT` |
| `GET` | `/{agent}/tags` | 列出已挂载的 tag。 | `LOAD_SCHEMA_AUTHORIZATION_EXPRESSION` |
| `POST` | `/{agent}/tags` | 挂载 tag。 | `APPLY_TAG` |
| `GET` | `/{agent}/policies` | 列出已挂载的 policy。 | `LOAD_SCHEMA_AUTHORIZATION_EXPRESSION` |
| `POST` | `/{agent}/policies` | 挂载 policy。 | `APPLY_POLICY` |

Tag 和 policy 绑定端点会复用 Gravitino 现有的 `MetadataObjectTagOperations` / `MetadataObjectPolicyOperations`，前提是 `MetadataObject.Type.AGENT` 已加入。

### 5.4 持久化 Schema

Agent 资产是**纯新增表**，沿用 Gravitino 的"两份真相"机制：

- **`scripts/{h2,mysql,postgresql}/schema-1.3.0-*.sql`** 是官方发布的 reference schema。每个 release 一份完整 DDL；ops 拿它做生产 init 或 sanity check。Agent 在 1.3.0 release 中追加 `agent_meta` + `agent_version_info` 两段 `CREATE TABLE`。
- **MyBatis 自动建表**（`gravitino.entity.store.auto-create-table=true` 时）在 server 启动时按 mapper 注解 / Provider 反射建表，是 dev / 单测场景的兜底；**生产部署仍应跑 scripts 里的 SQL**，不要依赖自动建表。

Agent 是新表，**不需要** `upgrade-1.x-to-1.y-*.sql`：1.3.0 之前的版本根本不存在 `agent_meta`，所以没有"老 Gravitino 升级到带 Agent 的 Gravitino"这条路径——部署 1.3.0 必须 fresh init schema-1.3.0-*.sql。**未来**对 agent_meta / agent_version_info 的列变更（如 1.4.0 加列）需要新增 `upgrade-1.3.0-to-1.4.0-*.sql`，沿用现有 `upgrade-*.sql` 命名约定。

Agent 的两个 mapper / provider（`AgentMetaMapper` + `AgentMetaBaseSQLProvider` + `AgentMetaPostgreSQLProvider`，以及对应的版本表）按 Function 的同构方式生成三方言 SQL，但**只有 dev/test 路径**用——生产路径走 scripts/。

新增两张表，建模参考 `function_meta` 与 `function_version_info`：

```sql
CREATE TABLE agent_meta (
  agent_id           BIGINT        NOT NULL,
  agent_name         VARCHAR(255)  NOT NULL,
  metalake_id        BIGINT        NOT NULL,
  catalog_id         BIGINT        NOT NULL,
  schema_id          BIGINT        NOT NULL,
  endpoint           VARCHAR(1024) NOT NULL,
  capabilities       TEXT          NULL,   -- JSON 数组；可空
  protocols          TEXT          NULL,   -- JSON 数组；可空
  owner              VARCHAR(255)  NULL,
  comment            TEXT          NULL,
  properties         TEXT          NULL,   -- JSON 对象
  audit_info         TEXT          NOT NULL,
  current_version    INT           NOT NULL DEFAULT 1,
  last_version       INT           NOT NULL DEFAULT 1,
  deleted_at         BIGINT        NOT NULL DEFAULT 0,
  PRIMARY KEY (agent_id),
  UNIQUE KEY uk_mcs_name (metalake_id, catalog_id, schema_id, agent_name, deleted_at)
);

CREATE TABLE agent_version_info (
  id                 BIGINT       NOT NULL,
  metalake_id        BIGINT       NOT NULL,
  catalog_id         BIGINT       NOT NULL,
  schema_id          BIGINT       NOT NULL,
  agent_id           BIGINT       NOT NULL,
  version            INT          NOT NULL,
  agent_comment      TEXT         NULL,
  agent_properties   TEXT         NULL,   -- JSON 对象
  audit_info         TEXT         NOT NULL,
  deleted_at         BIGINT       NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY uk_avi_agent_version (agent_id, version, deleted_at)
);
```

`capabilities`、`protocols`、`properties` 用 `TEXT` + JSON 序列化；Agent 的"当前态"在 `agent_meta`，每次 alter 后写一条新版本到 `agent_version_info`（与 Function 一致）。Tag/policy 绑定走已有的 `tag_relation_object` / `policy_relation_object` 等 metadata-object 关系表。

> 软删除：`(metalake_id, catalog_id, schema_id, agent_name, deleted_at)` 唯一键允许同名 Agent 在删除后被同名复用，同时保留旧行在版本表里——默认 7 天清理窗口、最多保留 10 个版本（沿用 Gravitino 全局 `gravitino.entity.store.soft-delete.*` 配置）。

### 5.5 用户流程

1. 平台工程师先为 AI Agent 注册创建一个 schema：`POST /api/metalakes/acme/catalogs/ai_lakehouse/schemas/ai_agents`。
2. 开发者注册一个 Agent：
   ```bash
   curl -X POST /api/metalakes/acme/catalogs/ai_lakehouse/schemas/ai_agents/agents \
     -H "Authorization: Bearer $TOKEN" \
     -d '{
       "name": "sql-assistant",
       "endpoint": "https://agents.internal/sql-assistant",
       "capabilities": ["text2sql", "schema-explain"],
       "protocols": [{"type": "A2A",
                       "agentCardUrl": "https://agents.internal/sql-assistant/.well-known/agent.json"}],
       "owner": "zhang.san",
       "comment": "SQL 生成与表结构解释 Agent"
     }'
   ```
   响应：`200 OK`，包含完整 Agent 对象及 `audit_info`。
3. 其他开发者发现 Agent：
   ```bash
   # 仅返回 NameIdentifier 列表
   curl /api/metalakes/acme/catalogs/ai_lakehouse/schemas/ai_agents/agents
   # 返回 Agent[] 详情
   curl "/api/metalakes/acme/catalogs/ai_lakehouse/schemas/ai_agents/agents?details=true"
   ```
4. 运维施加治理：
   ```bash
   # 挂分类 tag
   curl -X POST /api/metalakes/acme/objects/agent/acme.ai_lakehouse.ai_agents.sql-assistant/tags \
     -d '{"tagsToAdd": ["production-approved"]}'

   # 挂成本上限 policy
   curl -X POST /api/metalakes/acme/objects/agent/acme.ai_lakehouse.ai_agents.sql-assistant/policies \
     -d '{"policiesToAdd": ["daily-cost-cap"]}'
   ```
5. 增量更新 Agent：
   ```bash
   curl -X POST /api/metalakes/acme/catalogs/ai_lakehouse/schemas/ai_agents/agents/sql-assistant/alter \
     -d '{"changes": [
       {"@type": "addProtocol",
        "protocol": {"type": "MCP", "agentCardUrl": "https://agents.internal/mcp.json"}},
       {"@type": "setProperty",
        "property": "routing.priority", "value": "high"},
       {"@type": "updateComment", "newComment": "更新后的描述"}
     ]}'
   ```
   > 注：`AgentChange` 在 REST payload 里仍使用 `@type` discriminator 序列化（具体命名由 `DTOConverters` 决定，如 `addProtocol` / `setProperty` / `updateComment`），与 Function 的 REST 协议一致。
6. 下线：`DELETE /agents/sql-assistant` → 软删除；保留窗口内记录可恢复（恢复通过 DBA 操作 DB，v1 不暴露 `undelete` 端点）。

### 5.6 实现流程

Agent 资产复用 Gravitino 的四层 dispatcher 链（Function 模式，自外到内）：

```
Client → AgentOperations (JAX-RS)
         │
         ▼
       AgentHookDispatcher        ← 纯委派，为未来 hook 留口
         │
         ▼
       AgentEventDispatcher       ← PreEvent / PostEvent / FailureEvent
         │
         ▼
       AgentNormalizeDispatcher   ← 入参规范化
         │
         ▼
       AgentOperationDispatcher   ← TreeLock、jcasbin 鉴权、业务逻辑
         │
         ▼
       ManagedAgentOperations     ← 实现 AgentCatalog，通过 EntityStore 抽象层调存储
         │
         ▼
       EntityStore (RelationalEntityStore)
         │
         ▼
       AgentMetaService            ← PO ↔ Entity 转换（仅 JDBCBackend 内部用）
         │
         ▼
       AgentMetaMapper (MyBatis)   ← 三方言 SQL provider
         │
         ▼
       JDBC → H2/MySQL/PG

EventBus → audit-log 监听器、Lineage、自定义
```

**装配顺序（与 Function 完全一致）**：在 `GravitinoEnv.start()` 里，`AgentOperationDispatcher` 是最内层，依次包 `AgentNormalizeDispatcher` / `AgentEventDispatcher` / `AgentHookDispatcher`，最终通过 `GravitinoEnv.agentDispatcher()` 暴露。每层都是独立类，方便替换（未来可挂 `RangerHookDispatcher`）。

**调用顺序**：client → HookDispatcher → EventDispatcher (PreEvent) → NormalizeDispatcher → OperationDispatcher (TreeLock + jcasbin + ManagedAgentOperations) → EventDispatcher (PostEvent / FailureEvent)。

**与 Function 的关键差异**：
- 没有 `ManagedAgentOperations` 之外的独立 `AgentNormalizeDispatcher.normalizeCapability(...)` 之类的领域校验——所有校验在 `ManagedAgentOperations.registerAgent` / `applyChanges` 里集中完成。
- v1 不实现 `AgentHookDispatcher.alterAgentPreCheck` 等 owner / privilege 检查（与 Function 一致）。

#### 鉴权集成

- 新增权限名（与 Function 一一对应，复用既有 `Privilege.Name` 风格）：
  - `REGISTER_AGENT`：schema 级，等价于 Function 的 `REGISTER_FUNCTION`，bit 值沿用下一个可用槽位。
  - `EXECUTE_AGENT`：agent 级，"发现 + 调用" 入口权限；不在 `AgentCatalog` 方法上直接校验，由 dispatcher 视上下文决定。
  - `MODIFY_AGENT`：schema 级，覆盖 alter / drop（与 `MODIFY_FUNCTION` 同——drop 走 modify 语义）。
- `AGENT` 加入 `MetadataObject.Type` 枚举、`Entity.EntityType` 枚举；`MetadataObjects.java` 的 `parent()` switch 加 `case AGENT`（与 `FUNCTION` 共用 SCHEMA 父节点），同时把 `AGENT` 加入 `VALID_THREE_LEVEL_NAME_TYPES` 集合；`MetadataObjectUtil.toEntityType(...)` 增加 `MetadataObject.Type.AGENT → Entity.EntityType.AGENT` 映射；`Privileges.java` 新增 `AGENT_SUPPORTED_TYPES` 并加入 `MANAGE_GRANTS_SUPPORTED_TYPES`。
- 新增常量在 `Privileges.allow(...)` switch 里沿用 `RegisterFunction.allow()` 模式（每个权限单独 final class `RegisterAgent.allow()` / `ExecuteAgent.allow()` / `ModifyAgent.allow()`）。
- jcasbin GRule 模型扩展：定位 `server-common/src/main/resources/jcasbin/` 下的 `*.conf` model 文件，参考 Function 的 `g_function` / `p_function` 规则新增 `g_agent` / `p_agent` 规则，并在 metalake 默认 policy 中补 AGENT 相关项。
- Tag/policy 绑定走既有 `APPLY_TAG` / `APPLY_POLICY`，无需新增。
- **Role 绑定**：`Agent` 接口保留 `SupportsRoles` 标记以维持 mixin 一致性，但 v1 **不**实现 Role 在 Agent 上的 `grantRole` / `revokeRole` 落点 dispatcher；用户如需 Role 控制，走 schema 级 `BIND_ROLE` 覆盖 Agent 子树（参见 §3 非目标 #8）。Phase 1 任务清单相应地不增 `AgentRoleDispatcher` 项。

#### 事件集成

每次操作发布 PreEvent（带拟执行状态）、PostEvent（成功）、FailureEvent（失败）。事件载荷包含完整 Agent 对象（Pre/Post）或异常（Failure）。内置 `audit-log` 监听器自动接收。

### 5.7 向后兼容性

这次改动是 **纯增量**：

- 没有任何已有 REST 端点的请求 / 响应结构被改动。
- 没有任何已有权限的含义被改变。
- 没有任何已有实体增减字段。
- `MetadataObject.Type.AGENT` 是一个新增的枚举值——严格的"加法"。

现有客户端（Python、Java、CLI）无需任何改动即可继续工作。它们通过 import 新包来启用 Agent API。

---

## 6. 任务拆分

按 Phase 分组，依赖在前。每个 checkbox 是 **1 个 Subtask = 1 个 commit**，挂在一个 **Epic** 下；Phase 之间是 **PR 边界**。

> **粒度约定**：
> - **Epic** = 1 个（标题 `[EPIC] Gravitino Agent Asset`），作为父 issue；其 `Task list` 字段列出 6 个 Phase 的 PR 编号与每个 Phase 内的 Subtask 编号（参考 `.github/ISSUE_TEMPLATE/epic.yml`）。
> - **Subtask issue** = 每个 checkbox 1 个（标题 `[Subtask] <简述>`，标签 `subtask`，父 issue 字段填 Epic 编号；模板 `.github/ISSUE_TEMPLATE/subtask.yml`）。
> - **Commit** = Subtask 级别。一个 Subtask 一个 commit，commit 标题格式 `[Subtask] <标题> (#<subtask-issue>)`；同一 Subtask 内的所有文件改动（含对应单元测试）落在同一 commit，不另起测试 commit。
> - **PR（Phase PR）** = Phase 边界。Phase 1、Phase 2、Phase 3、Phase 4 各自 1 个 PR（独立可编译、可测、可 squash merge）；Phase 5 + Phase 6 合并为 1 个 PR（纯文档 + 发布流程）。**总计 5–6 个 PR**。
> - **测试前置**：每个 Phase 的单元测试在**该 Phase 内**完成，**不**延后到 Phase 5。Phase 5 只负责 OpenAPI / 用户文档 / 集成测试 / 发布。**测试滞后会导致 bug 漏过 phase 边界**（真实教训：Phase 1 #9 AgentMetaService 没单测，潜伏了 4 个 bug 到 Phase 5 阶段才被 test 触发修——`AgentProtocol` 反序列化接口失败、`deleteAgent` 缺 NotFound catch、`applyChanges` 私有无法复用、`NameIdentifierUtil.toMetadataObject` 不识 AGENT）。
> - 拆分原则：每个 Phase PR 必须是"自包含"——能 `./gradlew compileJava test -PskipITs` 通过；不允许 Phase 1 PR 引用 Phase 2 才出现的新类。
> - **PR-level 完整构建**：每个 Phase PR 提交前**必须** `./gradlew compileDistribution -x test` 通过。这会顺带验证 `:api:javadoc`（api 模块跑 javadoc 配 `-Werror`，新 public 方法漏 Javadoc 就会失败）、checkstyle、spotless 等所有静态检查。**这一条比单元测试更严**，能抓到像"public 方法漏 @return Javadoc"这种只靠 test 抓不到的 bug。真实教训：Phase 1 #2 AgentChange 9 个 getter 漏 Javadoc，单元测试全过，但 `compileDistribution` 失败——补 commit `08e5ee816` 才修。

### Phase 1：核心 API 与存储

> **Phase 1 验收标准**：除脚本 / DTO / 权限映射这几类纯数据类外，**所有核心类（AgentEntity / AgentPO / AgentPOAgentMetaMapper / AgentMetaService 等）必须在同一 Phase 落地时附带单元测试**，没有测试 = 该 Subtask 不算完成。Phase 1 PR 不接受"Subtask 落地、测试留 Phase 5"的状态。
>
> **Phase 1 验收补充：AGENT 在「按 MetadataObject.Type dispatch 的所有 Map / switch」必须显式补齐**。历史教训——Phase 1 #14 给 `MetadataObject.Type.AGENT` 加 enum 后，单测只覆盖 `MetadataObjectUtil.toEntityType`，以下几处独立持有一份 dispatch 表的事实被遗漏，每个都会在生产触发 NPE / IllegalArgumentException：
>
> | # | 位置 | 类别 | 修复 commit |
> |---|---|---|---|
> | 1 | `JDBCBackend.insert` 的 `instanceof` 链（缺 `AgentEntity` 分支） | Dispatcher (write) | `fd813d46f` |
> | 2 | `MetadataObjectUtil.checkMetadataObject` switch | Dispatcher (read) | `1b7676949` |
> | 3 | `TagManager.SUPPORTED_METADATA_OBJECT_TYPES_FOR_TAGS` / `PolicyManager.SUPPORTED_METADATA_OBJECT_TYPES_FOR_POLICIES` 集合 | Whitelist | `1b7676949` |
> | 4 | `MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP` 和 `TYPE_TO_STORAGE_OPS_MAP` | Dispatcher (read) | **未修，需要 AgentPOStorageOps（Phase 2 follow-up）** |
> | 5 | `JcasbinAuthorizationCacheKeys.metadataIdCacheKey` switch | Cache key | `d682e9c02` |
>
> Phase 1 PR review checklist：审计 `grep -rn 'MetadataObject.Type\.\|switch.*\.type()' core/ server-common/ server/`，确认新增的 `AGENT` 在每一处 dispatch 表中都被覆盖。**单测覆盖一条路径 ≠ 该类型在所有 dispatch 表中都被加入**——这是个反复验证的家族 bug。

- [ ] 在 `MetadataObject.Type` 枚举加入 `AGENT`，在 `Entity.EntityType` 枚举加入 `AGENT`，并在 `api/MetadataObjects.java` 的 `parent()` switch 加 `case AGENT`（与 `FUNCTION` 共用 SCHEMA 父节点），同时把 `AGENT` 加入 `VALID_THREE_LEVEL_NAME_TYPES` 集合
- [ ] 在 `api/src/main/java/org/apache/gravitino/agent/` 定义 `Agent`、`AgentProtocol`、`AgentChange`（含 `RenameAgent` / `SetProperty` / `RemoveProperty` / `AddProtocol` / `RemoveProtocol` / `AddCapability` / `RemoveCapability` / `UpdateComment` / `SetOwner` 9 个嵌套 final class 与对应静态工厂方法），参照 `FunctionChange.java`
- [ ] 在 `api/agent/` 定义 `AgentCatalog` 能力接口（含 `default agentExists`）；参照 `FunctionCatalog.java`
- [ ] 在 `api/exceptions/` 新增 `AgentAlreadyExistsException` 与 `NoSuchAgentException`，参照 `FunctionAlreadyExistsException` / `NoSuchFunctionException`
- [ ] 在 `core/src/main/java/org/apache/gravitino/meta/` 实现 `AgentEntity`（实现 `Entity`、`Auditable`、`Agent`；`HasIdentifier` 通过 `Entity` 接口的 identifier 字段满足；`SupportsTags/Policies/Roles` 由 `Agent` 接口的 default 方法实现）**+ 配套 `TestAgentEntity` 单测**
- [ ] 在 `core/src/main/java/org/apache/gravitino/storage/relational/po/` 定义 `AgentPO` 与 `AgentVersionPO`
- [ ] 在 `core/src/main/java/org/apache/gravitino/storage/relational/mapper/` 定义 `AgentMetaMapper`、`AgentVersionMetaMapper` 接口与 `AgentMetaSQLProviderFactory`（含 H2/MySQL/PostgreSQL 三个 dialect provider），参照 `FunctionMetaSQLProviderFactory` 的三 provider 结构
- [ ] 在 `MapperHelpers` 注册新 mapper，在 `JdbcDatabase` 注册 JDBC config
- [ ] 实现 `AgentMetaService` 处理 PO ↔ Entity 转换（参照 `FunctionMetaService`，含软删除与版本写入）**+ 配套 `TestAgentMetaService` 单测**（覆盖 insert / list / get / delete / alter 五条主要路径 + 协议 type 唯一性校验 + rename+setOwner 组合）
- [ ] 在 `scripts/{h2,mysql,postgresql}/schema-1.3.0-*.sql` 末尾追加 `agent_meta` + `agent_version_info` 的 `CREATE TABLE`（参考 `function_meta` 段；H2/MySQL 用反引号 + ENGINE=InnoDB，PostgreSQL 用双引号 + `COMMENT ON` + 独立 `CREATE INDEX`）
- [ ] 在 `common/src/main/java/org/apache/gravitino/dto/agent/` 增加 `AgentDTO`、`AgentProtocolDTO`、`AgentRegisterRequest`、`AgentRegisterResponse`、`AgentListResponse`、`AgentResponse`、`AgentUpdatesRequest`，以及对应的 `ErrorConstants` 错误码
- [ ] 在 `DTOConverters` 增加 `toAgentDTO(...)`、`toAgentProtocolDTO(...)`、`fromAgentRequest(...)`、`fromAgentProtocolRequest(...)`
- [ ] 在 `api/authorization/Privileges.java` 增加 `REGISTER_AGENT` / `EXECUTE_AGENT` / `MODIFY_AGENT` 三个 `Privilege.Name` 与 `RegisterAgent.allow()` / `ExecuteAgent.allow()` / `ModifyAgent.allow()` final class；同时新增 `AGENT_SUPPORTED_TYPES` 并加入 `MANAGE_GRANTS_SUPPORTED_TYPES`
- [ ] 更新 jcasbin GRule 模型（`server-common/src/main/resources/jcasbin/*.conf`）：参照 Function 的 `g_function` / `p_function` 规则新增 `g_agent` / `p_agent` 规则；并在 metalake 默认 policy 中补 AGENT 相关项
- [ ] 在 `core/src/main/java/org/apache/gravitino/utils/MetadataObjectUtil.java` 增加 `MetadataObject.Type.AGENT → Entity.EntityType.AGENT` 映射

### Phase 2：Dispatcher 链

> **Phase 2 验收标准**：每个 dispatcher 与 hook 类必须配套 mock-based 单测（沿用 `TestFunctionHookDispatcher` 的模式），验证 Pre / Success / Failure 事件序列 + owner 设置 + 异常透传。无测试 = Subtask 不算完成。

- [x] 在 `core/src/main/java/org/apache/gravitino/catalog/` 定义 `AgentDispatcher` 接口（参照 `FunctionDispatcher`，是空接口仅 extends `AgentCatalog`）
- [ ] 实现 `AgentOperationDispatcher`（TreeLock、命名空间校验、jcasbin 调用、业务逻辑），参照 `FunctionDispatcher` 实现
- [ ] 实现 `AgentNormalizeDispatcher`（入参规范化、默认值），规范化规则：`properties == null → empty map`、`capabilities/protocols == null → []`、`endpoint` 不 trim trailing slash
- [ ] 实现 `AgentHookDispatcher`（纯委派，为未来 hook 留口）+ 配套 `TestAgentHookDispatcher` 单测（验证 registerAgent → setOwner 序列）
- [ ] 实现 `AgentEventDispatcher`（PreEvent / PostEvent / FailureEvent）+ 配套 `TestAgentEventDispatcher` 单测（mock `EventBus`，验证 register / alter / drop / get / list 都正确发 Pre + 成功 + Failure 三事件，agentExists 不发任何事件）
- [ ] 在 `core/src/main/java/org/apache/gravitino/listener/api/event/agent/` 定义 21 个事件类（命名空间 `org.apache.gravitino.listener.api.event.agent.*`，与 `function.*` / `model.*` 并列）—— 实际事件清单按 Function 1:1 镜像：`RegisterAgentEvent` / `RegisterAgentPreEvent` / `RegisterAgentFailureEvent` + `AlterAgentEvent` / `AlterAgentPreEvent` / `AlterAgentFailureEvent` + `DropAgentEvent` / `DropAgentPreEvent` / `DropAgentFailureEvent` + `GetAgentEvent` / `GetAgentPreEvent` / `GetAgentFailureEvent` + `ListAgentsEvent` / `ListAgentsPreEvent` / `ListAgentsFailureEvent` + `AgentExistsEvent` / `AgentExistsPreEvent` + 顶层基类 `AgentEvent` / `AgentPreEvent` / `AgentFailureEvent` + `AgentInfoEvent`（Function 同样有此中间基类）。任务清单原列的 `AgentFailureEvent` 已在顶层基类覆盖。
- [x] 在 `core/src/main/java/org/apache/gravitino/catalog/` 实现 `ManagedAgentOperations`，对接 `AgentCatalog` 接口（Phase 2 Subtask A 落地：commit `68b7d7469`）
- [ ] 把 `AgentDispatcher` 装配进 `GravitinoEnv` 初始化流程

### Phase 3：REST API

> **Phase 3 验收标准**：`AgentOperations` JAX-RS 资源必须配套 `TestAgentOperations` 单测（参考 `TestFunctionOperations`），覆盖路径 / 请求体 / 状态码 / 鉴权失败路径。无测试 = Subtask 不算完成。

- [ ] 在 `server/src/main/java/org/apache/gravitino/server/web/rest/` 实现 `AgentOperations` JAX-RS 资源（路径前缀 `metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/agents`），参照 `FunctionOperations.java` 的注解组合与鉴权表达式 + 配套 `TestAgentOperations` 单测
- [ ] 在 Jersey 资源绑定里注册 `AgentOperations`（参照 `Binder` 类对 `FunctionOperations` 的注册方式）
- [ ] 在 `server/src/main/java/org/apache/gravitino/server/web/ExceptionHandlers.java` 增加 `handleAgentException(...)`，参照 `handleFunctionException` + 配套单测
- [ ] 验证 `MetadataObjectTagOperations` / `MetadataObjectPolicyOperations` 对 `AGENT` 类型的端到端支持

### Phase 4：客户端

- [ ] 在 `clients/client-java/` 增加 Java 客户端支持（`GravitinoAgent` 类、`AgentChange` 静态工厂、DTO 更新、客户端 `BaseSchemaCatalog` 实现 `AgentCatalog` 并通过 `asAgentCatalog()` 暴露）
- [ ] 在 `clients/client-python/` 增加 Python 客户端支持（pydantic 模型、同步 / 异步方法），与 Function 客户端保持结构一致
- [ ] 视情况在 `clients/cli/` 加 CLI 支持（`gravitino agent list/register/alter/delete`），复用 Function CLI 子命令骨架

### Phase 5：文档、集成测试与发布

> **Phase 5 范围已缩小**：Phase 1 / 2 / 3 / 4 内**各自的单元测试已经在落地时完成**。Phase 5 只做：OpenAPI 规范 + 用户文档 + 跨模块集成测试 + 发布流程。**不**在 Phase 5 补 Phase 1/2/3 的单测——那是 Phase 1/2/3 各自的 acceptance。

- [ ] 在 `docs/open-api/agents.yaml` 写 OpenAPI 规范（与 Function 的 `functions.yaml` 并列）
- [ ] 用 `./gradlew :docs:build` 校验
- [ ] 在 `docs/agent-asset.md` 写面向用户的文档
- [ ] 在 `docs/index.md` 加交叉链接
- [ ] 加集成测试，标记 `@Tag("gravitino-docker-test")`，覆盖注册 / 列表 / 改 / 删 / tag 绑定 / policy 绑定 / 鉴权失败用例
- [ ] 跑 `./gradlew test -PskipITs -PskipDockerTests=false` 和 `./gradlew build -PskipDockerTests=false`

### Phase 6：发布

- [ ] 在下一个版本的 `CHANGELOG.md` 更新
- [ ] 在 `docs/how-to-upgrade.md` 加升级说明（纯增量——`MetadataObject.Type.AGENT` 是新增枚举值、jcasbin GRule 模型新增 `g_agent` / `p_agent` 规则并有 default policy 兜底，**不**需要数据迁移）
- [ ] （可选）在 `web/web` 与 `web-v2/web` 前端增加 Agent 资产的可视化页面；v1 不强制

---

## 7. 质量检查清单

提交本文档评审前核对：

- [x] Apache License HTML 注释块是文件首块
- [x] Background 既描述了当前状态也描述了痛点
- [x] Goals 都是具体且可验证的
- [x] Non-Goals 明确划出范围，每条带一行说明
- [x] Solution Investigations 列出了 3 个候选，并给出具体拒绝原因
- [x] Proposal 覆盖 API、数据模型、用户流程、实现、向后兼容
- [x] 新 REST API 定义了请求 / 响应 / 行为 / 鉴权
- [x] Task Breakdown 每项对应一个 PR，按依赖顺序、按 Phase 分组
- [x] 全程与 `org.apache.gravitino.function.*`（Function 资产）保持命名、注解、异常、dispatcher 链、SQL provider 结构对齐
- [x] 与 `org.apache.gravitino.model.Model` 实际形态对齐：`Agent` 仅 `extends Auditable`，治理能力通过 `default supportsTags()/supportsPolicies()/supportsRoles()` 暴露；`SupportsStatistics` 不在资产接口落地，由 `MetadataObjectStatisticsOperations` 在 metadata-object 维度提供；`HasIdentifier` 仅在 core Entity 层使用
- [x] `protocols[].type` 唯一约束：`addProtocol` 遇重复抛 `IllegalArgumentException`（见 §3 非目标 #9、§5.2 AgentChange 表）
- [x] Role 绑定范围：v1 不实现 Agent 级 `grantRole` / `revokeRole` dispatcher，`SupportsRoles` 仅作 mixin 占位（见 §3 非目标 #8、§5.6 鉴权集成）
- [x] `agent_card_url` 是不透明可选字段，v1 不抓取、不解析（见 §3 非目标 #11、§5.1 AgentProtocol 表）
- [x] `capabilities` 的 `null` 与空数组等价，统一落库为 `[]`（见 §3 非目标 #10、§5.1 Agent 表）
- [x] **测试前置**：Phase 1/2/3/4 的单元测试在所属 Phase 完成，**不**延后到 Phase 5。Phase 5 只做集成测试 / OpenAPI / 用户文档 / 发布流程（见 §6 粒度约定）
