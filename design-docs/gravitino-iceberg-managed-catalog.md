<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 设计文档：Gravitino 自管理 Iceberg Catalog（JDBC Backend）

## 1. 背景

### 1.1 当前的 Iceberg catalog 是一个代理

Gravitino 的 Iceberg catalog 在文档里就被描述为 *catalog proxy*（`docs/lakehouse-iceberg-catalog.md`），而它实际上也确实如此：所有 schema、table、view 操作都转发给 Iceberg SDK，由 SDK 去访问后端（`hive`、`jdbc`、`rest`、`memory`、`custom`）。

当 `catalog-backend = jdbc` 时，`IcebergCatalogOperations` 转发给 `IcebergCatalogWrapper`（`IcebergCatalogOperations.java:593-596`），后者转发给 `CatalogHandlers`，最终落到 `org.apache.iceberg.jdbc.JdbcTableOperations`。这个类会写**两样**东西：

1. 表元数据 JSON 文件，写入 warehouse（`BaseMetastoreTableOperations.writeNewMetadataIfRequired`）；
2. Iceberg JDBC 表 `iceberg_tables` 中的一行（`catalog_name`、`table_namespace`、`table_name`、`metadata_location`、`previous_metadata_location`、`iceberg_type`）。

### 1.2 Gravitino 再写一行互不相关的记录

本节描述 **Gravitino native API 路径**；Iceberg REST 路径的现状见 §1.4。

由于 Iceberg catalog 没有声明支持 `managedStorage(TABLE)`（`Capability.managedStorage` 默认不支持，`IcebergCatalogCapability` 也没有覆写它），`TableOperationDispatcher` 会把每一个 Iceberg 表当作 *unmanaged*，在 SDK 调用之后，再往 Gravitino 自己的 entity store 里写一条 `TableEntity`：

```
TableOperationDispatcher.internalCreateTable
  ├─ doWithCatalog(...)  → IcebergCatalogOperations.createTable → Iceberg SDK
  │                          ├─ 写 <warehouse>/metadata/<ver>-<uuid>.metadata.json
  │                          └─ INSERT INTO iceberg_tables ...
  └─ store.put(tableEntity, true)        // TableOperationDispatcher.java:689
        └─ INSERT INTO table_meta / table_version / table_column_version_info
```

这两次写入发生在**两条连接、两个事务**里：

* Iceberg 侧使用 `JdbcCatalog` 自己持有的 `JdbcClientPool`；
* Gravitino 侧通过 MyBatis 使用 entity store 的 `DataSource`（`TableMetaService.insertTable` → `SessionUtils.doMultipleWithCommit`）。

### 1.3 失败是静默的（Gravitino native 路径）

比"无法回滚"更糟的是：第二次写入的失败被**吞掉**了，接口仍然返回成功（`TableOperationDispatcher.java:688-696`）：

```java
try {
  store.put(tableEntity, true /* overwrite */);
} catch (Exception e) {
  LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
  return EntityCombinedTable.of(table)...;   // 照样返回成功
}
```

Schema 创建的行为完全一样（`SchemaOperationDispatcher.java:157-167`）。最终结果是一张存在于 Iceberg 中、属性里带着一条悬空 `gravitino.identifier`、却没有 `table_meta` 记录的表。只能依赖后续 `loadTable`（`imported = false`）和客户端重新 import 来恢复。

### 1.4 两条 API 路径、授权开关，在元数据上互不相同

Iceberg 表有两个入口，它们对 Gravitino 元数据的处理并不一致：

| | Gravitino native API | Iceberg REST API |
| --- | --- | --- |
| 入口 | `POST /api/metalakes/.../tables` | `POST /v1/namespaces/{ns}/tables` |
| 分发层 | `TableOperationDispatcher`（core） | `IcebergTableOperationExecutor` → `IcebergTableHookDispatcher` |
| 写 `iceberg_tables` | 是 | 是 |
| 写 `table_meta` | **总是** | **仅当 `gravitino.authorization.enable = true`** |
| 写 `table_meta` 与 SDK 调用是否同一事务 | 否 —— 两次写入是两个事务（§1.2） | 否 —— import 是独立的 `store.put` |

REST 路径的实体写入挂在 Hook 层（`IcebergTableHookDispatcher` / `IcebergNamespaceHookDispatcher` / `IcebergViewHookDispatcher`），而 Hook 层只在授权开启时装配（`RESTService.java:156-179`）。因此：

* **默认配置（`gravitino.authorization.enable = false`）下，通过 Iceberg REST 建的表完全不写 `table_meta`**；
* 打开授权后，同一个操作才会写。

也就是说，**一个开关会改变数据模型**。这带来两个后果：

1. **治理能力有洞。** 授权关闭时，通过 Spark / Flink 建的表没有 `table_meta`，tag / policy / statistics / owner / lineage 全都挂不上去；只有 Gravitino native API 建的表才能被治理。
2. **失败方向也不一致。** 授权开启时，REST 路径的 import 失败会向上抛（`IcebergTableHookDispatcher.java:206-219` 明确注释 "intentionally NOT wrapped in try-catch"），于是客户端看到 500，而 Iceberg 表已经建好；native 路径的失败则被吞掉、返回成功（§1.3）。同一个根因（两个事务），两种相反的对外表现。

这两点正是 §2 目标 6（写入与授权解耦）和 §5.1（两个入口统一到一个实现层）的动机。

### 1.5 为什么值得解决

对于真正**外部**的 Iceberg catalog（别人的 HMS、远端 REST catalog），两阶段元数据是可以接受、甚至是必要的妥协 —— Gravitino 不应该假装自己能对一个它并不拥有的系统做事务。

但对于 `catalog-backend = jdbc`、且**这个数据库就是 Gravitino 自己配置的那个库**的情况，没有任何理由接受它：两次写入落在同一个数据库，完全可以放进同一个事务。

---

## 2. 目标

1. **原子性。** 在 `managed` 模式下，创建 / 修改 / 删除 / 重命名 Iceberg 表时，`iceberg_tables` 行与 `table_meta` 行要么一起提交，要么一起不提交。
2. **SQL 与连接归 Gravitino。** managed 模式下 Iceberg SDK 不得打开任何数据库连接。不需要 `uri` / `jdbc-user` / `jdbc-password` / `jdbc-driver` 这些 catalog 属性。
3. **SDK 以库的方式使用。** Gravitino 主动调用 Iceberg 的 public API 来获得 Iceberg 语义（元数据构建与演进、元数据文件格式）；而不是由 SDK 驱动流程。
4. **federated 模式零回归。** 不带新模式开关的 `catalog-backend = jdbc` 保持现有行为，逐字节不变。
5. **保持 Iceberg 兼容性。** managed 模式建出的表，仍可被任何直接指向同一张 `iceberg_tables` 的 Iceberg 客户端、以及 Gravitino 的 Iceberg REST 服务读取。
6. **元数据写入与授权开关解耦。** managed 模式下，无论 `gravitino.authorization.enable` 取值如何，Gravitino 元数据一律写入（见 §5.1.1）。

### 2.1 实施优先级

**Iceberg REST API 优先，Gravitino native API 次之。**

理由：Iceberg REST 是 Spark / Flink / Trino 等引擎实际使用的入口，也是 managed 模式价值最直接的地方；Gravitino native API 的表级 DDL 可以复用同一个实现层，属于低成本的后续增量。

这意味着本文档中：

* §5.8 描述的是**与入口无关的写路径原语**，两个入口都收敛到它；
* §5.9 描述**入口适配器**，其中 Iceberg REST（§5.9.1）是第一阶段交付目标，Gravitino native（§5.9.2）放最后阶段；
* §9 的实施计划按此顺序排列。

## 3. 非目标

* 不改变 `catalog-backend = hive` / `rest` / `memory` / `custom`。这些后端按定义就是外部的，两阶段行为保留。
* 不让 Gravitino 负责元数据 JSON 的**内容**。文件格式是 Iceberg 的契约，必须继续由 SDK 产出。
* 不做跨库事务（XA / 2PC）。managed 模式要求 `iceberg_tables` 与 Gravitino entity store 位于同一个数据库。
* 不引入分布式事务/锁。`iceberg_tables` 上的乐观并发保留；现有 JVM 内 `TreeLock` 的限制不变。
* 视图。`iceberg_type = 'VIEW'` 不在本次范围内（见 §9）。
* 不支持 Iceberg REST 独立进程部署（`distribution/gravitino-iceberg-rest-server`）。managed 模式要求 auxiliary 模式（见 §5.1.1）。
* 不改变授权**判定**逻辑。`gravitino.authorization.enable` 仍然决定是否做权限校验；改变的只是"元数据是否写入"。

---

## 4. 方案选型

### 4.1 方案 A —— 借用 Iceberg 的连接

仍然让 `JdbcCatalog` 做写入方，但通过公开构造函数 `JdbcCatalog(Function ioBuilder, Function clientPoolBuilder, boolean initializeCatalogTables)` 注入一个自定义 `JdbcClientPool`，让 Iceberg 的 SQL 跑在 Gravitino 的事务连接上。

**否决。** `ClientPoolImpl.get()` 只要 `Deque` 非空就直接 `removeFirst()` 返回缓存连接，只有池为空时才调用 `newClient()`，因此"在 `newClient()` 里做线程绑定"根本不会被可靠地咨询。要做对就必须覆写 `run(Action, boolean)`，并让 `close`/`reconnect`/重试全部失效 —— 等于为了掩盖设计不匹配去模拟一个连接池。另外 Iceberg 通篇假设 JDBC auto-commit（`JdbcUtil` 里没有任何 `setAutoCommit`、`commit()` 或 `rollback()` 调用），把它绑到 `autoCommit = false` 的连接上，会改变它从未被编写过的语义。

### 4.2 方案 B —— 实现 `TableOperations` 作为持久化钩子

继承 `BaseMetastoreTableOperations`，只填它留空的两个钩子（`doRefresh()` 与 `doCommit()` 的默认实现就是 `throw new UnsupportedOperationException("Not implemented")`）。其余部分 —— refresh 生命周期、版本计数、过期 base 检测、元数据文件命名与写入、JSON 解析 —— 全部免费获得。

这正是 Iceberg 自己所有后端实现的方式（`JdbcTableOperations`、`HiveTableOperations`、`InMemoryCatalog` 都是它的子类）。这是成本最低的正确实现，而且 SQL 依然 100% 属于 Gravitino。它不作为**主**机制被采纳的唯一原因是控制反转：由 SDK 决定何时执行 Gravitino 的 SQL。

### 4.3 方案 C —— 纯 SDK 调用 + Gravitino 掌控 SQL（采纳）

由 Gravitino 编排。对每个 DDL 操作：

1. 调用 Iceberg 公开 API，产出一个未提交的 `TableMetadata`；
2. 调用 `TableMetadataParser.overwrite` 写入元数据文件，并拿到返回的 location；
3. 在同一个 Gravitino 事务里，用自己的 SQL 同时写 `iceberg_tables` **和** `table_meta`。

控制流始终不离开 Gravitino，SDK 被收敛成一组 Iceberg 语义库函数。具体 API 清单见 §5.4。

### 4.4 数据写入的提交同样是纯调用

Iceberg 引擎并不只通过 Gravitino 做 DDL —— Spark/Flink 会通过 Gravitino 的 Iceberg REST 服务提交 **snapshot**（`IcebergTableOperations.updateTable` → `IcebergCatalogWrapper.updateTable` → `CatalogHandlers.updateTable`）。

乍看之下这条路必须实现 `TableOperations`，因为 `CatalogHandlers.updateTable` 是这么写的：

```java
Table table = catalog.loadTable(ident);
if (table instanceof BaseTable) {
  TableOperations ops = ((BaseTable) table).operations();
  finalMetadata = commit(ops, request);      // → ops.commit(base, newMetadata)
}
```

但把 `CatalogHandlers.commit`（`CatalogHandlers.java:613-666`）读到底就会发现，`ops` 只被用了三次 —— `current()`、`refresh()`、`commit()` —— 中间所有的活儿都是纯函数：

```java
TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();   // ops 用法 1、2

request.requirements().forEach(requirement -> requirement.validate(base));   // 纯函数
TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
request.updates().forEach(update -> update.applyTo(metadataBuilder));        // 纯函数
TableMetadata updated = metadataBuilder.build();                             // 纯函数
if (updated.changes().isEmpty()) { return; }                                 // 无变化不提交

taskOps.commit(base, updated);                                               // ops 用法 3
```

而这两个关键 API 都是公开的：

| API | 可见性 |
| --- | --- |
| `UpdateRequirement.validate(TableMetadata)` | `public interface UpdateRequirement`，`default void validate(...)` |
| `MetadataUpdate.applyTo(TableMetadata.Builder)` | `public interface MetadataUpdate`，`default void applyTo(...)` |
| `TableMetadata.buildFrom(TableMetadata)` / `Builder.build()` | public static |
| `UpdateTableRequest.requirements()` / `.updates()` | public |

因此提交路径同样是纯调用：读 base → 校验 requirement → 应用 update → 写新元数据文件 → 更新 `iceberg_tables` 行。**managed 模式不需要把 `TableOperations` 当作持久化钩子。**（alterTable 路径仍需要一个极简的只读载体来获取 `PendingUpdate` 对象，见 §5.4.1，但它不承担任何持久化。）

需要一并复刻的两点：

* **重试**：`CatalogHandlers.commit` 用 `Tasks.foreach(ops).retry(COMMIT_NUM_RETRIES_DEFAULT).onlyRetryOn(CommitFailedException.class)` 做指数退避重试 —— SQL 乐观并发失败（0 行）时重新读 base、重新校验、重新应用 updates、重新写文件、重试 UPDATE。requirements 校验失败则包成 `ValidationFailureException` 直接返回客户端，不重试。
* **`isCreate` 分支**：requirements 含 `AssertTableDoesNotExist` 时（`CREATE OR REPLACE TABLE`、以及 staged create 的提交）走的是 `CatalogHandlers.create(ops, request)` 而非 `commit`，同样需要复刻。语义见 §5.9.1。

**"纯调用"的准确边界。** 上述"纯"是指**不提交表元数据、不访问数据库**，并不意味着零 I/O：客户端构造 `AddSnapshot` 载荷时（`AppendFiles.apply()`）会写 data manifest 与 manifest list 文件，这发生在数据提交方（Spark/Flink）一侧，与 Gravitino 无关；Gravitino 侧真正需要落盘的是新版本的元数据 JSON（§5.5）。实施前已对 Iceberg 1.11.0 逐项实证（见 §5.12）。

### 4.5 与 Apache Polaris 的对照

Apache Polaris 是同类问题最成熟的参考实现 —— 它同样要把"Iceberg 元数据文件 + 自己的元数据存储"绑在一起。对照它有两点价值：**它验证了什么**，以及**它为什么和我们做了不同的选择**。

#### 4.5.1 存储模型：Polaris 一行，我们两行

| | Apache Polaris | 本设计（Gravitino managed） |
| --- | --- | --- |
| Iceberg 指针存哪 | Polaris entity 的 `internalProperties["metadata-location"]`（`IcebergTableLikeEntity.java:45`、`getMetadataLocation():105`） | `iceberg_tables.metadata_location`（独立表） |
| 治理元数据存哪 | **同一个 entity**（properties / baseLocation / 实体关系） | `table_meta` + `table_version_info` + `table_column_version_info` |
| 原子性来源 | **天然原子** —— 一次 entity update 同时改指针与治理元数据 | **同库同事务** —— 两条 SQL 一次 commit |

Polaris 的 metastore 就是它的 Iceberg catalog store，没有第二个库。**它不需要事务方案，因为它只有一行要写。**

这也预先回答了最容易被提出的质疑："Polaris 都没做事务，Gravitino 为什么要做？" —— 因为 Gravitino 选择保留 `iceberg_tables` 作为 Iceberg 兼容层（§2 目标 5），指针与治理元数据分属两个 store，必须靠事务把它们绑起来。

#### 4.5.2 SDK 调用方式：Polaris 用 `TableOperations` 钩子，本设计用纯调用

**这是本设计与 Polaris 最大的实现差异**，需要说明清楚。

Polaris 的形态是**回调**：

```
LocalIcebergCatalog extends BaseMetastoreViewCatalog         (:163)
  └─ newTableOps(...) → BasePolarisTableOperations           (:1714)
        implements TableOperations                           (:1715)
        ├─ doRefresh() → 读 entity 的 metadata-location       (:1806)
        └─ doCommit()  → writeNewMetadataIfRequired + CAS 持久化 (:1902、:1966)

REST 层 → Iceberg SDK 的 CatalogHandlers → ops.commit() → doCommit()
```

`IcebergCatalogHandler` 的类注释写得很直白（`:145`）：

> "Authorization-aware adapter between REST stubs and **shared Iceberg SDK CatalogHandlers**."

Polaris 为什么能做得这么薄？因为它的 `doCommit` 里除了写文件，还要做 Polaris 特有的、**必须发生在"拿到新 metadata 且即将写盘"这一刻**的事：

* 存储位置校验与防重叠（`validateLocationsForTableLike`、`validateNoLocationsOverlap`、`validateMetadataFileInTableDir`）；
* 按请求凭据选择 `FileIO`（`loadFileIOForTableLike`，带 `PolarisStorageActions.READ/WRITE/LIST`）。

这使 `TableOperations` 成为一种自然选择：它是唯一同时握有"新 metadata"和"写盘时机"的扩展点。

本设计不走这条路，因为我们**不需要**在那个点上做任何事 —— 位置与凭据校验在 Gravitino 侧另有位置，而持久化必须跨两个 store。纯调用换来的是：事务边界由 Gravitino 显式掌控，`iceberg_tables` 与 `table_meta` 可以被放进同一次 `commit()`。

**权衡记录。** 回调路线并非错误 —— Polaris 在生产规模上验证了它可行，且它复用了 Iceberg 的 refresh 生命周期，代码量可能更少（无需只读载体，也无需改写 `buildIcebergTableChanges`，见 §4.2 的评估）。本设计选择纯调用，是一个**基于"两个 store 必须显式同事务"的取舍**，而不是否定回调路线。如果后续认为代码量更重要，§4.2 的路线仍然开放。

#### 4.5.3 Polaris 独立验证了 §4.4

Polaris 的多表事务路径**没有**走 `CatalogHandlers`，而是自己手写（`IcebergCatalogHandler.java:1385-1440`）：

```java
TableOperations tableOps = baseTable.operations();                            // :1403
TableMetadata baseMetadata = tableOps.current();
TableMetadata currentMetadata = baseMetadata;
for (UpdateTableRequest change : changes) {
    change.requirements().forEach(r -> r.validate(metadataForValidation));    // 纯
    TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(currentMetadata);
    for (MetadataUpdate singleUpdate : change.updates()) {
        // 应用到 builder                                                      // 纯
    }
    ...
}
```

注释说明了动机（`:1387-1391`）：

> All UpdateTableRequests for a given table are coalesced into a single metadata update and a single `tableOps.commit()`, which results in **one Polaris entity update per table**.

这是独立的、生产级的佐证：`requirement.validate` + `MetadataUpdate.applyTo` 确实是纯的，Polaris 在需要"把多次请求合并成一次持久化"时就是这么做的。§4.4 的结论成立。

同时这也是一个**警告**：Polaris 因此有了两套更新变换逻辑（单表走 `CatalogHandlers`、多表手工实现），它自己留了 TODO 要合并（`:1427-1430`）。本设计的"一个实现层 + 两个入口适配器"必须刻意避免同样的分叉（见 §8 测试 17）。

#### 4.5.4 可以借鉴的三点

| 点 | Polaris 做法 | 对本设计的意义 |
| --- | --- | --- |
| **metadata 文件命名规则** | Polaris 把 `METADATA_FOLDER_NAME`、`parseVersion`（`:2566+`）、`newTableMetadataFilePath`（`:2162`）当作"从 Iceberg 源码抄来的代码"，因此放进了自己的 LICENSE 归因流程 | 这几条规则在 **Iceberg 表格式规范**里就明文规定（`format/spec.md:1334`），属"实现规范"而非"抄源码"，见 §5.5。因此本设计**不引入** Polaris 的 `CODE_COPIED_*` 标记，也不需要在 LICENSE 中登记 |
| **失败后清理新写的 metadata 文件** | `record MetadataWriteResult(String location, boolean written)`（`:170`）记录是否真的写了新文件；`finally` 里 `cleanupWrittenMetadataFiles(...)`（`:2053-2058`） | **采纳**：§5.7 从"接受孤儿文件"改为带 `written` 标志的 best-effort 兜底删除 |
| **两层 CAS** | 应用层 `existingLocation == oldLocation` 比对（`:2018`）+ 持久层 `updateEntityPropertiesIfNotChanged` 的 entity version CAS（`AtomicOperationMetaStoreManager.java:921`） | 与本设计"前置 SELECT 比对 + 条件 UPDATE"结构一致，增强信心 |

写文件与持久化的顺序两边也一致：Polaris 先 `writeNewMetadataIfRequired`（`:1966`）再 `createTableLike` / `updateTableLike`。

---

## 5. 方案设计

### 5.1 总体结构：两个入口，一个实现层

```
   Iceberg REST API（优先）                Gravitino native API（次优先）
   POST /v1/namespaces/{ns}/tables         POST /api/metalakes/.../tables
   POST /v1/namespaces/{ns}/tables/{t}     PUT  .../tables/{t}
   （commit：requirements + updates）       （TableChange[]）
   ...                                     ...
        │                                          │
        ▼                                          ▼
   IcebergTableOperationExecutor            TableOperationDispatcher
   （协议适配器）                            （协议适配器）
        │                                          │
        └──────────────────┬───────────────────────┘
                           ▼
        ┌──────────────────────────────────────────────┐
        │  IcebergManagedCatalogOperations             │
        │  写路径原语：create / alter(commit) / drop /   │
        │             rename / register                │
        │                                              │
        │  · SDK 纯函数：构建 / 演进 TableMetadata       │
        │  · SDK：TableMetadataParser.overwrite         │
        │  · Gravitino SQL：iceberg_tables              │
        │  · Gravitino 事务：+ table_meta / schema_meta │
        └──────────────────────┬───────────────────────┘
                               ▼
                 ManagedIcebergMetadataStore
            (所有 SQL + 元数据文件辅助方法，跑在
             Gravitino 的 entity store 连接上)
```

两个入口都收敛到同一个实现层，managed 模式下 SDK 永不打开 JDBC 连接，它唯一的 I/O 是 warehouse 上的元数据 JSON 文件。

#### 5.1.1 两个入口的职责边界

**（1）元数据写入与授权开关解耦。**

现状：REST 路径的实体写入挂在 `IcebergTableHookDispatcher` 等 Hook 层，而 Hook 层只在授权开启时装配（`RESTService.java:156-179`）。因此默认配置（`gravitino.authorization.enable = false`）下，通过 Iceberg REST 建的表**不会**写入 `table_meta`。

managed 模式取消这个耦合：无论授权开关如何，Gravitino 元数据一律写入。理由：

* `table_meta` / `schema_meta` / `view_meta` 不只是鉴权依据，也是 tag / policy / statistics / owner / lineage 的挂载点；
* "翻一个开关就改变数据模型"在运维上不可接受；
* managed 模式的语义本身就是"Gravitino 拥有元数据"。

实现上，写路径从 Hook 层移入 §5.1 的实现层，这个 gate 自然消失。

**（2）仅支持同进程（auxiliary）部署。**

`auxMode` 由 `RESTService.serviceInit(properties, auxMode)` 决定（`RESTService.java:228-229`）。standalone 部署（`distribution/gravitino-iceberg-rest-server`）没有 entity store，`GravitinoEnv` 不可用。

managed 模式在 **catalog 创建时**校验 `auxMode`，不满足直接失败。这里刻意不做"静默降级"：现有代码对 `store == null` 有容错，会静默什么都不做，那会得到一个"看起来建好了、实际完全不工作"的 catalog。

**（3）共享什么，不共享什么。**

| 层 | 是否共享 |
| --- | --- |
| 持久化层（SQL、元数据文件读写、版本号解析、DDL） | **完全共享** |
| 写路径原语（create / alter / drop / rename / register） | **完全共享** |
| 事务边界、乐观并发、失败方向、`table_meta` 同步规则 | **完全共享** |
| 请求协议（`TableChange[]` vs `MetadataUpdate[]`） | 不共享，各自翻译 |
| 入口特有能力（credential vending、scan planning、分页、`stageCreate`） | 不共享 |
| 请求上下文（`IcebergRequestContext` vs `PrincipalUtils`） | 不共享 |

目标不是"入口不同、实现相同"，而是"**入口只做协议翻译和入口特有的事，所有涉及元数据读写的逻辑只有一份**"。

**（4）优先级。** 第一阶段交付 Iceberg REST 入口（§5.9.1）；Gravitino native 入口（§5.9.2）复用同一实现层，成本低，放最后阶段（§9 第五阶段）。

### 5.2 配置

新增一个不可变的 catalog 属性用于选择模式。

| 属性 | 取值 | 默认值 | 不可变 | 说明 |
| --- | --- | --- | --- | --- |
| `catalog-backend-mode` | `federated`、`managed` | `federated` | 是 | 仅在 `catalog-backend = jdbc` 时合法 |

managed 模式下现有 JDBC 属性的行为：

| 属性 | `federated` | `managed` |
| --- | --- | --- |
| `catalog-backend` | 必填，`jdbc` | 必填，`jdbc` |
| `uri` | 必填 | **不使用、不要求** |
| `jdbc-user`、`jdbc-password`、`jdbc-driver` | 必填 | **不使用** |
| `jdbc-initialize`、`jdbc-schema-version` | 可选 | **不使用**（Gravitino 固定 V1） |
| `warehouse` | 必填 | 必填（元数据文件位置） |
| `catalog-backend-name` | 可选 | **必填**（即 `catalog_name` 列） |

`IcebergCatalogPropertiesMetadata` 需要两处改动：

* 注册 `catalog-backend-mode` 为一个不可变的枚举属性；
* 把 `uri` 改成**条件必填** —— `federated` 时必填，`managed` 时忽略/拒绝。校验发生在 catalog 创建阶段，而不是静态的 `PropertyEntry` 表里。

开关由 `IcebergCatalog` 读取：一处用于 capability，一处由 `IcebergCatalog.newOps(config)` 选择 operations 实现。由于 `BaseCatalog.newCapability()` 是无参的，`IcebergCatalog` 需要覆写 `newCapability()`，从 `entity()` 读取 catalog 属性（`BaseCatalog.entity()`，`BaseCatalog.java:415`），它在任何操作执行前就已填充。

```
IcebergCatalog.newCapability()
    mode = entity().getProperties().get(CATALOG_BACKEND_MODE)
    return new IcebergCatalogCapability(schemaSeparator, mode)
        .managedStorage(TABLE | SCHEMA) = SUPPORTED   // 当 mode == managed
```

让 capability 返回 `SUPPORTED`，就是通知 `TableOperationDispatcher`（`isManagedEntity` → `catalog.capabilities().managedStorage(scope).supported()`）跳过它自己在 `TableOperationDispatcher.java:660-667` 的 `store.put`，把 entity 写入交给 catalog。这与 `catalog-lakehouse-generic` 为 Delta 和 Lance 使用的是同一个机制。

**前置条件（catalog 创建时校验，不满足直接失败）：**

| # | 条件 | 不满足时 |
| --- | --- | --- |
| 1 | `catalog-backend = jdbc` | 拒绝：`hive` / `rest` / `memory` / `custom` 是外部后端，无法同库同事务 |
| 2 | Iceberg REST 服务处于 auxiliary 模式（与 entity store 同进程） | 拒绝：standalone 部署没有 entity store，managed 模式无法工作（见 §5.1.1） |
| 3 | `iceberg_tables` / `iceberg_namespace_properties` 可访问（或允许 Gravitino 建表） | 拒绝：给出可执行的错误信息（见 §7 R3） |

这三条都刻意做成 fail fast，而不是运行期静默降级。

### 5.3 组件职责

| 组件 | 职责 |
| --- | --- |
| `IcebergCatalog` | 依据 `catalog-backend-mode` 选择 operations 实现与 capability |
| `IcebergCatalogCapability` | managed 模式下返回 `managedStorage(TABLE/SCHEMA) = SUPPORTED` |
| `IcebergManagedCatalogOperations` | 实现层：§5.8 的五个写路径原语 + 读路径；事务、SDK 调用、SQL、entity 写入 |
| `ManagedIcebergMetadataStore` | 所有针对 `iceberg_tables` / `iceberg_namespace_properties` 的 SQL；元数据文件辅助方法；版本号解析 |
| `ManagedIcebergCommitHandler` | Iceberg REST 入口的 commit 适配器：校验 requirement、应用 update、写文件、更新行、失败重试、按 §5.9.3 决定是否同步 `table_meta` |
| `ManagedIcebergCatalogOps`（`iceberg-common`） | REST 入口契约：用 Iceberg 类型表达的 create/load/update/register/list/exists/drop/rename/metadataLocation；由 `IcebergManagedCatalogOperations` 实现 |
| `IcebergTableOperationExecutor`（改造） | Iceberg REST 入口适配器：把 REST 请求翻译成 §5.8 原语（经 `ManagedIcebergCatalogOps`） |
| `TableOperationDispatcher`（改造） | Gravitino native 入口适配器（第五阶段） |
| `IcebergTableHookDispatcher` 等 Hook 层 | managed 模式下不再承担实体写入（写入移入实现层）；保留授权相关职责。实现方式：`IcebergCatalogWrapperManager.isManagedCatalog(name)` 作为唯一判定入口，Hook 在 managed 时跳过 `importTableEntity` / drop 后 reconcile / rename 的实体搬迁，但**保留** `setTableOwner` 与 `IcebergOrphanSchemaCleanup`（授权与 schema 生命周期与写入归属无关） |
| `IcebergCatalogWrapper` | 契约不变；managed 模式下读写都不再走 Iceberg 的 `JdbcCatalog` 与 `CatalogHandlers` |
| `ManagedCatalogWrapperForREST`（`iceberg-rest-server`） | `CatalogWrapperForREST` 的 managed 子类：所有表操作委托给 `ManagedIcebergCatalogOps`，永不初始化 Iceberg SDK catalog |

**managed wrapper 的取得方式与 `uri` 的处理（实施阶段补充）。** `IcebergCatalogWrapperManager.createCatalogWrapper` 在 `catalog-backend-mode=managed` 时返回 `ManagedCatalogWrapperForREST`，它通过 `CatalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalogName)).catalog().ops()` 拿到实现层实例（`CatalogManager` 已缓存 catalog，`ops()` 是懒加载），metalake 取自 `IcebergRESTServerContext`。两个实现细节：

* `IcebergCatalogWrapper` 的构造函数原本对非 memory 后端强制要求 `uri`（`IcebergConfig.CATALOG_URI` 带 `checkValue(isNotBlank)`）。managed 模式不需要 `uri`（§5.8 的 `uri` 是可选的），因此构造函数在 managed 模式下跳过该读取；`catalogUri` 只用于 JDBC 驱动清理，为 null 是安全的。
* managed wrapper 覆盖 `getCatalog()` 直接抛错：managed 模式下任何"顺手拿 SDK catalog"的代码都是 bug，应当立即暴露而不是悄悄连库。

**为什么契约放在 `iceberg-common`。** 实现层在 `catalog-lakehouse-iceberg`，REST 入口在 `iceberg-rest-server`，后者**不依赖**前者（catalogs 是插件，反向依赖会破坏分发层布局）。契约因此放在两者共同依赖的 `iceberg-common`。这里有一个必须留意的细节：`IsolatedClassLoader.isSharedClass` 把 `org.apache.gravitino.catalog.*` 之外的一切都当作 **shared**，委派给 server 的 `baseClassLoader`，所以 `org.apache.gravitino.iceberg.common.ops.ManagedIcebergCatalogOps` 由 server 加载；而实现类落在 `org.apache.gravitino.catalog.lakehouse.iceberg.managed`（隔离加载）。因此跨隔离边界对 `catalog.ops()` 做 `instanceof ManagedIcebergCatalogOps` 是安全的，**但绝不能把契约放在 `org.apache.gravitino.catalog.*` 下**——那会被隔离加载，`instanceof` 永远为 false。

`IcebergManagedCatalogOperations` 继承 `ManagedTableOperations`（与 `DeltaTableOperations`、`LanceTableOperations` 的做法一致），从而复用 `TableEntity` 构建、column entity 和 audit info 的既有代码。它通过 `GravitinoEnv` 提供 `store()`、`schemas()` 和 `idGenerator()`。

### 5.4 使用的 SDK API 清单（全部为 public）

已针对 `apache-iceberg-1.11.0` 逐项核实。以下没有任何 `protected`、包级私有或反射调用。

| 用途 | API | 说明 |
| --- | --- | --- |
| 构建新表元数据 | `TableMetadata.newTableMetadata(Schema, PartitionSpec, SortOrder, String location, Map)` | 纯函数 |
| 由 `CreateTableRequest` 构建 | `CatalogHandlers.stageTableCreate(Catalog, Namespace, CreateTableRequest)` | 纯函数；`IcebergCatalogWrapper` 的 `stageCreate` 分支已在用 |
| 读元数据 | `TableMetadataParser.read(FileIO, String location)` | 仅文件 I/O |
| 写元数据 | `TableMetadataParser.overwrite(TableMetadata, OutputFile)` | 仅文件 I/O；用 `overwrite` 而非 `write`（规避 S3 negative caching） |
| 文件扩展名 | `TableMetadataParser.getFileExtension(String codecName)` | 纯函数 |
| 应用 schema 变更 | `base.updateSchema(Schema)` | 纯函数；字节码只有 `new Builder(base)` + `Math.max` + `build()` |
| 应用分区变更 | `base.updatePartitionSpec(PartitionSpec)` | 纯函数 |
| 应用排序变更 | `base.replaceSortOrder(SortOrder)` | 纯函数 |
| 应用属性变更 | `base.replaceProperties(Map)` | 纯函数 |
| 精细构建 | `TableMetadata.buildFrom(base)` → `Builder`（`setCurrentSchema`、`setProperties`、`assignUUID`、`upgradeFormatVersion`、`build`） | 纯函数 |
| 获取 pending update | `new BaseTable(ops, name).updateSchema()/updateSpec()/updateProperties()/replaceSortOrder()` | 公开构造函数 `BaseTable(TableOperations, String)` |
| 计算未提交变更 | `PendingUpdate.apply()` | 官方文档明确说明"does not result in a permanent update" |
| 路径组件 | `TableProperties.METADATA_COMPRESSION`、`METADATA_COMPRESSION_DEFAULT`、`WRITE_METADATA_LOCATION`、`LocationUtil.stripTrailingSlash(String)` | 纯函数 |
| 清理 | `CatalogUtil.deleteRemovedMetadataFiles(FileIO, TableMetadata, TableMetadata)`、`CatalogUtil.dropTableData(FileIO, TableMetadata)` | public static |

#### 5.4.1 为什么用 `PendingUpdate.apply()` 而不是直接构造 update 类

`SchemaUpdate`、`PropertiesUpdate`、`BaseUpdatePartitionSpec`、`BaseReplaceSortOrder` 的构造函数都是 **包级私有**（`SchemaUpdate(Schema, int)` 等），无法从 `org.apache.gravitino.*` 实例化。官方支持的获取方式是通过 `Table`：

```java
Table table = new BaseTable(readOnlyOps(baseMetadata), tableName);
Schema newSchema = table.updateSchema().addColumn("x", Types.IntegerType.get()).apply();  // 纯函数
TableMetadata updated = baseMetadata.updateSchema(newSchema);                             // 纯函数
```

`PendingUpdate.apply()` 被官方文档明确定义为产出未提交变更；`commit()` 才是持久化调用，本路径上永不使用。

这需要一个极简的只读 `TableOperations` 载体（仅用于 alterTable 路径，见 §5.8.2）。它是**载体**而非**钩子**：`commit()` 抛 `UnsupportedOperationException`，且本路径上 Gravitino 从不让 Iceberg 调用它。REST 提交路径不需要它，因为 `MetadataUpdate.applyTo(TableMetadata.Builder)` 可以直接构造，见 §4.4。

### 5.5 元数据文件路径与版本号

命名规则由 **Iceberg 表格式规范**定义，不是 Iceberg 的实现细节：

> Each version of table metadata is stored in a metadata folder under the table's base location using a naming scheme that includes a version and UUID: `<V>-<random-uuid>.metadata.json`.
> —— Iceberg table spec，`format/spec.md`（v1 用 `v<V>.metadata.json`，见同文件另一节）

Gravitino 按规范实现，并返回 `MetadataWriteResult(location, written)`（`written` 用于 §5.7 的失败兜底清理）：

```java
/** location：新元数据文件的位置；written：本次是否真的写了新文件。 */
record MetadataWriteResult(String location, boolean written) {}

private MetadataWriteResult writeMetadata(TableMetadata metadata, int version) {
  String codecName = metadata.property(
      TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
  String filename = String.format(Locale.ROOT, "%05d-%s%s",
      version, UUID.randomUUID(), TableMetadataParser.getFileExtension(codecName));

  String baseDir = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);
  String path = (baseDir != null)
      ? String.format("%s/%s", LocationUtil.stripTrailingSlash(baseDir), filename)
      : String.format("%s/metadata/%s", metadata.location(), filename);

  OutputFile out = io.newOutputFile(path);
  TableMetadataParser.overwrite(metadata, out);
  return new MetadataWriteResult(out.location(), true /* written */);
}
```

当元数据已经带有 location 时（register），返回 `new MetadataWriteResult(metadata.metadataFileLocation(), false /* written */)`，不写文件。

**为什么必须沿用规范，而不是自由命名：**

* 表元数据 JSON **不包含自己的位置** —— `TableMetadataParser` 只在 `metadata-log` 数组里输出 `metadata-file`。`iceberg_tables.metadata_location` 才是唯一权威指针，所以命名规则在技术上确实是自由的；
* 但下面的 `parseVersion` 会从文件名 `%05d-` 前缀解析元数据版本号，缺失时返回 `-1`。沿用规范可以保持版本推导与 Iceberg 工具链（`removeOrphanFiles`、time travel）正常工作。

**实现规范 ≠ 抄写源码。** 上面这段是"实现公开的格式规范"，与 Gravitino 实现 Iceberg REST 规范同一性质，因此**不涉及"抄自 Iceberg 源码"的归因流程**（不引入 `CODE_COPIED_*` 标记，也不在 LICENSE 中登记）。只有当确实抄写了非平凡的 Iceberg 实现代码时才需要 —— 例如 `BaseMetastoreTableOperations.refreshFromMetadataLocation` 那段带指数退避的重试逻辑，但本设计不需要它（直接用 public 的 `TableMetadataParser.read`）。若将来出现这种情况，按 Gravitino 既有约定处理：`// Referred from Apache Iceberg's ... implementation` 加原始路径，并在 `LICENSE` 的 `Apache Iceberg` 段落登记。

升级 Iceberg 时只需对照规范重核，不必对照 Iceberg 源码（对应 R2、R9）。

版本号解析同样只有四行：

```java
private static int parseVersion(String metadataLocation) {
  int versionStart = metadataLocation.lastIndexOf('/') + 1;
  int versionEnd = metadataLocation.indexOf('-', versionStart);
  if (versionEnd < 0) {
    return -1;
  }
  return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
}
```

### 5.6 SQL 层

managed 模式把 `iceberg_tables` 的 schema 固定为 V1，因此只需要 V1 语句。所有语句都通过 Gravitino 的 entity store 连接执行。

```sql
-- refresh / load
SELECT metadata_location, previous_metadata_location
  FROM iceberg_tables
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- list
SELECT table_name FROM iceberg_tables
 WHERE catalog_name = ? AND table_namespace = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- exists
SELECT 1 FROM iceberg_tables
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- create
INSERT INTO iceberg_tables
       (catalog_name, table_namespace, table_name,
        metadata_location, previous_metadata_location, iceberg_type)
VALUES (?, ?, ?, ?, null, 'TABLE')

-- commit update（乐观并发）
UPDATE iceberg_tables
   SET metadata_location = ?, previous_metadata_location = ?
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND metadata_location = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- rename
UPDATE iceberg_tables
   SET table_namespace = ?, table_name = ?
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)

-- drop
DELETE FROM iceberg_tables
 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?
   AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)
```

必须保住两条不变量：

1. **`table_namespace` 是命名空间各层用点号 join 的结果**（`JdbcUtil.namespaceToString` 使用 `Joiner.on('.')`）。这必须与 Iceberg 客户端和 Gravitino REST 服务的期望一致。
2. **`UPDATE ... WHERE metadata_location = ?` 返回 0 行时必须抛 `CommitFailedException`。** 这是 Iceberg 唯一的并发写者安全机制，不能弱化。Iceberg 另外还做了一次前置校验（`JdbcTableOperations.validateMetadataLocation`）：先 SELECT 当前行，与 `base.metadataFileLocation()` 比较，不一致就在尝试 UPDATE 之前失败。两层都要复现。

`iceberg_tables`（以及 `iceberg_namespace_properties`）本身的 DDL 在 managed 模式下由 Gravitino 负责。由于固定为 V1 schema，DDL 为：

```sql
CREATE TABLE iceberg_tables (
  catalog_name VARCHAR(255) NOT NULL,
  table_namespace VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  metadata_location VARCHAR(1000),
  previous_metadata_location VARCHAR(1000),
  iceberg_type VARCHAR(5),
  PRIMARY KEY (catalog_name, table_namespace, table_name))
```

**DDL 通过 init 脚本交付**，放在 `distribution/package/scripts/<db>/` 下，与现有 entity store 脚本一致。catalog 创建时做一次存在性校验并 fail fast（§5.2 前置条件 3），不在运行期自动建表。

### 5.7 事务边界

```java
// 1. 先写元数据文件 —— 对象存储 I/O 不应长时间占用数据库事务
//    writeMetadata 返回 MetadataWriteResult(location, written)：
//    written 表示本次是否真的新写了文件（register / 已有 location 时为 false）
TableMetadata updated = ...;                       // SDK：纯构建 / 演进
MetadataWriteResult writeResult = writeMetadata(updated, version);

boolean persisted = false;
try {
  // 2. 一个事务覆盖两行
  SessionUtils.beginTransaction();                   // 将 SqlSession + Connection 绑定到当前线程
  metadataStore.insertOrUpdateIcebergTableRow(...);   // Gravitino SQL，直接跑在绑定连接上
  entityStore.put(tableEntity, /* overwrite */ true); // table_meta，嵌套 session，同一条连接
  // 必须显式提交 JDBC 事务，不能只用 SessionUtils.commitTransaction()，理由见下
  SessionUtils.commitTransactionAndConnection();
  persisted = true;
} catch (Throwable t) {
  SessionUtils.rollbackTransactionAndConnection();
  throw t;
} finally {
  // 3. 持久化失败时兜底删除刚写的文件（采纳 Polaris 的做法，见 §4.5.4）
  //    仅在 written == true 时删除，避免误删复用的旧文件；删除本身也是 best-effort
  if (!persisted && writeResult.written()) {
    deleteMetadataFileQuietly(writeResult.location());
  }
}
```

`MetadataWriteResult` 记录 `written` 标志是必要的：`registerTable`、以及"元数据已带 location"的短路分支都不会写新文件，此时**不能**删除 —— 那个文件是别人已经在用的。

嵌套之所以可行，是因为 `SqlSessions` 是 thread-local 且**引用计数**的：`handleSessionClose` 只在计数归零时才 commit（`SqlSessions.java:106-138`）。因此 `TableMetaService.insertTable` 内部嵌套的 `SessionUtils.doMultipleWithCommit` 会把提交推迟到外层边界，一次 `commit()` 覆盖 `iceberg_tables`、`table_meta`、`table_version` 和 `table_column_version_info`。

**必须显式提交/回滚 JDBC 事务（实施阶段实测发现的硬约束）。** 仅用 `SessionUtils.commitTransaction()` / `rollbackTransaction()` 覆盖"原生 JDBC + 嵌套 MyBatis 写入"的混合事务是**错的**，原因是 MyBatis 的两条实现细节：

1. `DefaultSqlSession.rollback()` 只对**脏** session 才真正调用 `Transaction.rollback()`；直接跑在绑定连接上的 `PreparedStatement` 不会被 MyBatis 记为 dirty，于是回滚被静默跳过；
2. `JdbcTransaction.close()` 会把连接的 autoCommit 复位为 `true`，而多数 JDBC 驱动（含 H2/MySQL）在 `setAutoCommit(true)` 时会**隐式提交**当前未提交事务 —— 也就是把那条未被追踪的原生 SQL 提交掉。

净效果：`beginTransaction()` → 原生 SQL →（实体写入失败）→ `rollbackTransaction()` 之后，`iceberg_tables` 行会**残留**。因此 core 增加了 `SessionUtils.commitTransactionAndConnection()` / `rollbackTransactionAndConnection()`：先在绑定连接上显式 `commit()`/`rollback()`，再走原有的引用计数收尾。任何"原生 SQL + 实体写入"混用的事务（§5.8、§5.9.1、§5.11）都必须用它，并有回归测试锁定（`TestIcebergManagedCatalogOperations#testRawSqlOnTheSessionConnectionNeedsAnExplicitResolution`）。

由此带来的后果：

* **正常情况下不再有孤儿 metadata 文件**：回滚后由上面的 `finally` 兜底删除。只有"删除本身也失败"（对象存储不可用、权限不足）时才会残留，属于低概率情形，仍由 §7 R7 的维护任务兜底。反向顺序（行已提交、文件缺失）永不产生。
* **事务内不能有 DDL。** MySQL 的 DDL 会隐式提交。`iceberg_tables` 的建表与任何 `ALTER` 必须发生在 catalog 初始化阶段，位于任何事务之外。
* 事务只为需要写 `table_meta` 的操作开启。纯数据写入提交（§5.9.3 判定为"不影响实体内容"）不需要 Gravitino 事务，因为只有一条 `iceberg_tables` 的 UPDATE。

### 5.8 写路径原语（与入口无关）

本节描述五个写路径原语。两个入口（§5.9）都翻译成这些原语，共享同一份实现。

#### 5.8.1 createTable

```
IcebergManagedCatalogOperations.createTable(ident, columns, comment, properties, partitions, ...)
  ├─ 校验 schema 存在（entity store）
  ├─ md = TableMetadata.newTableMetadata(schema, spec, sortOrder, location, properties)   // SDK，纯函数
  ├─ location = writeMetadata(md, 0)   // 与 Iceberg 一致：首个元数据文件是版本 0         // SDK，文件 I/O
  ├─ SessionUtils.beginTransaction()
  │    ├─ INSERT INTO iceberg_tables (..., metadata_location = location, ..., 'TABLE')     // 0 行 → CommitFailedException
  │    └─ super.createTable(...)  → store.put(TableEntity)                                 // 嵌套 session
  └─ SessionUtils.commitTransactionAndConnection()   // 见 §5.7
```

Gravitino native 入口下，由于 capability 声明了 managed storage，`TableOperationDispatcher` 不会再写 entity。

#### 5.8.2 alterTable / commit

"修改表"在两个入口的形态不同，但收敛到同一个原语：**读 base → 计算新 `TableMetadata` → 写文件 → 乐观并发更新 `iceberg_tables` →（必要时）同步 `table_meta`**。

Gravitino native 入口传入的是 `TableChange[]`。映射逻辑是 Gravitino 现有的 `IcebergCatalogWrapperHelper.buildIcebergTableChanges`（位于 `catalog-lakehouse-iceberg/.../ops/`，366 行），它当前把变更应用到一个 Iceberg `Transaction` 里的 `BaseTable` 上。managed 模式下，同样的变更序列被应用到构建在只读载体之上的 `BaseTable`，并以 `apply()` 而不是 `commit()` 收尾：

```
  ├─ base = TableMetadataParser.read(io, currentMetadataLocation)                         // SDK，文件 I/O
  ├─ table = new BaseTable(readOnlyOps(base), ident.name())
  ├─ 对每个 TableChange：table.updateSchema()/updateProperties()/... （累积）
  ├─ updated = base.updateSchema(applied) | base.replaceProperties(applied) | ...          // SDK，纯函数
  ├─ newLocation = writeMetadata(updated, parseVersion(currentMetadataLocation) + 1)       // SDK，文件 I/O
  ├─ SessionUtils.beginTransaction()
  │    ├─ SELECT 当前行；与 base.metadataFileLocation() 比较                                 // 过期校验
  │    ├─ UPDATE iceberg_tables SET metadata_location = newLocation,
  │    │         previous_metadata_location = currentLocation
  │    │   WHERE ... AND metadata_location = currentLocation                              // 0 行 → CommitFailedException
  │    └─ store.update(TableEntity)                                                       // 嵌套 session
  └─ SessionUtils.commitTransactionAndConnection()   // 见 §5.7
```

Iceberg REST 入口传入的是 `UpdateTableRequest`（requirements + updates），走 §5.9.1 的翻译，落到同一个原语。两者的差异见 §5.9.3。

#### 5.8.3 dropTable / purgeTable

```
  ├─ row = SELECT metadata_location ...                                                   // 可能不存在
  ├─ if purge: base = TableMetadataParser.read(io, row.metadata_location)                 // SDK
  │             CatalogUtil.dropTableData(io, base)                                       // SDK
  ├─ SessionUtils.beginTransaction()
  │    ├─ DELETE FROM iceberg_tables WHERE ...                                            // 0 行 → 返回 false
  │    └─ store.delete(ident, TABLE)                                                      // 嵌套 session
  └─ SessionUtils.commitTransactionAndConnection()   // 见 §5.7
```

#### 5.8.4 renameTable

先校验目标命名空间存在、目标表不存在，然后一条 `UPDATE iceberg_tables SET table_namespace = ?, table_name = ?`，再 `store.update` 更新 entity，全部在一个事务内。`updatedRecords == 0` 表示源表已消失 → `NoSuchTableException`。

#### 5.8.5 registerTable

`registerTable(ident, metadataLocation)` 用 `TableMetadataParser.read` 读取并校验元数据文件（Iceberg 在读取时会校验格式与版本），然后在一个事务里做一次普通 INSERT 加 entity 写入。不写元数据文件 —— 复用已存在的文件。

### 5.9 入口适配器

#### 5.9.1 Iceberg REST 入口（主要交付目标）

**create / register / drop / rename**：`IcebergTableOperationExecutor` 把 REST 请求翻译成 §5.8 的原语。与现状的关键差别是**去掉 Hook 层的授权 gate** —— 实体写入移入 §5.1 的实现层，因此无论 `gravitino.authorization.enable` 取值如何都会执行。

**commit**：从 `IcebergTableOperations.updateTable` 进来，携带一个 `UpdateTableRequest`（`requirements` + `updates`）。managed 模式下 Gravitino 不再调用 `CatalogHandlers.updateTable`，而是复刻它的纯逻辑（§4.4）：

```
ManagedIcebergCommitHandler.commit(ident, UpdateTableRequest request)
  ├─ 重试循环（最多 N 次，指数退避，仅在 CommitFailedException 时重试）
  │    ├─ base = metadataStore.readMetadata(ident)                    // SDK: TableMetadataParser.read
  │    ├─ request.requirements().forEach(r -> r.validate(base))        // SDK: 纯函数
  │    ├─ builder = TableMetadata.buildFrom(base)                      // SDK: 纯函数
  │    ├─ request.updates().forEach(u -> u.applyTo(builder))           // SDK: 纯函数
  │    ├─ updated = builder.build()                                    // SDK: 纯函数
  │    ├─ if (updated.changes().isEmpty()) return;                     // 无变化不提交
  │    ├─ newLocation = metadataStore.writeMetadata(updated, version+1) // SDK: 写文件
  │    └─ if (touchesEntityContent(request.updates()))                 // 见 §5.9.3
  │         SessionUtils.beginTransaction()                            //   结构与属性变更
  │           ├─ UPDATE iceberg_tables ...                             //   0 行 → CommitFailedException
  │           └─ store.update(TableEntity)                             //   嵌套 session
  │         SessionUtils.commitTransactionAndConnection()  // 见 §5.7
  │       else
  │         UPDATE iceberg_tables ...                                  //   仅数据版本变更，单条 SQL
  └─ 返回更新后的 TableMetadata
```

两个分支要一并复刻：

* requirements 校验失败 → 包成 `ValidationFailureException` 直接返回客户端，**不重试**（照抄 `CatalogHandlers.commit` 的行为）；
* requirements 含 `AssertTableDoesNotExist` → 走 `CatalogHandlers.create` 的等价逻辑（即下面的 staged create）。

**staged create（`stageCreate = true`）语义保留。** Iceberg 的暂存创建只构造元数据、不落任何存储：`CatalogHandlers.stageTableCreate` 返回带 `TableMetadata` 的响应（供凭据下发与后续数据写入使用），**不写 `iceberg_tables`**。客户端随后通过 `POST /v1/namespaces/{ns}/tables/{t}` 提交，此时 requirements 含 `AssertTableDoesNotExist`，即走上一条 `isCreate` 分支。

因此 managed 模式下：

| 请求 | 写 `iceberg_tables` | 写 `table_meta` |
| --- | --- | --- |
| `POST /tables`（`stageCreate = false`） | 是（§5.8.1） | 是，同一事务 |
| `POST /tables`（`stageCreate = true`） | **否** | **否** |
| `POST /tables/{t}`（`isCreate`，即 staged create 的提交） | 是 | 是，同一事务 |

这与现有 Hook 层的行为一致（`IcebergTableHookDispatcher.java:71-75`：暂存创建跳过 import，提交时才登记）。区别在于现在两条记录在**同一事务**里落地，因此不再出现 §1.4 里"失败方向相反"的问题。

**重试循环必须与调用同线程。** `CatalogHandlers.commit` 的重试用 `Tasks.foreach(ops).retry(...).run(...)`；Iceberg 的 `TaskGroup.run(...)` 在未调用 `.executeWith(ExecutorService)` 时走 `runSingleThreaded`（`Tasks.java:199-206`），与调用同线程。

这一点对本设计是**硬约束**：Gravitino 的事务绑定在 `SqlSessions` 的 thread-local 上（§5.7）。一旦重试切到线程池，`beginTransaction()` 绑定的连接就会失效。因此：

* 自己实现重试循环时必须保持同步、同线程；
* 若复刻时沿用 `Tasks.foreach(...)`，**不得**添加 `.executeWith(...)`；
* 用测试锁定（§8 测试 18）。

**约束的准确理由（实施前实证后的修正）。** 上文原稿把后果写成"`iceberg_tables` 与 `table_meta` 会落到不同的连接与事务上"，这在**事务开在重试任务外部**时成立；但按本节伪代码，`beginTransaction()` 实际位于每次尝试**内部**，此时两个 store 都在重试线程自己的 session 上，并不会彼此分裂。线程池真正破坏的是三件事：

1. 调用方可能存在的**外层事务边界**（例如把多个操作合并进一次提交时）；
2. **跨 attempt 的事务连续性** —— 第 N 次与第 N+1 次尝试可能落在不同线程，上一轮的未提交写入会滞留在别的连接上；
3. 其它 thread-local 上下文（principal / 请求上下文、`TreeLock` 的持有线程）。

结论不变（不得 `.executeWith`、自研重试也必须同线程），但实现者应按上面三条来评估风险，而不是假设两个 store 一定会分裂。

**requirement 校验失败不得重试（实现细节）。** Iceberg 把校验失败包成包级私有的 `CatalogHandlers.ValidationFailureException`，正是为了让 `onlyRetryOn(CommitFailedException.class)` 不重试它。该类型无法从 Gravitino 复用，因此 `ManagedIcebergCommitHandler` 用等价的公开 `ValidationFailure`（内部持有原始 `CommitFailedException`）承载，语义不变：不重试、REST 层映射为冲突。另外 `isCreate` 分支（requirements 含 `AssertTableDoesNotExist`）按 `CatalogHandlers.create` 的语义实现：requirements 对**空 base** 校验，updates 应用到 `TableMetadata.buildFromEmpty(formatVersion)`，随后作为新表落库。

#### 5.9.2 Gravitino native 入口（第五阶段）

`TableOperationDispatcher` 把 Gravitino 的 `Column[]` / `TableChange[]` 翻译成 §5.8 的原语。由于 managed capability 让 dispatcher 跳过自己的 `store.put`（`TableOperationDispatcher.java:660-667`），实体写入由实现层完成。

翻译逻辑基本沿用现有的 `IcebergCatalogWrapperHelper.buildIcebergTableChanges`，只是终点从 `Transaction.commitTransaction()` 改为 `PendingUpdate.apply()` + `TableMetadata.updateXxx()`。

#### 5.9.3 两个入口的语义对齐：`table_meta` 的同步规则

这是统一之后必须显式定义的一条规则 —— 因为 **REST commit 也能改 schema 和属性**。

`MetadataUpdate` 的子类（`core/src/main/java/org/apache/iceberg/MetadataUpdate.java`）：

| 类别 | update 类型 | 影响 Gravitino 实体内容？ |
| --- | --- | --- |
| 结构（列） | `AddSchema`、`SetCurrentSchema`、`RemoveSchemas` | **是** → `table_meta.columns` |
| 结构（分区 / 排序） | `AddPartitionSpec`、`RemovePartitionSpecs`、`SetDefaultPartitionSpec`、`AddSortOrder`、`SetDefaultSortOrder` | **是** → `table_meta` 的 partitioning / sortOrders |
| 属性 / 位置 | `SetProperties`、`RemoveProperties`、`SetLocation` | **是** → `table_version_info.properties` |
| 格式 | `UpgradeFormatVersion`、`AssignUUID` | **是** → `table_version_info.format` |
| 数据版本 | `AddSnapshot`、`SetSnapshotRef`、`RemoveSnapshots`、`RemoveSnapshotRef` | 否 |
| 统计 | `SetStatistics`、`RemoveStatistics`、`SetPartitionStatistics`、`RemovePartitionStatistics` | 否 |
| 加密密钥 | `AddEncryptionKey`、`RemoveEncryptionKey` | 否 |
| 视图专属 | `AddViewVersion`、`SetCurrentViewVersion` | 否（本次范围不含视图，见 §3） |

上表是对 Iceberg 1.11.0 的 `MetadataUpdate` **全部 25 个子类**的显式归类（R10）；实现中用白名单落在 `ManagedIcebergCommitHandler`，并由 `TestMetadataUpdateClassification` 反射枚举全部子类，新增子类时测试失败。注意**分区与排序变更也属于"影响实体内容"**——原表把它们漏掉了。

Iceberg 客户端的 commit 就是这样产生的：`RESTTableOperations.commit` 用 `metadata.changes()` 作为 `updates`（`RESTTableOperations.java:161-180`，`TableMetadata.changes()` 在 `TableMetadata.java:586`）。**Spark 执行 `ALTER TABLE ... ADD COLUMN` 走的正是这个端点。**

因此规则是：

> REST commit 检查 `request.updates()`：
> * 含**结构（列 / 分区 / 排序）/ 属性 / 格式**类 update → 开启 Gravitino 事务，同时更新 `iceberg_tables` 与 `table_meta`（`IcebergManagedCatalogOperations.commit`）；
> * 只含 **snapshot / statistics / encryption / 视图** 类 update → 只做一条 `UPDATE iceberg_tables`，由 SQL 乐观并发保证正确性，不动 `table_meta`（`IcebergManagedCatalogOperations.commitDataOnly`）。

这样两个入口在 `table_meta` 上的语义完全一致：**凡是改变"结构 / 属性"的操作都同步；只改变"数据版本"的操作不同步。**

这实际上是把现有 Gravitino native 读路径的 `updateColumnsIfNecessaryWhenLoad`（`loadTable` 时发现列不一致就回写）前移到写入时刻，不再依赖读时修补。

### 5.10 读取路径

`loadTable`、`tableExists`、`listTables` 与命名空间查询执行 §5.6 的 SQL；`loadTable` 另外用 `TableMetadataParser.read` 读取文件，然后直接构造 `LoadTableResponse`（不经 `CatalogHandlers.loadTable`，后者需要一个 `TableOperations` 载体）。

读取不需要事务。唯一需要看到未提交状态的场景是某个写操作正在进行，此时连接已经绑定在事务上；两个入口（§5.9）共享同一份读实现。

Gravitino 的内存 `LocalTableMetadataCache` 必须在每次写入后失效、每次读取后填充，与今天 `IcebergCatalogWrapper` 的做法一致。

### 5.11 命名空间

managed 模式直接针对 `iceberg_namespace_properties` 实现 `SupportsNamespaces`（外加命名空间存在性检查 —— Iceberg 的 JDBC 后端是从 `iceberg_tables` 与 `iceberg_namespace_properties` 共同推导命名空间存在性的）。对应的 `SchemaEntity` 由 Gravitino 在同一事务内写入，因为 `managedStorage(SCHEMA)` 同样被声明为 `SUPPORTED`。

### 5.12 实施前置验证结论

本设计的三块地基在**写实现之前**用一次性证伪测试（falsification spike）实测过，结论如下（证据为本地记录，未进入仓库）：

| 假设 | 结论 | 关键证据 |
| --- | --- | --- |
| §5.7 一个事务覆盖 `iceberg_tables` + `table_meta`，嵌套实体写入不提前提交 | 成立 | 提交后两行都可见、回滚后两行都不存在、外层提交前外部不可见；负对照（独立连接）回滚后 iceberg 行残留 |
| §4.4 / §5.4 SDK 可当纯函数库用 | 成立 | 元数据构建/演进零提交、零输出文件；"校验 requirement + 应用 update + build"的复刻产物与 `CatalogHandlers.updateTable` 在 uuid / formatVersion / location / `lastColumnId` / schema·spec·sort id / properties / `currentSnapshotId` / refs / snapshot 数量上一致 |
| §5.9.1 / R13 重试同线程 | 成立 | `Tasks.foreach(...).run(...)` 3/3 次尝试在同一线程；线程池反例落到另一条连接且调用方提交不覆盖其写入 |
| §1.2 / §1.3 现状缺陷 | 复现 | `createTable` 返回成功、`iceberg_tables` 有行、`table_meta` 缺行；`loadTable` 自动 import 并抛 `RuntimeException("Fail to import the table entity to the store.")` |

由验证结果修正的两处表述：§4.4 的"纯调用"边界（不含 manifest 写入）、§5.9.1 线程池后果的精确描述。

---

## 6. 兼容性与迁移

* `catalog-backend-mode` 默认 `federated`，现有 catalog 不受影响。
* 该属性不可变：切换它会同时改变连接来源与写入方，并使已有行变成孤儿。改变模式需要重建 catalog。
* managed 模式仅在 `catalog-backend = jdbc` 时合法，且要求 Iceberg REST 服务与 entity store 同进程（auxiliary 模式）。独立的 `distribution/gravitino-iceberg-rest-server` 没有 entity store，无法服务 managed catalog；catalog 创建必须 fail fast（§5.1.1、§5.2）。
* managed 模式下元数据写入与 `gravitino.authorization.enable` 解耦：无论授权开关如何，实体都会写入。这与 federated 模式的行为不同（R11）。
* 同一个数据库内，managed catalog 与 federated catalog 不得共用同一个 `catalog-backend-name`，否则两条代码路径会争抢同一批行。
* 切换前创建的表（federated 模式写入 `iceberg_tables` 的行）在 managed 模式下仍可读，因为行格式完全相同。这提供了一条迁移路径：新建一个 managed catalog 指向同一个数据库和同一个 `catalog-backend-name`。
* 对于在授权关闭的 federated 模式下创建、因而没有 `table_meta` 的存量表：managed 模式读到时会走 import 补齐（与今天 Gravitino native `loadTable` 的 `imported = false` 路径一致），因此不需要一次性数据迁移。

## 7. 风险与待定问题

| # | 项 | 说明 |
| --- | --- | --- |
| R1 | 复刻 `CatalogHandlers` 的提交语义 | §4.4、§5.9。需要复刻的是 requirement 校验、update 应用、`changes().isEmpty()` 短路、`CommitFailedException` 重试循环，以及 `isCreate` 分支。语义本身由 public API 提供，但重试与分支逻辑要自己维护。 |
| R2 | metadata 文件命名与版本号解析 | 命名规则由 **Iceberg 表格式规范**定义（`format/spec.md:1334`），属实现规范而非抄源码（§5.5）。但规范与 `BaseMetastoreTableOperations` 的内部实现可能不同步（例如 `parseVersion` 只在实现里、规范里没有），因此每次升级 Iceberg 仍需对照规范与实现两处重核。 |
| R3 | `iceberg_tables` 的 DDL 归属 | Gravitino entity store 的账号需要 DDL 权限，或者必须交付 init 脚本。相关上游诉求：apache/gravitino#9208。 |
| R4 | H2 默认 entity store | `H2Database.constructH2URI` 会把 `jdbc:h2` 改写成 `jdbc:h2:file:<path>;DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=TRUE`，并把结果写回 config（`H2Database.java:75`）。任何派生 URL 必须从**启动之后**的 `GravitinoEnv.config()` 读取，绝不能读原始 `gravitino.conf`。固定 DDL 在 `MODE=MYSQL` 下的兼容性必须测试。 |
| R5 | `table_meta` 的并发写入 | `iceberg_tables` 有乐观并发保护；`table_meta` 行只有 JVM 内的 `TreeLock` 保护（见 apache/gravitino#10474）。多实例部署需要单独决策。 |
| R6 | 视图 | `iceberg_type = 'VIEW'` 与 `iceberg_namespace_properties` 会和 Gravitino 独立的 `view_meta` 交互。本次不实现，留待后续。 |
| R7 | 元数据文件残留 | §5.7 的 `finally` 兜底删除已经把残留概率降到"删除本身也失败"这一低概率情形。仍需一个维护任务（把 `metadata/` 目录内容与已引用的 location 比对）作为最终兜底，可复用 Iceberg 的 `removeOrphanFiles` action。 |
| R8 | `uri` 属性校验 | `uri` 目前是 `stringRequiredPropertyEntry`；条件校验不能削弱 federated 模式。 |
| R9 | Iceberg 版本耦合 | 本设计依赖 `TableMetadata.newTableMetadata`、`updateSchema`/`updatePartitionSpec`/`replaceSortOrder`/`replaceProperties`、`TableMetadata.buildFrom`、`TableMetadataParser`、`PendingUpdate.apply()`、`UpdateRequirement.validate`、`MetadataUpdate.applyTo`。这些都是 public 且稳定，脆弱点是 R1 与 R2 的复刻部分。 |
| R10 | **`table_meta` 同步判定需跟随 Iceberg 演进** | §5.9.3 按 `MetadataUpdate` 类型区分"是否影响实体内容"。Iceberg 新增 update 类型时必须重新归类，否则要么漏同步（实体漂移）、要么过度同步（每次数据写入都写 `table_meta`）。建议用白名单 + 单元测试枚举所有 `MetadataUpdate` 子类来防漏。 |
| R11 | **federated 与 managed 的实体写入语义不同** | managed 模式下实体无条件写入；federated 模式下仍受 `gravitino.authorization.enable` gate 控制。同一个 Iceberg catalog 在两种模式下行为不同是预期内的，但文档和运维手册必须写清楚，否则会被误判为 bug。 |
| R12 | **去掉授权 gate 的影响面** | 把写路径从 `IcebergTableHookDispatcher` 等 Hook 层移入实现层，会同时影响授权关闭与开启两种配置下的行为（授权开启时不再需要 Hook 重复 import）。需要回归：授权开启时的 owner 设置、list 可见性过滤、以及 `IcebergOrphanSchemaCleanup` 的调用时机。 |
| R13 | **重试/异步必须保持同线程** | 事务绑定在 `SqlSessions` 的 thread-local 上。`Tasks.foreach(...).run(...)` 默认同线程（`Tasks.java:199-206`，已实证为 `runSingleThreaded`），但一旦有人加上 `.executeWith(pool)`，或 Gravitino 自己实现重试时引入线程池，事务与 thread-local 上下文就会被破坏。精确后果（而非"两个 store 必然分裂"）见 §5.9.1。需用测试锁定（§8 测试 18）。 |

## 8. 测试策略

**单元测试**

1. `ManagedIcebergMetadataStoreTest` —— 针对嵌入式数据库验证每一条 SQL，包括"更新 0 行 → `CommitFailedException`"以及过期 base 的前置校验。
2. `TestWriteMetadataPath` —— 路径与版本推导符合 Iceberg 的 `%05d-<uuid>.metadata.json` 规则，覆盖 `write.metadata.location` 覆盖项与 codec 扩展名。
3. `TestParseVersion` —— 与 Iceberg 的版本解析器做往返比对。
4. `TestManagedCapability` —— 仅当 `catalog-backend-mode = managed` 且 `catalog-backend = jdbc` 时，`managedStorage(TABLE/SCHEMA)` 才是 `SUPPORTED`。

**集成测试**

5. **原子性 —— 验收基线。** 在 `createTable` 期间向 `table_meta` 写入注入失败（故障注入钩子或 mock 的 `EntityStore`），然后断言：
   * `iceberg_tables` 行**不存在**（当前代码做不到这一点，而这正是整件事的意义）；
   * 接口把失败暴露出来，而不是返回成功。
6. **孤儿文件容忍。** 在 (5) 之后，断言元数据 JSON 存在但无人引用，且随后的 `loadTable` 返回"表不存在"。
7. **往返兼容性。** 在 managed 模式建表，然后用一个指向同一数据库的原生 Iceberg `JdbcCatalog`、以及 Gravitino 的 Iceberg REST 服务分别读取。
8. **alter 等价性。** 对 Gravitino 在 Iceberg 上支持的每一种 `TableChange`，把结果 `TableMetadata`（schema id、spec id、sort order id、`last-column-id`、properties）与 federated 路径的结果做比对。
9. **数据写入路径。** 通过 Iceberg REST 服务对 managed catalog 执行 Spark/Flink append；断言 `iceberg_tables.metadata_location` 前进、`previous_metadata_location` 正确，且**不产生任何 `TableOperations` 调用**（managed 模式下该接口不应被使用）。
10. **提交重试。** 人为让 UPDATE 第一次返回 0 行（模拟并发写入），断言 `ManagedIcebergCommitHandler` 重读 base、重新应用 update 并成功；再让它持续失败，断言最终抛出 `CommitFailedException`。
11. **requirement 失败不重试。** requirements 校验失败时断言立即返回 `ValidationFailureException` 语义的错误，且没有额外的 metadata 文件被写出。
12. **并发。** 两个基于同一个 base 的并发 `alterTable` 必须产生一个成功、一个 `CommitFailedException`，而不是丢更新。
13. **元数据写入与授权解耦（§5.1.1）。** 在 `gravitino.authorization.enable = false` 下，通过 Iceberg REST 建表 / 建 namespace / register / rename / drop，断言 `table_meta` / `schema_meta` 都被正确写入、更新、软删。这一条在当前代码上会失败（Hook 层未装配），是 managed 模式的行为基线。
14. **REST commit 的 `table_meta` 同步（§5.9.3）。** 通过 Iceberg REST 提交一个含 `AddSchema` + `SetCurrentSchema` 的 commit（等价于 `ALTER TABLE ADD COLUMN`），断言 `table_meta` / `table_column_version_info` 在同一事务内同步；再提交一个纯 `AddSnapshot` + `SetSnapshotRef` 的 commit，断言 `table_meta` **没有**被写。
15. **`MetadataUpdate` 分类完备性（R10）。** 单元测试枚举 Iceberg 的 `MetadataUpdate` 全部子类，断言每一个都被显式归类为"影响实体内容"或"不影响"，新增子类时测试失败。
16. **前置条件 fail fast（§5.2）。** 分别用 `catalog-backend = hive`、standalone 模式、无 DDL 权限的账号创建 managed catalog，断言都在创建阶段失败并给出可执行的错误信息。
17. **两个入口一致性（§5.9）。** 用 Iceberg REST 入口和 Gravitino native 入口执行语义等价的操作（建表、加列、改属性、重命名、删除），断言最终 `iceberg_tables` 行、`table_meta` 行、元数据文件内容完全一致。
18. **重试同线程（R13）。** 在提交重试路径上断言事务绑定未被破坏：重试期间 `SqlSessions.getSessionCount()` 与绑定线程保持一致，且最终 `iceberg_tables` 与 `table_meta` 的更新落在同一连接、同一事务上。

## 9. 实施计划

按 §2.1 的优先级：**Iceberg REST 入口先落地，Gravitino native 入口复用同一实现层后置。**

**第一阶段 —— 基础设施（与入口无关）**

* `ManagedIcebergMetadataStore`：SQL、元数据文件辅助方法、版本解析、DDL。
* `catalog-backend-mode` 属性、`uri` 条件校验、`IcebergCatalogCapability` 改动、`IcebergCatalog.newOps`/`newCapability` 的选择逻辑。
* §5.2 的三条前置条件校验（fail fast）。
* 单元测试 1–4、15、16。

**第二阶段 —— 写路径原语**

* `IcebergManagedCatalogOperations` 实现 §5.8 的五个原语，包含 §5.7 的事务边界与 entity 写入。
* 集成测试 5、6。

**第三阶段 —— Iceberg REST 入口（主要交付目标）**

* `IcebergTableOperationExecutor` / `IcebergTableOperations` 接入实现层：create / register / drop / rename 走 §5.8 原语；`stageCreate` 保持「不落库、提交时登记」的语义（§5.9.1）。接入方式：`IcebergCatalogWrapperManager` 在 `catalog-backend-mode=managed` 时返回 managed wrapper，wrapper 通过 `CatalogManager.loadCatalogAndWrap(...).catalog().ops()` 取得 `ManagedIcebergCatalogOps` 并转换 `LoadTableResponse`；`CatalogInfo.namespace().level(0)` 提供 metalake，用于把 Iceberg 标识符解析成 Gravitino `NameIdentifier`。
* **去掉 Hook 层的授权 gate**：实体写入移入实现层，`IcebergTableHookDispatcher` / `IcebergNamespaceHookDispatcher` / `IcebergViewHookDispatcher` 中重复的 import 逻辑随之收敛（§5.1.1）。owner 设置、list 可见性过滤、`IcebergOrphanSchemaCleanup` 的调用时机需要回归（R12）。
* 读取路径：`loadTable` / `listTables` / `tableExists` 走 §5.6 的 SQL + `TableMetadataParser.read`，直接构造 `LoadTableResponse`；接入 `IcebergCatalogWrapper` 与 `IcebergCatalogWrapperManager` 的 managed 分支。
* `ManagedIcebergCommitHandler`：复刻 `CatalogHandlers.commit` 的纯逻辑 + 重试 + `isCreate` 分支，并实现 §5.9.3 的 `table_meta` 同步规则。
* 集成测试 7、9、10、11、13、14、18。

**第四阶段 —— 命名空间与文档**

* 针对 `iceberg_namespace_properties` 实现 `SupportsNamespaces`；schema entity 在同一事务内写入。
* `distribution/package/scripts/` 下的 init 脚本。
* `docs/lakehouse-iceberg-catalog.md`：记录 `catalog-backend-mode`、其前置条件（auxiliary 模式、同库）、与授权开关的关系、以及限制。

**第五阶段 —— Gravitino native 入口（次优先）**

* `TableOperationDispatcher` → 实现层的翻译：把 `TableChange` 映射改写为 `PendingUpdate.apply()` + `TableMetadata.updateXxx()`（§5.8.2）。
* 集成测试 8、12、17。

视图（`iceberg_type = 'VIEW'`）有意推迟。

---

## 附录 A —— 已核实的证据

Gravitino 的行号基于 `tag-v1.3.0`（`40fdf6ab9`），Iceberg 基于 `apache-iceberg-1.11.0`。

| 结论 | 证据 |
| --- | --- |
| Iceberg catalog 是代理 | `docs/lakehouse-iceberg-catalog.md:37`；`IcebergCatalogOperations.java:377-617` |
| `createTable` 先调用 SDK | `IcebergCatalogOperations.java:593-596` → `IcebergCatalogWrapper.java:220-233` |
| SDK 写元数据文件 + `iceberg_tables` 行 | `BaseMetastoreTableOperations.java:149-165`；`JdbcTableOperations.java:103-144`；`JdbcUtil.java:366-400` |
| 随后 Gravitino 写 `table_meta` | `TableOperationDispatcher.java:642-703`，`store.put` 在 `:689` |
| 失败被吞掉 | `TableOperationDispatcher.java:688-696`；schema 等价位置 `SchemaOperationDispatcher.java:157-167` |
| 两个独立事务 | `TableMetaService.insertTable` → `SessionUtils.doMultipleWithCommit`（`TableMetaService.java:111-137`）对比 Iceberg 的 `JdbcClientPool` |
| Iceberg 表默认 unmanaged | `Capability.java:96`（默认）、`:161-168`（TABLE 不支持）；`IcebergCatalogCapability` 只覆写了 `columnDefaultValue`、`supportsHierarchicalSchema`、`specificationOnName` |
| managed capability 会让 dispatcher 跳过写入 | `TableOperationDispatcher.java:660-667`；`OperationDispatcher.java:228-233`；先例 `GenericCatalogCapability.java:28-38` |
| Session 嵌套是引用计数的 | `SqlSessions.java:59-70`、`:106-138`；`SessionUtils.java:97-118` |
| `TableMetadata.newTableMetadata` 不设置 `metadataFileLocation` | `TableMetadata.java:148-155` |
| 元数据 JSON 不含自身位置 | `TableMetadataParser.java:277`（`metadata-file` 只出现在 `metadata-log` 内） |
| 路径规则 | `BaseMetastoreTableOperations.java:342-350`；`LocationUtil.stripTrailingSlash` 为 public |
| 版本解析要求 `%05d-` 前缀 | `BaseMetastoreTableOperations.java:359-374` |
| `PendingUpdate.apply()` 是不持久化的调用 | `PendingUpdate` javadoc："This does not result in a permanent update." |
| `SchemaUpdate` 构造函数为包级私有 | `SchemaUpdate.java:71-84`；同理 `PropertiesUpdate.java:44`、`BaseUpdatePartitionSpec.java:65`、`BaseReplaceSortOrder.java:39` |
| `BaseTable(TableOperations, String)` 为 public | `BaseTable.java:48` |
| `TableOperations` 两个钩子默认未实现 | `BaseMetastoreTableOperations.java:104`（`doRefresh`）、`:137`（`doCommit`），均抛 `UnsupportedOperationException("Not implemented: …")` |
| 提交路径的活儿基本都是纯函数 | `CatalogHandlers.java:613-666`：`ops` 只用于 `current()`/`refresh()`/`commit()`，中间是 `requirement.validate(base)` + `update.applyTo(builder)` + `builder.build()` |
| `UpdateRequirement.validate` 为 public | `UpdateRequirement.java:27-28`（`public interface UpdateRequirement`，`default void validate(TableMetadata base)`） |
| `MetadataUpdate.applyTo` 为 public | `MetadataUpdate.java:31-32`（`public interface MetadataUpdate`，`default void applyTo(TableMetadata.Builder)`） |
| `UpdateTableRequest` 暴露 requirements/updates | `UpdateTableRequest.java:55`、`:59` |
| 提交重试与短路 | `CatalogHandlers.java:616-623`（`Tasks.foreach(ops).retry(...).onlyRetryOn(CommitFailedException.class)`）、`:652-655`（`changes().isEmpty()` 不提交） |
| `isCreate` 分支 | `CatalogHandlers.java:578-593`（requirements 含 `AssertTableDoesNotExist`）、`:595`（`create(ops, request)`） |
| `JdbcClientPool` 从 deque 复用连接 | `ClientPoolImpl.get()`：`if (!clients.isEmpty()) return clients.removeFirst();` 在 `newClient()` 之前 |
| Iceberg 假设 JDBC auto-commit | `JdbcUtil` 中没有任何 `setAutoCommit`、`commit()` 或 `rollback()` 调用 |
| entity store 必然是 JDBC | `conf/gravitino.conf.template:46-49`（"we only supports relational / JDBC"） |
| H2 URL 会被归一化并写回 | `H2Database.java:75`、`:80-98` |
| `uri` 当前是必填 | `IcebergCatalogPropertiesMetadata.java:75-76` |
| **Iceberg REST 的实体写入受授权开关 gate 控制** | `RESTService.java:151-179`：`if (authorizationContext.isAuthorizationEnabled())` 才装配 `IcebergTableHookDispatcher` / `IcebergViewHookDispatcher` / `IcebergNamespaceHookDispatcher`；默认值 `conf/gravitino.conf.template:84` = `false` |
| Iceberg REST 写实体的位置（授权开启时） | `IcebergTableHookDispatcher.java:68-78`（create）、`:87-95`（仅 staged create 的 commit）、`:100-109`（drop + reconcile）、`:128-171`（rename）；`IcebergNamespaceHookDispatcher.java:62-96`（createNamespace + owner）、`:136-161`（dropNamespace + orphan cleanup）、`:127-133`（updateNamespace 纯透传）、`:180-189`（registerTable）；`IcebergViewHookDispatcher.java:69-88`（createView + owner） |
| Iceberg REST 读路径不碰实体（授权关闭时） | `IcebergTableOperationExecutor.java:106-119`、`:162-196` 只调用 `icebergCatalogWrapperManager.getCatalogWrapper(...)`；`CatalogWrapperForREST.java` 与 `IcebergTableOperationExecutor.java` 中无 `EntityStore` / `TableEntity` / `GravitinoEnv` 引用 |
| Iceberg REST 的 import 失败会向上抛 | `IcebergTableHookDispatcher.java:206-219` 注释："Import is intentionally NOT wrapped in try-catch: if it fails the table exists in Iceberg but not in Gravitino…" |
| import 依赖 metalake / catalog / schema 实体存在 | `TableMetaService.fillTablePOBuilderParentEntityId:379-387` → `EntityIdService.getEntityIds(...SCHEMA)` |
| import 写入列版本 | `TableMetaService.insertTable:147-152` → `TableColumnMetaService.insertColumnPOs`；`importTable` 只构建 `id/name/namespace/columns/auditInfo`（`TableOperationDispatcher.importTable`） |
| `table_version_info` 的 format 列可空 | `distribution/package/scripts/mysql/schema-1.3.0-mysql.sql:467`（注释："it will be null if it is not a lakehouse table"） |
| `iceberg_cleanup_job` 是 Iceberg 专属的 Gravitino 表 | `distribution/package/scripts/mysql/schema-1.3.0-mysql.sql:603` |
| `table_meta` 软删 + 级联 | `TableMetaService.deleteTable:244+` → `softDeleteTableMetasByTableId`，并级联软删 owner / columns / securable / tags / statistics |
| rename 会递增表版本 | `POConverters.updateTablePOWithVersionAndSchemaId:450-455`（`lastVersion = old + 1`，`currentVersion = lastVersion`） |
| 授权读路径会批量预加载实体 | `IcebergTableOperations.java:665-685` → `MetadataAuthzHelper.filterByExpression`（`server-common/.../MetadataAuthzHelper.java:165-172`，`preloadToCache` + `preloadOwner`） |
| auxiliary / standalone 由 `auxMode` 区分 | `RESTService.java:228-229`（`serviceInit(properties, auxMode)`）、`:120-127` 传入 `IcebergCatalogWrapperManager` 与 `IcebergRESTServerContext` |
| **REST commit 能改 schema / 属性** | `MetadataUpdate` 子类：`AddSchema`、`SetCurrentSchema`、`RemoveSchemas`、`SetProperties`、`RemoveProperties`、`SetLocation`、`UpgradeFormatVersion`、`AssignUUID`、`AddSnapshot`、`SetSnapshotRef` 等（`core/src/main/java/org/apache/iceberg/MetadataUpdate.java`） |
| REST commit 的 updates 来自 metadata diff | `RESTTableOperations.java:161-180`（`ImmutableList.<MetadataUpdate>builder().addAll(metadata.changes())`）；`TableMetadata.changes():586` |
