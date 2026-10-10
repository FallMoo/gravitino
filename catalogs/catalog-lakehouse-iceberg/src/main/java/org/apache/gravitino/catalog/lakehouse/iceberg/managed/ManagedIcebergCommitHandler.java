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
package org.apache.gravitino.catalog.lakehouse.iceberg.managed;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.util.Tasks;

/**
 * The Iceberg REST commit adapter (design doc sections 4.4, 5.9.1 and 5.9.3).
 *
 * <p>It replicates the metadata-construction core of Iceberg's {@code CatalogHandlers.commit} using
 * only public APIs: read the base, validate the requirements, apply the updates to a builder, build
 * the new metadata, then persist it through {@link IcebergManagedCatalogOperations}. It also
 * replicates the two behaviours that matter for correctness:
 *
 * <ul>
 *   <li>retry only on {@link CommitFailedException} (an optimistic-concurrency loss), never on a
 *       requirement validation failure;
 *   <li>only structure / property / format updates synchronize {@code table_meta}; data-version
 *       updates (snapshots, statistics, encryption keys) move the {@code iceberg_tables} pointer
 *       alone.
 * </ul>
 *
 * <p>The retry loop is synchronous and single-threaded on purpose: Gravitino's transaction and
 * request context live in thread-locals, so {@code .executeWith(...)} must never be added here
 * (design doc section 5.9.1 and risk R13).
 */
public class ManagedIcebergCommitHandler {

  /** Mirrors {@code CatalogHandlers.COMMIT_NUM_RETRIES_DEFAULT}. */
  private static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  private static final String RETRY_ITEM = "commit";

  /**
   * Updates that change what {@code table_meta} describes (design doc section 5.9.3, extended with
   * the partition-spec and sort-order updates that also live in the Gravitino entity).
   */
  private static final Set<Class<? extends MetadataUpdate>> ENTITY_CONTENT_UPDATES =
      ImmutableSet.of(
          MetadataUpdate.AddSchema.class,
          MetadataUpdate.SetCurrentSchema.class,
          MetadataUpdate.RemoveSchemas.class,
          MetadataUpdate.AddPartitionSpec.class,
          MetadataUpdate.RemovePartitionSpecs.class,
          MetadataUpdate.SetDefaultPartitionSpec.class,
          MetadataUpdate.AddSortOrder.class,
          MetadataUpdate.SetDefaultSortOrder.class,
          MetadataUpdate.SetProperties.class,
          MetadataUpdate.RemoveProperties.class,
          MetadataUpdate.SetLocation.class,
          MetadataUpdate.UpgradeFormatVersion.class,
          MetadataUpdate.AssignUUID.class);

  /**
   * Updates that only move the data version, statistics or encryption state and therefore must not
   * touch {@code table_meta}.
   */
  private static final Set<Class<? extends MetadataUpdate>> DATA_ONLY_UPDATES =
      ImmutableSet.of(
          MetadataUpdate.AddSnapshot.class,
          MetadataUpdate.SetSnapshotRef.class,
          MetadataUpdate.RemoveSnapshots.class,
          MetadataUpdate.RemoveSnapshotRef.class,
          MetadataUpdate.SetStatistics.class,
          MetadataUpdate.RemoveStatistics.class,
          MetadataUpdate.SetPartitionStatistics.class,
          MetadataUpdate.RemovePartitionStatistics.class,
          MetadataUpdate.AddEncryptionKey.class,
          MetadataUpdate.RemoveEncryptionKey.class,
          MetadataUpdate.AddViewVersion.class,
          MetadataUpdate.SetCurrentViewVersion.class);

  private final IcebergManagedCatalogOperations ops;

  /**
   * @param ops the managed operations that own the metadata file and the two stores
   */
  public ManagedIcebergCommitHandler(IcebergManagedCatalogOperations ops) {
    this.ops = ops;
  }

  /**
   * Commits an Iceberg REST {@code UpdateTableRequest}.
   *
   * @param ident the table identifier
   * @param request the requirements and updates sent by the client
   * @return the committed metadata
   * @throws NoSuchTableException if the table does not exist
   * @throws CommitFailedException if a concurrent writer keeps winning, or if a requirement fails
   * @throws ValidationFailure if a requirement does not match the base metadata (never retried)
   */
  public TableMetadata commit(NameIdentifier ident, UpdateTableRequest request)
      throws NoSuchTableException {
    if (isCreate(request)) {
      return create(ident, request);
    }

    AtomicReference<TableMetadata> committed = new AtomicReference<>();
    Tasks.foreach(RETRY_ITEM)
        .retry(COMMIT_NUM_RETRIES_DEFAULT)
        .exponentialBackoff(100L, 60_000L, 1_800_000L, 2.0)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            task -> {
              // Re-read on every attempt, so a retry works against the metadata the winner wrote.
              TableMetadata base = ops.readMetadata(ident);
              validateRequirements(base, request);
              TableMetadata updated = applyUpdates(base, request);
              if (updated.changes().isEmpty()) {
                committed.set(base);
                return;
              }

              committed.set(
                  touchesEntityContent(request.updates())
                      ? ops.commit(ident, base, updated)
                      : ops.commitDataOnly(ident, base, updated));
            });

    return committed.get();
  }

  /**
   * @param request the REST commit request
   * @return {@code true} when the request is a staged-create commit (design doc section 5.9.1)
   */
  public static boolean isCreate(UpdateTableRequest request) {
    return request.requirements().stream()
        .anyMatch(requirement -> requirement instanceof UpdateRequirement.AssertTableDoesNotExist);
  }

  /**
   * The {@code table_meta} synchronization rule (design doc section 5.9.3).
   *
   * @param updates the updates of the commit request
   * @return {@code true} when the Gravitino entity content has to be rewritten
   */
  public static boolean touchesEntityContent(List<MetadataUpdate> updates) {
    return updates.stream().anyMatch(update -> ENTITY_CONTENT_UPDATES.contains(update.getClass()));
  }

  /**
   * @return the update types that synchronize {@code table_meta}
   */
  public static Set<Class<? extends MetadataUpdate>> entityContentUpdateTypes() {
    return ENTITY_CONTENT_UPDATES;
  }

  /**
   * @return the update types that only move the data version
   */
  public static Set<Class<? extends MetadataUpdate>> dataOnlyUpdateTypes() {
    return DATA_ONLY_UPDATES;
  }

  /**
   * The pure replication of {@code CatalogHandlers.commit}'s metadata construction.
   *
   * @param base the current metadata
   * @param request the commit request
   * @return the new metadata
   */
  static TableMetadata applyUpdates(TableMetadata base, UpdateTableRequest request) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    request.updates().forEach(update -> update.applyTo(builder));
    return builder.build();
  }

  /**
   * The {@code isCreate} branch (design doc section 5.9.1): the updates are applied to an empty
   * base and the result is persisted as a brand new table.
   */
  private TableMetadata create(NameIdentifier ident, UpdateTableRequest request) {
    // For a create, the "current" metadata is absent; AssertTableDoesNotExist.validate(null) passes
    // and any other requirement fails, exactly like CatalogHandlers.create.
    request.requirements().forEach(requirement -> requirement.validate((TableMetadata) null));

    int formatVersion =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.UpgradeFormatVersion)
            .map(update -> ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion())
            .findFirst()
            .orElse(1);

    TableMetadata.Builder builder = TableMetadata.buildFromEmpty(formatVersion);
    boolean hasLocation =
        request.updates().stream().anyMatch(update -> update instanceof MetadataUpdate.SetLocation);
    if (!hasLocation) {
      // Iceberg's REST clients normally send SetLocation; fall back to the catalog's default so the
      // metadata file always has a home.
      builder.setLocation(ops.defaultTableLocation(ident));
    }
    request.updates().forEach(update -> update.applyTo(builder));
    TableMetadata metadata = builder.build();

    ops.createTableFromMetadata(ident, metadata, metadata.properties());
    return ops.readMetadata(ident);
  }

  private static void validateRequirements(TableMetadata base, UpdateTableRequest request) {
    try {
      request.requirements().forEach(requirement -> requirement.validate(base));
    } catch (CommitFailedException e) {
      // Iceberg wraps this in its package-private ValidationFailureException for the same reason:
      // onlyRetryOn(CommitFailedException.class) must not retry a deterministic validation failure.
      throw new ValidationFailure(e);
    }
  }

  /**
   * A requirement validation failure. Iceberg uses a package-private {@code
   * CatalogHandlers.ValidationFailureException} for this; the observable contract is the same: it
   * is not retried and the REST layer maps it to a conflict.
   */
  public static final class ValidationFailure extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailure(CommitFailedException wrapped) {
      super(wrapped.getMessage(), wrapped);
      this.wrapped = wrapped;
    }

    /**
     * @return the wrapped {@link CommitFailedException} that describes the failed requirement
     */
    public CommitFailedException wrapped() {
      return wrapped;
    }
  }
}
