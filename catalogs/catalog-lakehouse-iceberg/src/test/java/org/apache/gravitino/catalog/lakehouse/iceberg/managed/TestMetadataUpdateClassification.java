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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.junit.jupiter.api.Test;

/**
 * Design doc section 8, test 15 and risk R10: every {@link MetadataUpdate} subtype must be
 * classified explicitly as either "affects the Gravitino entity content" or "data version only".
 * When Iceberg adds a subtype, this test fails until it is classified.
 */
public class TestMetadataUpdateClassification {

  private static Set<Class<?>> allMetadataUpdates() {
    return Arrays.stream(MetadataUpdate.class.getDeclaredClasses())
        .filter(clazz -> MetadataUpdate.class.isAssignableFrom(clazz))
        .filter(clazz -> !clazz.isInterface())
        .collect(Collectors.toSet());
  }

  @Test
  public void testEveryMetadataUpdateIsClassifiedExactlyOnce() {
    Set<Class<?>> all = allMetadataUpdates();
    Set<Class<?>> entityContent =
        new HashSet<>(ManagedIcebergCommitHandler.entityContentUpdateTypes());
    Set<Class<?>> dataOnly = new HashSet<>(ManagedIcebergCommitHandler.dataOnlyUpdateTypes());

    assertFalse(all.isEmpty(), "reflection found no MetadataUpdate subtypes");

    Set<Class<?>> unclassified = new HashSet<>(all);
    unclassified.removeAll(entityContent);
    unclassified.removeAll(dataOnly);
    assertTrue(
        unclassified.isEmpty(),
        "New Iceberg MetadataUpdate types must be classified in ManagedIcebergCommitHandler: "
            + unclassified.stream()
                .map(Class::getSimpleName)
                .sorted()
                .collect(Collectors.toList()));

    Set<Class<?>> both = new HashSet<>(entityContent);
    both.retainAll(dataOnly);
    assertTrue(
        both.isEmpty(),
        "Types classified as both entity content and data-only: "
            + both.stream().map(Class::getSimpleName).sorted().collect(Collectors.toList()));

    Set<Class<?>> unknown = new HashSet<>(entityContent);
    unknown.addAll(dataOnly);
    unknown.removeAll(all);
    assertTrue(
        unknown.isEmpty(),
        "Classification mentions types that are not MetadataUpdate subtypes: "
            + unknown.stream().map(Class::getSimpleName).sorted().collect(Collectors.toList()));

    assertEquals(
        all.size(),
        entityContent.size() + dataOnly.size(),
        "every MetadataUpdate subtype must be classified exactly once");
  }

  @Test
  public void testRepresentativeClassifications() {
    assertTrue(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(
                new MetadataUpdate.AddSchema(
                    new org.apache.iceberg.Schema(
                        org.apache.iceberg.types.Types.NestedField.required(
                            1, "id", org.apache.iceberg.types.Types.IntegerType.get()))))));
    assertTrue(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.SetProperties(Map.of("k", "v")))));
    assertTrue(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.SetDefaultPartitionSpec(1))));
    assertTrue(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.SetDefaultSortOrder(1))));

    assertFalse(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.RemoveSnapshotRef("main"))));
    assertFalse(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.RemoveStatistics(1L))));
    assertFalse(
        ManagedIcebergCommitHandler.touchesEntityContent(
            Arrays.asList(new MetadataUpdate.RemoveEncryptionKey("key"))));
    assertFalse(ManagedIcebergCommitHandler.touchesEntityContent(List.of()));
  }
}
