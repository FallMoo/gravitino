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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@code parseVersion} (design doc section 8, test 3).
 *
 * <p>Besides the pure round trip, this cross-checks the parser against the file names Iceberg
 * itself produces: an {@link InMemoryCatalog} builds its table metadata files with the very same
 * {@code %05d-<uuid>.metadata.json} scheme, so the version parsed out of a real Iceberg file name
 * must match the version Iceberg advanced to.
 */
public class TestParseVersion {

  private static Schema schema() {
    return new Schema(required(1, "id", Types.IntegerType.get()));
  }

  @Test
  public void testRoundTripForEveryVersionShape() {
    for (int version : new int[] {0, 1, 2, 9, 10, 42, 99999}) {
      String location =
          String.format(
              Locale.ROOT,
              "file:///tmp/warehouse/db/t/metadata/%05d-%s.metadata.json",
              version,
              UUID.randomUUID());
      assertEquals(version, ManagedIcebergMetadataStore.parseVersion(location), location);
    }
  }

  @Test
  public void testParsesIcebergsOwnMetadataFileNames() throws Exception {
    Path warehouse = Files.createTempDirectory("gravitino-parse-version-");
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("parse-version", ImmutableMap.of("warehouse", warehouse.toString()));
    catalog.createNamespace(Namespace.of("db"));
    Table table = catalog.buildTable(TableIdentifier.of("db", "t"), schema()).create();

    TableMetadata first =
        ((BaseTable) catalog.loadTable(TableIdentifier.of("db", "t"))).operations().current();
    String firstLocation = first.metadataFileLocation();
    // Iceberg writes the very first metadata file as version 0 (the version encoded in the name is
    // a monotonic counter, not a 1-based count), so managed mode must do the same on create.
    assertTrue(
        firstLocation.contains("/metadata/00000-"),
        "Iceberg's own naming changed: " + firstLocation);
    assertEquals(0, ManagedIcebergMetadataStore.parseVersion(firstLocation));

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(table.location() + "/data/f1.parquet")
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .withFormat(FileFormat.PARQUET)
            .build();
    table.newAppend().appendFile(file).commit();

    TableMetadata second =
        ((BaseTable) catalog.loadTable(TableIdentifier.of("db", "t"))).operations().current();
    assertEquals(1, ManagedIcebergMetadataStore.parseVersion(second.metadataFileLocation()));
  }

  @Test
  public void testRejectsNamesWithoutTheVersionPrefix() {
    assertEquals(-1, ManagedIcebergMetadataStore.parseVersion("metadata/file.json"));
    assertEquals(-1, ManagedIcebergMetadataStore.parseVersion("file.json"));
    assertEquals(-1, ManagedIcebergMetadataStore.parseVersion("file:///tmp/db/t/metadata/x.json"));
  }

  @Test
  public void testMatchingFileNameHelperAgreesWithTheParser() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema(),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/warehouse/db/t",
            ImmutableMap.of());
    String fileName = ManagedIcebergMetadataStore.newMetadataFileName(metadata, 12);
    String location = ManagedIcebergMetadataStore.metadataFileLocation(metadata, fileName);
    assertEquals(12, ManagedIcebergMetadataStore.parseVersion(location));
    assertTrue(fileName.startsWith("00012-"), fileName);
  }
}
