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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the metadata file naming / version rules (design doc section 8, test 2 and section
 * 5.5). The rules come from the Iceberg table format spec ({@code
 * <V>-<random-uuid>.metadata.json}).
 */
public class TestWriteMetadataPath {

  private static final String TABLE_LOCATION = "file:///tmp/managed-warehouse/db/t";

  private ManagedIcebergMetadataStore store;
  private FileIO io;

  @BeforeEach
  public void setUp() {
    store = new ManagedIcebergMetadataStore("catalog-1");
    io = new InMemoryFileIO();
    io.initialize(Collections.emptyMap());
  }

  private boolean inMemoryFileExists(String location) {
    try {
      return io.newInputFile(location).exists();
    } catch (NotFoundException e) {
      return false;
    }
  }

  private static TableMetadata newMetadata(Map<String, String> extraProperties) {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        TABLE_LOCATION,
        extraProperties);
  }

  @Test
  public void testDefaultPathFollowsTheSpec() {
    TableMetadata metadata = newMetadata(ImmutableMap.of());

    ManagedIcebergMetadataStore.MetadataWriteResult result = store.writeMetadata(io, metadata, 7);

    assertTrue(result.written(), "a fresh write must be marked as written");
    assertTrue(
        result.location().startsWith(TABLE_LOCATION + "/metadata/00007-"),
        "unexpected location: " + result.location());
    // Iceberg 1.11's default metadata compression codec is "none".
    assertTrue(result.location().endsWith(".metadata.json"), result.location());
    assertEquals(7, ManagedIcebergMetadataStore.parseVersion(result.location()));
    assertTrue(inMemoryFileExists(result.location()), "the file was not written");

    // The round-tripped metadata must still be readable and identical in payload.
    TableMetadata read = TableMetadataParser.read(io, result.location());
    assertEquals(metadata.uuid(), read.uuid());
    assertEquals(metadata.location(), read.location());
    assertEquals(metadata.lastColumnId(), read.lastColumnId());
    assertEquals(metadata.properties(), read.properties());
  }

  @Test
  public void testGzipCodecAddsTheCompressedExtension() {
    TableMetadata metadata =
        newMetadata(ImmutableMap.of(TableProperties.METADATA_COMPRESSION, "gzip"));

    ManagedIcebergMetadataStore.MetadataWriteResult result = store.writeMetadata(io, metadata, 1);

    // Iceberg places the codec extension before ".metadata.json": <version>-<uuid>.gz.metadata.json
    assertTrue(result.location().endsWith(".gz.metadata.json"), result.location());
    assertEquals(1, ManagedIcebergMetadataStore.parseVersion(result.location()));
    assertEquals(metadata.uuid(), TableMetadataParser.read(io, result.location()).uuid());
  }

  @Test
  public void testWriteMetadataLocationOverridesTheBaseDirectory() {
    TableMetadata metadata =
        newMetadata(
            ImmutableMap.of(
                TableProperties.WRITE_METADATA_LOCATION, "file:///tmp/custom-metadata/"));

    ManagedIcebergMetadataStore.MetadataWriteResult result = store.writeMetadata(io, metadata, 3);

    assertTrue(
        result.location().startsWith("file:///tmp/custom-metadata/00003-"),
        "unexpected location: " + result.location());
    assertFalse(result.location().contains("/metadata/"), result.location());
  }

  @Test
  public void testReusedMetadataIsNotMarkedAsWritten() {
    TableMetadata metadata = newMetadata(ImmutableMap.of());
    ManagedIcebergMetadataStore.MetadataWriteResult written = store.writeMetadata(io, metadata, 1);
    TableMetadata read = TableMetadataParser.read(io, written.location());

    ManagedIcebergMetadataStore.MetadataWriteResult reused = store.reuseMetadataFile(read);
    assertEquals(read.metadataFileLocation(), reused.location());
    assertFalse(reused.written(), "a reused file must never be cleaned up on failure");
  }

  @Test
  public void testDeleteMetadataFileQuietly() {
    ManagedIcebergMetadataStore.MetadataWriteResult written =
        store.writeMetadata(io, newMetadata(ImmutableMap.of()), 1);
    assertTrue(inMemoryFileExists(written.location()));

    assertTrue(store.deleteMetadataFileQuietly(io, written.location()));
    assertFalse(inMemoryFileExists(written.location()));

    // Deleting a missing file is best effort and must not blow up the caller.
    assertFalse(store.deleteMetadataFileQuietly(io, written.location()));
  }

  @Test
  public void testWriteMetadataDoesNotSetTheLocationOnTheMetadataItself() {
    TableMetadata metadata = newMetadata(ImmutableMap.of());
    assertNull(metadata.metadataFileLocation());
    ManagedIcebergMetadataStore.MetadataWriteResult result = store.writeMetadata(io, metadata, 1);
    assertNull(metadata.metadataFileLocation());
    assertEquals(
        result.location(), TableMetadataParser.read(io, result.location()).metadataFileLocation());
  }

  @Test
  public void testParseVersionRejectsNonSpecNames() {
    assertEquals(-1, ManagedIcebergMetadataStore.parseVersion("/tmp/nouuid/metadata/file.json"));
    assertThrows(
        NumberFormatException.class,
        () -> ManagedIcebergMetadataStore.parseVersion("file:///tmp/abc-def.metadata.json"));
  }
}
