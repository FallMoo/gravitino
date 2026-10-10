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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * The mode an Iceberg catalog runs in.
 *
 * <ul>
 *   <li>{@link #FEDERATED} - the default. Gravitino proxies every schema/table/view operation to
 *       the Iceberg SDK, which owns both the metadata files and the JDBC ({@code hive} / {@code
 *       jdbc} / {@code rest} / ...) backend.
 *   <li>{@link #MANAGED} - Gravitino owns the transaction: the SDK is used as a pure library for
 *       Iceberg semantics, while all SQL (including {@code iceberg_tables}) runs on the Gravitino
 *       entity store connection, so the metadata pointer and {@code table_meta} commit together.
 * </ul>
 *
 * <p>Managed mode is only legal together with {@code catalog-backend = jdbc}; see {@code
 * IcebergManagedCatalogPrerequisites}.
 */
public enum IcebergCatalogBackendMode {
  FEDERATED,
  MANAGED;

  /**
   * Reads the mode from a catalog property map, defaulting to {@link #FEDERATED} when the property
   * is absent or blank.
   *
   * @param properties the catalog properties, may be {@code null}
   * @return the configured mode, never {@code null}
   * @throws IllegalArgumentException if the value is not a known mode
   */
  public static IcebergCatalogBackendMode fromProperties(Map<String, String> properties) {
    if (properties == null) {
      return FEDERATED;
    }

    String value = properties.get(IcebergConstants.CATALOG_BACKEND_MODE);
    if (StringUtils.isBlank(value)) {
      return FEDERATED;
    }

    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value [%s] for property '%s'. Valid values are [%s, %s].",
              value,
              IcebergConstants.CATALOG_BACKEND_MODE,
              FEDERATED.name().toLowerCase(Locale.ROOT),
              MANAGED.name().toLowerCase(Locale.ROOT)),
          e);
    }
  }

  /**
   * @return the mode in the form used by catalog properties, e.g. {@code managed}
   */
  public String value() {
    return name().toLowerCase(Locale.ROOT);
  }
}
