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

package org.apache.gravitino.storage.relational.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility methods to perform database operations with MyBatis mappers in the
 * SqlSession.
 */
public class SessionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SessionUtils.class);

  private SessionUtils() {}

  /**
   * Performs a database operation with a commit. Manages the full transaction lifecycle. Can be
   * nested within other transactions.
   */
  public static <T> void doWithCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
      SqlSessions.commitAndCloseSqlSession();
    } catch (Throwable t) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw t;
    }
  }

  /**
   * Performs a database operation with a commit and fetches a result. Manages the full transaction
   * lifecycle. Can be nested within other transactions.
   */
  public static <T, R> R doWithCommitAndFetchResult(Class<T> mapperClazz, Function<T, R> func) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      R result = func.apply(mapper);
      SqlSessions.commitAndCloseSqlSession();
      return result;
    } catch (Throwable t) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw t;
    }
  }

  /**
   * Performs a read-only database operation without a commit. Can be used standalone or nested
   * within other transactions.
   */
  public static <T, R> R getWithoutCommit(Class<T> mapperClazz, Function<T, R> func) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      return func.apply(mapper);
    } finally {
      // This will decrement the counter, the session is closed only when the counter is 0.
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * Performs a database operation without a commit. Can be used standalone or nested within other
   * transactions. This method is for operations that do not return a result.
   */
  public static <T> void doWithoutCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
    } finally {
      // This will decrement the counter, the session is closed only when the counter is 0.
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * Performs multiple database operations within a single commit. Manages the full transaction
   * lifecycle.
   */
  public static void doMultipleWithCommit(Runnable... operations) {
    // This method acts as the outermost transaction boundary.
    // It increments the session count once.
    SqlSessions.getSqlSession();
    try {
      Arrays.stream(operations).forEach(Runnable::run);
      SqlSessions.commitAndCloseSqlSession();
    } catch (Throwable t) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw t;
    }
  }

  /**
   * Returns the JDBC connection bound to the transaction on the current thread.
   *
   * <p>Every SQL statement that must take part in the Gravitino transaction - including the Iceberg
   * {@code iceberg_tables} row written by managed mode - has to run on this connection. This method
   * does not change the session reference count, so it is safe to call it while a transaction is
   * open.
   *
   * @return the connection bound to the current thread's transaction
   * @throws IllegalStateException if no transaction is active on this thread
   */
  public static Connection currentConnection() {
    Connection connection = SqlSessions.getBoundConnection();
    if (connection == null) {
      throw new IllegalStateException(
          "No Gravitino transaction is bound to the current thread. Call"
              + " SessionUtils.beginTransaction() before running transactional SQL.");
    }
    return connection;
  }

  public static void beginTransaction() {
    SqlSessions.getSqlSession();
  }

  public static void commitTransaction() {
    SqlSessions.commitAndCloseSqlSession();
  }

  public static void rollbackTransaction() {
    SqlSessions.rollbackAndCloseSqlSession();
  }

  /**
   * Commits a transaction that mixes SQL executed directly on {@link #currentConnection()} with
   * nested MyBatis entity writes.
   *
   * <p>This is not the same as {@link #commitTransaction()}: MyBatis only issues a JDBC commit when
   * the session is dirty, and - more dangerously - {@code JdbcTransaction.close()} resets
   * auto-commit, which makes the JDBC driver commit any statement that MyBatis did not track (a
   * plain {@code PreparedStatement} executed on the bound connection, for example). Committing the
   * JDBC transaction explicitly before closing the session is therefore the only way to get a
   * well-defined outcome for such a transaction.
   *
   * @throws IllegalStateException if no transaction is bound to the current thread
   * @throws RuntimeException if the JDBC commit fails; the transaction is rolled back first
   */
  public static void commitTransactionAndConnection() {
    Connection connection = SqlSessions.getBoundConnection();
    if (connection == null) {
      throw new IllegalStateException(
          "No Gravitino transaction is bound to the current thread. Call"
              + " SessionUtils.beginTransaction() first.");
    }
    try {
      connection.commit();
    } catch (SQLException e) {
      rollbackTransactionAndConnection();
      throw new RuntimeException("Failed to commit the Gravitino transaction", e);
    }
    commitTransaction();
  }

  /**
   * Rolls back a transaction that mixes SQL executed directly on {@link #currentConnection()} with
   * nested MyBatis entity writes and closes the session.
   *
   * <p>See {@link #commitTransactionAndConnection()} for why the JDBC transaction has to be
   * resolved explicitly instead of relying on {@link #rollbackTransaction()}.
   */
  public static void rollbackTransactionAndConnection() {
    Connection connection = SqlSessions.getBoundConnection();
    if (connection != null) {
      try {
        connection.rollback();
      } catch (SQLException e) {
        // The session cleanup below must still happen; the caller is already handling a failure.
        LOG.warn("Failed to roll back the Gravitino transaction bound to the current thread", e);
      }
    }
    rollbackTransaction();
  }
}
